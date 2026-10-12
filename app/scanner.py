"""扫描源目录 → 稳定性检测 → 入队"""
import os
import stat
import time
from contextvars import ContextVar
from typing import Optional, Tuple, List
import fnmatch
from . import db, config, nfo_fix, transfer
from .douyin_nfo import (ensure_nfo, ensure_filename_nfo, backfill_nfo_for_link,
                         VIDEO_EXTS, POSTER_EXTS)


def _parse_patterns(raw: str) -> list[str]:
    return [p.strip() for p in (raw or "").split(",") if p.strip()]


def _match_any(rel_path: str, name: str, patterns: List[str]) -> bool:
    for p in patterns:
        if fnmatch.fnmatch(name, p) or fnmatch.fnmatch(rel_path, p):
            return True
    return False


# 每次扫描独立保存首轮采样；即使测试/调用方并发扫描也不会串用样本。
_scan_samples = ContextVar("scan_samples", default=None)


def _signature(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def _stable(path: str, wait: int) -> bool:
    """校验本轮首采样与当前 size/mtime/inode，且写入结束已达到静默期。"""
    context = _scan_samples.get()
    if context is None:
        # 未经 scan_task 首采样/静默期初始化的调用不能宣称下载完成。
        return False
    samples, quiet, generated = context
    try:
        if os.path.islink(path):
            return False
        current = os.stat(path)
    except OSError:
        return False
    first = samples.get(path)
    if first is None:
        return False
    return (first == _signature(current)
            and (path in generated or time.time() - current.st_mtime >= quiet))


def scan_task(task: dict, skip_stable_check: bool = False, progress: Optional[dict] = None,
              on_dir=None, directory_cache: Optional[dict] = None,
              force_full: bool = False) -> dict:
    """扫描入口。无缓存/立即同步为全量；周期扫描可按目录变更增量枚举。"""
    token = _scan_samples.set(None)
    try:
        return _scan_task(task, progress=progress, on_dir=on_dir,
                          directory_cache=directory_cache, force_full=force_full)
    finally:
        _scan_samples.reset(token)


def _collect_directories(src: str, previous: dict, force_full: bool):
    """未变化目录只 stat 并沿已知子目录向下走，不枚举其文件/软链。"""
    directories, current = [], {}

    pending = [src]
    while pending:
        root = pending.pop()
        try:
            info = os.stat(root) if root == src else os.lstat(root)
        except FileNotFoundError:
            continue
        if not stat.S_ISDIR(info.st_mode):
            continue
        identity = (info.st_dev, info.st_ino, info.st_mtime_ns)
        old = previous.get(root)
        if not force_full and old and old[0] == identity and not old[2]:
            current[root] = old
            pending.extend(os.path.join(root, name) for name in old[1])
            continue
        children, files = [], []
        with os.scandir(root) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    children.append(entry.name)
                else:
                    files.append(entry.name)
        current[root] = (identity, tuple(children), False)
        directories.append((root, files))
        pending.extend(os.path.join(root, name) for name in children)
    return directories, current


def compute_dst(dst_dir: str, rel_path: str, path_rule: str, date_str: str) -> str:
    if path_rule == "flatten":
        return os.path.join(dst_dir, os.path.basename(rel_path))
    if path_rule == "by_date":
        return os.path.join(dst_dir, date_str, rel_path)
    return os.path.join(dst_dir, rel_path)  # keep_structure


def _within_destination(path: str, destination: str) -> bool:
    if not os.path.isabs(path):
        return False
    base = os.path.realpath(destination)
    return os.path.commonpath((base, os.path.realpath(os.path.dirname(path)))) == base


def _trusted_link_target(task: dict, rel: str, link: str) -> bool:
    """只接受确实指向本任务该相对路径迁移目标的占位链接。"""
    target = os.path.realpath(link)
    recorded = db.get_migrated_dst(task["id"], rel)
    # 仅匹配目录结构不够：必须有该任务该文件的已迁移目标记录。
    return (bool(recorded)
            and _within_destination(target, task["dst_dir"])
            and _within_destination(recorded, task["dst_dir"])
            and target == os.path.realpath(recorded))


def _scan_task(task: dict, progress: Optional[dict] = None, on_dir=None,
               directory_cache: Optional[dict] = None, force_full: bool = False) -> dict:
    """扫描一个任务的源目录并入队已静默文件。"""
    src = task["src_dir"]
    cfg = config.get()
    if not os.path.isabs(src) or not os.path.isabs(task["dst_dir"]):
        raise ValueError("迁移目录必须是绝对路径")
    a, b = os.path.realpath(src), os.path.realpath(task["dst_dir"])
    if os.path.commonpath((a, b)) in (a, b):
        raise ValueError("源目录与目标目录重叠，已拒绝扫描")
    stats = {"enqueued": 0, "dedup_deleted": 0, "dedup_skipped": 0, "skipped_symlink": 0}
    # progress 为外部传入的共享进度字典（立即同步用），扫描过程实时更新
    def _bump(key, n=1):
        stats[key] += n
        if progress is not None:
            progress[key] = stats[key]
    if not os.path.isdir(src):
        return stats
    includes = _parse_patterns(task.get("include_patterns"))
    initial_device = transfer._mount_identity(task["dst_dir"])
    excludes = _parse_patterns(task.get("exclude_patterns"))
    ignores = cfg.get("ignore_suffixes", [])
    filename_nfo_enabled = task.get("filename_nfo_enabled", False)
    # 两种生成格式不能在同一任务上竞争同一个 .nfo 路径。
    douyin_nfo_enabled = cfg.get("douyin_nfo_enabled", False) and not filename_nfo_enabled
    re_download_action = cfg.get("re_download_action", "delete")  # delete/skip/keep
    date_str = time.strftime("%Y-%m-%d")
    quiet = max(0, float(cfg.get("stable_quiet_seconds", 60)))
    wait = cfg.get("stable_check_seconds", 2) if cfg.get("stable_check") else 0
    # 先收集整轮目录和首采样，统一等待一次；不对每个文件或每层目录 sleep。
    cache_key = (os.path.realpath(src), os.path.realpath(task["dst_dir"]),
                 task.get("include_patterns"), task.get("exclude_patterns"),
                 task.get("path_rule"), task.get("after_action"),
                 task.get("symlink_enabled"), filename_nfo_enabled,
                 douyin_nfo_enabled, cfg.get("nfo_fix_enabled"), re_download_action,
                 tuple(ignores), quiet, wait)
    if directory_cache is None:
        directories = [(root, files) for root, _dirs, files in os.walk(src)]
        next_entries = None
    else:
        previous = (directory_cache.get("entries", {})
                    if directory_cache.get("key") == cache_key else {})
        directories, next_entries = _collect_directories(src, previous, force_full)
    samples = {}
    for root, files in directories:
        for name in files:
            path = os.path.join(root, name)
            rel = os.path.relpath(path, src)
            if (name.startswith(".") or name.endswith(".moving")
                    or any(name.endswith(s) for s in ignores)
                    or (excludes and _match_any(rel, name, excludes))
                    or os.path.islink(path)):
                continue
            try:
                info = os.stat(path)
                samples[path] = _signature(info)
                if next_entries is not None and time.time() - info.st_mtime < quiet:
                    identity, children, _ = next_entries[root]
                    next_entries[root] = (identity, children, True)
            except OSError:
                if next_entries is not None:
                    identity, children, _ = next_entries[root]
                    next_entries[root] = (identity, children, True)
    if not directories:
        if directory_cache is not None:
            directory_cache["entries"] = next_entries
        return stats
    if samples and wait:
        time.sleep(wait)
    generated = set()
    _scan_samples.set((samples, quiet, generated))

    for root, files in directories:
        # 每层目录回调: 更新进度 (sync_now 传入)
        if on_dir:
            try:
                on_dir(root, len(files))
            except Exception:
                pass
        # 按任务选择一种补全格式；生成的新 NFO 加入本轮文件列表并经正常队列迁移。
        # 软链视频不能触发源目录 NFO 生成，避免处理遗留占位。
        if filename_nfo_enabled or douyin_nfo_enabled:
            # 发现已有 NFO 时必须使用原始目录清单，不能因过滤器排除它就误补一份。
            # 封面清单则只包含本任务会迁移的文件，避免 NFO 引用被过滤的图片。
            poster_files = [name for name in files if not name.startswith(".")
                            and not any(name.endswith(s) for s in ignores)
                            and not (excludes and _match_any(
                                os.path.relpath(os.path.join(root, name), src), name,
                                excludes))
                            and (not includes or _match_any(
                                os.path.relpath(os.path.join(root, name), src), name,
                                includes) or (os.path.splitext(name)[1].lower() in POSTER_EXTS
                                             and os.path.splitext(name)[0].lower().endswith("-poster")))]
            for fn in list(files):
                full_p = os.path.join(root, fn)
                rel_video = os.path.relpath(full_p, src)
                if (os.path.islink(full_p) or fn.startswith(".")
                        or any(fn.endswith(s) for s in ignores)
                        or (includes and not _match_any(rel_video, fn, includes))
                        or (excludes and _match_any(rel_video, fn, excludes))
                        or not _stable(full_p, wait)):
                    continue
                base, ext = os.path.splitext(fn)
                if ext.lower() in VIDEO_EXTS:
                    try:
                        if filename_nfo_enabled:
                            created, _ = ensure_filename_nfo(full_p, files, poster_files)
                        else:
                            created, _ = ensure_nfo(full_p)
                        if created:
                            nfo_path = os.path.join(root, base + ".nfo")
                            # 由本进程原子发布的 NFO 已完整写入；仍校验 inode/size/mtime。
                            samples[nfo_path] = _signature(os.stat(nfo_path))
                            generated.add(nfo_path)
                            files.append(base + ".nfo")
                    except OSError as exc:
                        db.add_log(task["id"], None, full_p,
                                   os.path.join(root, base + ".nfo"), 0, 0,
                                   "failed", f"源目录 NFO 生成失败: {exc}")
                        # 生成失败不影响视频迁移。
        # 重复下载检测：下载器把已迁移的文件重新下载回来 → 直接处置，不再入队
        # 注意：源路径的符号链接本身也会被 os.walk 枚举到，此处跳过软链
        # （软链是迁移成功后特意留下的占位，不是重复下载的文件）
        dedup_handled = set()
        if re_download_action != "keep" and task.get("after_action", "delete") == "delete":
            for fn in files:
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, src)
                if (os.path.islink(full) or rel.lower().endswith(".nfo")
                        or fn.endswith(".moving") or any(fn.endswith(s) for s in ignores) or fn.startswith(".")
                        or (includes and not _match_any(rel, fn, includes))
                        or (excludes and _match_any(rel, fn, excludes))):
                    continue
                if not db.has_migrated_rel(task["id"], rel) or not _stable(full, wait):
                    continue
                try:
                    size = os.path.getsize(full)
                except OSError:
                    continue
                if size <= 0:
                    continue
                target_chk = db.get_migrated_dst(task["id"], rel)
                try:
                    source_snapshot = os.lstat(full)
                except OSError:
                    continue
                # 旧记录或挂载不可靠时保留源；任何删除必须先比较完整内容。
                if (not target_chk or not _within_destination(target_chk, task["dst_dir"])
                        or not transfer._same_content(full, target_chk)):
                    continue
                if re_download_action == "skip":
                    dedup_handled.add(rel)
                    _bump("dedup_skipped")
                    continue
                try:
                    if transfer._mount_identity(task["dst_dir"]) != initial_device:
                        continue
                except OSError:
                    continue
                deleted, reason = transfer._remove_verified_source(
                    full, target_chk, source_snapshot, task["dst_dir"], initial_device)
                if not deleted:
                    db.add_log(task["id"], None, full, target_chk, size, 0,
                               "failed", reason)
                    continue
                dedup_handled.add(rel)
                _bump("dedup_deleted")
                db.add_log(task["id"], None, full, target_chk, size, 0,
                           "success", "重复下载且内容一致，已删除")
                if task.get("symlink_enabled"):
                    try:
                        os.symlink(target_chk, full)
                    except OSError:
                        pass
        for fn in files:
            full = os.path.join(root, fn)
            rel = os.path.relpath(full, src)
            if rel in dedup_handled:
                continue
            if os.path.islink(full):
                if directory_cache is not None and not force_full:
                    # 周期增量只处理真实文件；软链占位的授权/NFO 巡检交给每日全量扫描。
                    _bump("skipped_symlink")
                    continue
                # 只信任指向当前任务目标内的软链，其他软链绝不写入或补齐。
                if not _trusted_link_target(task, rel, full):
                    _bump("skipped_symlink")
                    continue
                # 软链占位：检查目标是否被穿透写污染（nfo 修正版被覆盖）
                if douyin_nfo_enabled and fn.lower().endswith(".nfo") and task.get("symlink_enabled"):
                    try:
                        expected = db.get_migrated_size(task["id"], rel)
                        actual = os.path.getsize(full)
                        with open(full, "rb") as f:
                            raw = f.read()
                        fixed, _ = nfo_fix.prepare_nfo(raw, cfg.get("nfo_fix_enabled"))
                        if expected >= 0 and (actual != expected or raw != fixed):
                            # 已污染：保留网盘目标，入队供后续核查；不得在扫描时
                            # 记为成功或自动覆盖外部改写的内容。
                            dst_p = db.get_migrated_dst(task["id"], rel) or os.path.realpath(full)
                            if not _within_destination(dst_p, task["dst_dir"]):
                                dst_p = os.path.realpath(full)
                            if db.enqueue(task["id"], full, rel, dst_p, actual):
                                _bump("enqueued")
                                db.add_log(task["id"], None, full, dst_p, actual,
                                           0, "conflict", "NFO 软链目标已被改写，已入队供核查；不自动覆盖")
                    except (OSError, ValueError) as exc:
                        db.add_log(task["id"], None, full, os.path.realpath(full),
                                   0, 0, "failed", f"NFO 软链污染检查失败: {exc}")
                else:
                    _bump("skipped_symlink")
                # nfo 补齐：dysync 重下/时序竞态可能跳过刮削，导致网盘有视频没 nfo。
                # 对软链视频检查网盘目标位置，缺 nfo 直接生成写入网盘（不碰软链）
                if douyin_nfo_enabled and task.get("symlink_enabled"):
                    base, ext = os.path.splitext(fn)
                    if ext.lower() in VIDEO_EXTS:
                        target = os.path.realpath(full)
                        nfo_target = os.path.splitext(target)[0] + ".nfo"
                        try:
                            # 此路径直接写云盘而不经迁移流程，写入前必须重查挂载身份。
                            if transfer._mount_identity(task["dst_dir"]) != initial_device:
                                raise OSError("目标文件系统已切换，停止 NFO 补齐")
                            if os.path.isfile(target):
                                ok, info = backfill_nfo_for_link(full, target)
                                if ok:
                                    db.add_log(task["id"], None, full, info,
                                               os.path.getsize(info), 0,
                                               "success", "网盘缺 nfo，已按抖音规则补齐生成")
                                elif info != "已存在 nfo":
                                    db.add_log(task["id"], None, full, nfo_target,
                                               0, 0, "failed", f"网盘 NFO 补齐失败: {info}")
                            else:
                                db.add_log(task["id"], None, full, target,
                                           0, 0, "skipped", "软链视频目标不是普通文件，未补齐 NFO")
                        except (OSError, ValueError) as exc:
                            db.add_log(task["id"], None, full, nfo_target,
                                       0, 0, "failed", f"网盘 NFO 补齐路径或挂载检查失败: {exc}")
                continue  # 软链占位跳过（已迁移留的软链不是迁移对象）
            if fn.endswith(".moving") or any(fn.endswith(s) for s in ignores) or fn.startswith("."):
                continue
            rel = os.path.relpath(full, src)
            companion_included = False
            if includes and (fn.lower().endswith(".nfo") or
                             (filename_nfo_enabled and
                              os.path.splitext(fn)[1].lower() in POSTER_EXTS)):
                stem, extension = os.path.splitext(fn)
                video_stem = (stem[:-7] if extension.lower() in POSTER_EXTS
                              and stem.lower().endswith("-poster") else stem)
                companion_included = any(
                    os.path.splitext(name)[0].casefold() == video_stem.casefold()
                    and os.path.splitext(name)[1].lower() in VIDEO_EXTS
                    and (not os.path.islink(os.path.join(root, name))
                         or _trusted_link_target(
                             task, os.path.relpath(os.path.join(root, name), src),
                             os.path.join(root, name)))
                    and not any(name.endswith(s) for s in ignores)
                    and not (excludes and _match_any(
                        os.path.relpath(os.path.join(root, name), src), name, excludes))
                    and _match_any(os.path.relpath(os.path.join(root, name), src), name, includes)
                    for name in files)
            if includes and not (_match_any(rel, fn, includes) or companion_included):
                continue
            if excludes and _match_any(rel, fn, excludes):
                continue
            # 两种扫描入口都必须经过相同的静默与采样校验。
            if not _stable(full, wait):
                if next_entries is not None:
                    identity, children, _ = next_entries[root]
                    next_entries[root] = (identity, children, True)
                continue
            try:
                current_signature = _signature(os.stat(full))
            except OSError:
                continue
            # 入队所记身份必须是本轮首采样且经过末次静默校验的同一文件。
            if samples.get(full) != current_signature:
                if next_entries is not None:
                    identity, children, _ = next_entries[root]
                    next_entries[root] = (identity, children, True)
                continue
            size = current_signature[2]
            prior_dst = db.get_migrated_dst(task["id"], rel) if fn.lower().endswith(".nfo") else None
            if (fn.lower().endswith(".nfo") and db.has_migrated_rel(task["id"], rel) and not prior_dst
                    and task.get("path_rule") == "by_date"
                    and not db.has_migrated_destination(task["id"], rel)):
                db.add_log(task["id"], None, full, "", size, 0, "skipped",
                           "旧迁移记录缺少实际日期目标，保留源文件；请核查历史目标位置")
                continue
            if prior_dst and not _within_destination(prior_dst, task["dst_dir"]):
                prior_dst = None
            dst = prior_dst or compute_dst(task["dst_dir"], rel, task.get("path_rule", "keep_structure"), date_str)
            if fn.lower().endswith(".nfo"):
                try:
                    with open(full, "rb") as f:
                        expected, _ = nfo_fix.prepare_nfo(f.read(), cfg.get("nfo_fix_enabled"))
                    with open(dst, "rb") as f:
                        if f.read() == expected:
                            continue
                except OSError:
                    pass  # 目标缺失或不可读时必须入队，不得删除源文件
            if db.enqueue(task["id"], full, rel, dst, size,
                          source_snapshot=current_signature if not fn.lower().endswith(".nfo") else None):
                _bump("enqueued")

    if directory_cache is not None:
        directory_cache["key"] = cache_key
        directory_cache["entries"] = next_entries
    return stats
