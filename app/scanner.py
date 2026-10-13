"""扫描源目录 → 稳定性检测 → 入队"""
import hashlib
import os
import stat
import time
from contextvars import ContextVar
from typing import Optional, Tuple, List
import fnmatch
from . import db, config, nfo_fix, transfer
from .douyin_nfo import (ensure_nfo, ensure_filename_nfo, backfill_nfo_for_video,
                         VIDEO_EXTS, POSTER_EXTS)


def _parse_patterns(raw: str) -> list[str]:
    return [p.strip() for p in (raw or "").split(",") if p.strip()]


def _match_any(rel_path: str, name: str, patterns: List[str]) -> bool:
    for p in patterns:
        if fnmatch.fnmatch(name, p) or fnmatch.fnmatch(rel_path, p):
            return True
    return False


def _exclude_patterns(raw: str, src: str) -> list[str]:
    """允许填写相对源目录的规则，或直接粘贴源目录下的绝对路径。"""
    patterns = []
    source = os.path.normpath(src)
    for pattern in _parse_patterns(raw):
        if os.path.isabs(pattern):
            pattern = os.path.normpath(pattern)
            if pattern == source:
                raise ValueError("不能排除整个源目录")
            if os.path.commonpath((source, pattern)) != source:
                raise ValueError(f"排除路径不在源目录内: {pattern}")
            pattern = os.path.relpath(pattern, source)
        patterns.append(pattern)
    return patterns


# 每次扫描独立保存首轮采样；即使测试/调用方并发扫描也不会串用样本。
_scan_samples = ContextVar("scan_samples", default=None)


def _signature(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def _scan_io_error(action: str, path: str, error: OSError) -> OSError:
    """保留 errno 和实际路径，并说明扫描时失败的文件系统操作。"""
    return OSError(error.errno, f"{action}: {error.strerror or error}", error.filename or path)


def _stable(path: str, wait: int) -> bool:
    """校验本轮首采样与当前 size/mtime/inode；I/O 故障不能伪装成不稳定。"""
    context = _scan_samples.get()
    if context is None:
        # 未经 scan_task 首采样/静默期初始化的调用不能宣称下载完成。
        return False
    samples, quiet, generated = context
    try:
        current = os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError as error:
        raise _scan_io_error("复查文件状态失败", path, error) from error
    if not stat.S_ISREG(current.st_mode):
        return False
    first = samples.get(path)
    if first is None:
        return False
    return (first == _signature(current)
            and (path in generated or time.time() - current.st_mtime >= quiet))


def scan_task(task: dict, skip_stable_check: bool = False, progress: Optional[dict] = None,
              on_dir=None, force_full: bool = False) -> dict:
    """每轮完整枚举源目录；force_full 仅控制历史视频的 NFO 补齐。"""
    token = _scan_samples.set(None)
    try:
        return _scan_task(task, progress=progress, on_dir=on_dir, force_full=force_full)
    finally:
        _scan_samples.reset(token)


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


def _backfill_migrated_nfo(task: dict, filename_format: bool, device: int):
    """仅在全量扫描中巡检已迁移视频；每个目标目录只枚举一次。"""
    grouped = {}
    source_names = {}
    for rel, dst, size, digest in db.list_migrated_videos(task["id"], VIDEO_EXTS):
        if (not rel or os.path.isabs(rel) or ".." in rel.split(os.sep)
                or not _within_destination(dst, task["dst_dir"])):
            continue
        grouped.setdefault(os.path.dirname(dst), []).append((rel, dst, size, digest))
    for directory, videos in grouped.items():
        try:
            names = os.listdir(directory)
        except FileNotFoundError:
            continue  # 已迁移目标丢失时绝不创建目录或凭旧记录补写。
        existing_nfo = {os.path.splitext(name)[0].casefold() for name in names
                        if os.path.splitext(name)[1].casefold() == ".nfo"}
        for rel, dst, size, expected_digest in videos:
            stem = os.path.splitext(os.path.basename(dst))[0].casefold()
            if stem in existing_nfo:
                continue
            original = os.path.join(task["src_dir"], rel)
            source_dir = os.path.dirname(original)
            original_stem = os.path.splitext(os.path.basename(original))[0].casefold()
            if source_dir not in source_names:
                try:
                    source_names[source_dir] = {name.casefold() for name in os.listdir(source_dir)}
                except FileNotFoundError:
                    source_names[source_dir] = set()
            # 源视频或源 NFO 还在时交由正常扫描/迁移，不抢先合成低质量 NFO。
            if (os.path.basename(original).casefold() in source_names[source_dir]
                    or original_stem + ".nfo" in source_names[source_dir]):
                continue
            try:
                target_info = os.lstat(dst)
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(target_info.st_mode) or target_info.st_size != size:
                continue
            # 只在缺 NFO 时完整读回视频；固定读回对象，目标被替换则不补写。
            checksum = hashlib.sha256()
            try:
                with open(dst, "rb") as stream:
                    before = os.fstat(stream.fileno())
                    if not stat.S_ISREG(before.st_mode) or before.st_size != size:
                        continue
                    for chunk in iter(lambda: stream.read(1 << 20), b""):
                        checksum.update(chunk)
                    after = os.fstat(stream.fileno())
                latest = os.lstat(dst)
            except FileNotFoundError:
                continue
            identity = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
            if (checksum.hexdigest() != expected_digest
                    or identity(before) != identity(after)
                    or identity(after) != identity(latest)):
                continue
            if transfer._mount_identity(task["dst_dir"]) != device:
                raise OSError("目标文件系统已切换，停止 NFO 补齐")
            if filename_format:
                created, nfo_path = ensure_filename_nfo(
                    original, names, poster_filenames=names, target_path=dst)
            else:
                created, nfo_path = backfill_nfo_for_video(original, dst, filenames=names)
            if created:
                existing_nfo.add(stem)
                names.append(os.path.basename(nfo_path))
                db.add_log(task["id"], None, None, nfo_path, 0, 0,
                           "nfo_backfilled", "已迁移视频缺失 NFO，目标侧补齐")


def _scan_task(task: dict, progress: Optional[dict] = None, on_dir=None,
               force_full: bool = False) -> dict:
    """扫描一个任务的源目录并入队已静默文件。"""
    src = task["src_dir"]
    cfg = config.get()
    if not os.path.isabs(src) or not os.path.isabs(task["dst_dir"]):
        raise ValueError("迁移目录必须是绝对路径")
    a, b = os.path.realpath(src), os.path.realpath(task["dst_dir"])
    if os.path.commonpath((a, b)) in (a, b):
        raise ValueError("源目录与目标目录重叠，已拒绝扫描")
    stats = {"enqueued": 0}
    # progress 为外部传入的共享进度字典（立即同步用），扫描过程实时更新
    def _bump(key, n=1):
        stats[key] += n
        if progress is not None:
            progress[key] = stats[key]
    # isdir() 会将 EIO 吞成 False，不能把损坏的源目录报告为成功的空扫描。
    try:
        source_info = os.stat(src)
    except FileNotFoundError:
        return stats
    except OSError as error:
        raise _scan_io_error("读取源目录状态失败", src, error) from error
    if not stat.S_ISDIR(source_info.st_mode):
        return stats
    includes = _parse_patterns(task.get("include_patterns"))
    initial_device = transfer._mount_identity(task["dst_dir"])
    excludes = _exclude_patterns(task.get("exclude_patterns"), src)
    ignores = cfg.get("ignore_suffixes", [])
    filename_nfo_enabled = task.get("filename_nfo_enabled", False)
    # 两种生成格式不能在同一任务上竞争同一个 .nfo 路径。
    douyin_nfo_enabled = cfg.get("douyin_nfo_enabled", False) and not filename_nfo_enabled
    date_str = time.strftime("%Y-%m-%d")
    quiet = max(0, float(cfg.get("stable_quiet_seconds", 60)))
    wait = cfg.get("stable_check_seconds", 2) if cfg.get("stable_check") else 0
    # 先收集整轮目录和首采样，统一等待一次；不对每个文件或每层目录 sleep。
    # 完整收集目录后才入队；目录枚举失败不能被当作空目录。
    def raise_walk_error(error):
        raise _scan_io_error("枚举目录失败", error.filename or src, error) from error
    directories = []
    for root, dirs, files in os.walk(src, onerror=raise_walk_error):
        # 明确排除的目录在进入前剪枝；未排除的目录故障仍必须中断扫描。
        dirs[:] = [name for name in dirs if not _match_any(
            os.path.relpath(os.path.join(root, name), src), name, excludes)]
        directories.append((root, files))
    samples = {}
    for root, files in directories:
        for name in files:
            path = os.path.join(root, name)
            rel = os.path.relpath(path, src)
            if (name.startswith(".") or name.endswith(".moving")
                    or any(name.endswith(s) for s in ignores)
                    or (excludes and _match_any(rel, name, excludes))):
                continue
            try:
                info = os.lstat(path)
                if not stat.S_ISREG(info.st_mode):
                    continue
                samples[path] = _signature(info)
            except FileNotFoundError:
                continue
            except OSError as error:
                raise _scan_io_error("首次读取文件状态失败", path, error) from error
    if not directories:
        if force_full and (filename_nfo_enabled or douyin_nfo_enabled):
            _backfill_migrated_nfo(task, filename_nfo_enabled, initial_device)
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
                if (fn.startswith(".")
                        or any(fn.endswith(s) for s in ignores)
                        or (includes and not _match_any(rel_video, fn, includes))
                        or (excludes and _match_any(rel_video, fn, excludes))
                        or not _stable(full_p, wait)):
                    continue
                base, ext = os.path.splitext(fn)
                if ext.lower() in VIDEO_EXTS:
                    # 重下视频的 NFO 若已在目标侧迁移完成，不要在源端重新合成一份，
                    # 否则生成文件会与真实 NFO 冲突并留下无法处理的源文件。
                    nfo_rel = os.path.relpath(os.path.join(root, base + ".nfo"), src)
                    prior_nfo = db.get_migrated_dst(task["id"], nfo_rel)
                    if prior_nfo and _within_destination(prior_nfo, task["dst_dir"]):
                        try:
                            if stat.S_ISREG(os.lstat(prior_nfo).st_mode):
                                continue
                        except FileNotFoundError:
                            pass
                    try:
                        if filename_nfo_enabled:
                            created, _ = ensure_filename_nfo(full_p, files, poster_files)
                        else:
                            created, _ = ensure_nfo(full_p, filenames=files)
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
                        # NFO 生成失败时不得删掉视频唯一的元信息来源；
                        # 下次扫描重试，已入队视频由调度熔断暂停。
                        raise
        for fn in files:
            full = os.path.join(root, fn)
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
                    and os.path.join(root, name) in samples
                    and not any(name.endswith(s) for s in ignores)
                    and not (excludes and _match_any(
                        os.path.relpath(os.path.join(root, name), src), name, excludes))
                    and _match_any(os.path.relpath(os.path.join(root, name), src), name, includes)
                    for name in files)
            if includes and not (_match_any(rel, fn, includes) or companion_included):
                continue
            if excludes and _match_any(rel, fn, excludes):
                continue
            # 已静默且仍是本轮枚举的普通文件才入队。
            if not _stable(full, wait):
                continue
            try:
                current_signature = _signature(os.lstat(full))
            except FileNotFoundError:
                continue
            except OSError as error:
                raise _scan_io_error("末次读取文件状态失败", full, error) from error
            # 本轮首采样与末次校验必须仍是同一文件。
            if samples.get(full) != current_signature:
                continue
            size = current_signature[2]
            prior_dst = db.get_migrated_dst(task["id"], rel) if task.get("after_action", "delete") == "delete" else None
            if (fn.lower().endswith(".nfo") and db.has_migrated_rel(task["id"], rel) and not prior_dst
                    and task.get("path_rule") == "by_date"
                    and not db.has_migrated_destination(task["id"], rel)):
                db.add_log(task["id"], None, full, "", size, 0, "skipped",
                           "旧迁移记录缺少实际日期目标，保留源文件；请核查历史目标位置")
                continue
            if prior_dst and not _within_destination(prior_dst, task["dst_dir"]):
                prior_dst = None
            dst = prior_dst or compute_dst(task["dst_dir"], rel, task.get("path_rule", "keep_structure"), date_str)
            if task.get("after_action") == "keep":
                # 保留源文件的复制任务每轮也检查实际目标；内容一致就不再重复复制/重命名。
                recorded = db.get_migrated_dst(task["id"], rel)
                candidate = recorded if recorded and _within_destination(recorded, task["dst_dir"]) else dst
                try:
                    if fn.lower().endswith(".nfo"):
                        with open(full, "rb") as f:
                            expected, _ = nfo_fix.prepare_nfo(f.read(), cfg.get("nfo_fix_enabled"))
                        if transfer._verified_nfo(candidate, expected):
                            continue
                    elif transfer._matching_digest(full, candidate) is not None:
                        continue
                except FileNotFoundError:
                    pass  # 目标缺失则正常迁移；I/O 错误必须上抛。
            if db.enqueue(task["id"], full, rel, dst, size):
                _bump("enqueued")

    if force_full and (filename_nfo_enabled or douyin_nfo_enabled):
        _backfill_migrated_nfo(task, filename_nfo_enabled, initial_device)
    return stats
