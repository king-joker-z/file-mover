"""扫描源目录 → 稳定性检测 → 入队"""
import os
import time
from typing import Optional, Tuple, List
import fnmatch
from . import db, config, nfo_fix, transfer
from .douyin_nfo import ensure_nfo, backfill_nfo_for_link, VIDEO_EXTS


def _parse_patterns(raw: str) -> list[str]:
    return [p.strip() for p in (raw or "").split(",") if p.strip()]


def _match_any(rel_path: str, name: str, patterns: List[str]) -> bool:
    for p in patterns:
        if fnmatch.fnmatch(name, p) or fnmatch.fnmatch(rel_path, p):
            return True
    return False


def _stable(path: str, wait: int) -> bool:
    """两次采样大小一致视为写入完成"""
    try:
        s1 = os.path.getsize(path)
    except OSError:
        return False
    time.sleep(wait)
    try:
        s2 = os.path.getsize(path)
    except OSError:
        return False
    return s1 == s2


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


def scan_task(task: dict, skip_stable_check: bool = False, progress: Optional[dict] = None, on_dir=None) -> dict:
    """扫描一个任务的源目录并入队新文件。
    skip_stable_check=True 时跳过稳定性检测（立即同步用，用户确认文件已就绪）。
    返回统计: {enqueued, dedup_deleted, dedup_skipped, skipped_symlink}"""
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
    # 批量预加载历史相对路径；删除仍需读回逐字节校验目标。
    migrated_rels = db.get_migrated_rels(task["id"])
    initial_device = transfer._mount_identity(task["dst_dir"])
    excludes = _parse_patterns(task.get("exclude_patterns"))
    ignores = cfg.get("ignore_suffixes", [])
    douyin_nfo_enabled = cfg.get("douyin_nfo_enabled", False)
    re_download_action = cfg.get("re_download_action", "delete")  # delete/skip/keep
    date_str = time.strftime("%Y-%m-%d")

    for root, _dirs, files in os.walk(src):
        # 每层目录回调: 更新进度 (sync_now 传入)
        if on_dir:
            try:
                on_dir(root, len(files))
            except Exception:
                pass
        # 抖音 nfo 自动生成：对无伴生 nfo 的视频先生成（这样后续扫描会正常入队 nfo+视频）
        # 注意：软链跳过 —— dysync 留下的软链目标可能悬空，避免 ensure_nfo 对软链做无用功
        if douyin_nfo_enabled:
            for fn in list(files):
                full_p = os.path.join(root, fn)
                rel_video = os.path.relpath(full_p, src)
                if (os.path.islink(full_p) or fn.startswith(".")
                        or any(fn.endswith(s) for s in ignores)
                        or (includes and not _match_any(rel_video, fn, includes))
                        or (excludes and _match_any(rel_video, fn, excludes))
                        or (not skip_stable_check and cfg.get("stable_check")
                            and not _stable(full_p, cfg.get("stable_check_seconds", 2)))):
                    continue
                base, ext = os.path.splitext(fn)
                if ext.lower() in VIDEO_EXTS and not os.path.lexists(os.path.join(root, base + ".nfo")):
                    try:
                        created, _ = ensure_nfo(os.path.join(root, fn))
                        if created:
                            files.append(base + ".nfo")
                    except OSError:
                        pass  # 生成失败不影响迁移
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
                if not skip_stable_check and cfg.get("stable_check") and not _stable(
                        full, cfg.get("stable_check_seconds", 2)):
                    continue
                try:
                    size = os.path.getsize(full)
                except OSError:
                    continue
                if size <= 0 or rel not in migrated_rels:
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
                # 只信任指向当前任务目标内的软链，其他软链绝不写入或补齐。
                if not _within_destination(os.path.realpath(full), task["dst_dir"]):
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
                            # 已污染：大小变化或同大小内容可修正，重新入队
                            # 注意：不能先删目标 —— 污染内容就存在于目标上，
                            # 源软链也指向它，删了内容就丢了（会导致 nfo 丢失）
                            dst_p = db.get_migrated_dst(task["id"], rel) or os.path.realpath(full)
                            if not _within_destination(dst_p, task["dst_dir"]):
                                dst_p = os.path.realpath(full)
                            if db.enqueue(task["id"], full, rel, dst_p, actual):
                                _bump("enqueued")
                                db.add_log(task["id"], None, full, dst_p, actual,
                                           0, "success", "软链穿透写污染，重新入队修正")
                    except OSError:
                        pass
                else:
                    _bump("skipped_symlink")
                # nfo 补齐：dysync 重下/时序竞态可能跳过刮削，导致网盘有视频没 nfo。
                # 对软链视频检查网盘目标位置，缺 nfo 直接生成写入网盘（不碰软链）
                if douyin_nfo_enabled and task.get("symlink_enabled"):
                    base, ext = os.path.splitext(fn)
                    if ext.lower() in VIDEO_EXTS:
                        try:
                            target = os.path.realpath(full)
                            if os.path.isfile(target):
                                ok, info = backfill_nfo_for_link(full, target)
                                if ok:
                                    db.add_log(task["id"], None, full, info,
                                               os.path.getsize(info), 0,
                                               "success", "网盘缺 nfo，已按抖音规则补齐生成")
                        except OSError:
                            pass
                continue  # 软链占位跳过（已迁移留的软链不是迁移对象）
            if fn.endswith(".moving") or any(fn.endswith(s) for s in ignores) or fn.startswith("."):
                continue
            rel = os.path.relpath(full, src)
            companion_included = False
            if fn.lower().endswith(".nfo") and includes:
                base = os.path.splitext(fn)[0]
                companion_included = any(
                    os.path.splitext(name)[0] == base
                    and os.path.splitext(name)[1].lower() in VIDEO_EXTS
                    and _match_any(os.path.relpath(os.path.join(root, name), src), name, includes)
                    for name in files)
            if includes and not (_match_any(rel, fn, includes) or companion_included):
                continue
            if excludes and _match_any(rel, fn, excludes):
                continue
            # 稳定性检测（立即同步时跳过：用户确认文件已就绪）
            if not skip_stable_check and cfg.get("stable_check") and not _stable(full, cfg.get("stable_check_seconds", 2)):
                continue
            try:
                size = os.path.getsize(full)
            except OSError:
                continue
            prior_dst = db.get_migrated_dst(task["id"], rel) if fn.lower().endswith(".nfo") else None
            if (fn.lower().endswith(".nfo") and rel in migrated_rels and not prior_dst
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
            if db.enqueue(task["id"], full, rel, dst, size):
                _bump("enqueued")

    return stats
