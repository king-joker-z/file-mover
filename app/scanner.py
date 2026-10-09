"""扫描源目录 → 稳定性检测 → 入队"""
import os
import time
from typing import Optional, Tuple, List
import fnmatch
from . import db, config
from .douyin_nfo import ensure_nfo, VIDEO_EXTS


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


def scan_task(task: dict, skip_stable_check: bool = False, progress: Optional[dict] = None, on_dir=None) -> dict:
    """扫描一个任务的源目录并入队新文件。
    skip_stable_check=True 时跳过稳定性检测（立即同步用，用户确认文件已就绪）。
    返回统计: {enqueued, dedup_deleted, dedup_skipped, skipped_symlink}"""
    src = task["src_dir"]
    cfg = config.get()
    stats = {"enqueued": 0, "dedup_deleted": 0, "dedup_skipped": 0, "skipped_symlink": 0}
    # progress 为外部传入的共享进度字典（立即同步用），扫描过程实时更新
    def _bump(key, n=1):
        stats[key] += n
        if progress is not None:
            progress[key] = stats[key]
    if not os.path.isdir(src):
        return stats
    includes = _parse_patterns(task.get("include_patterns"))
    # 批量预加载已迁移指纹集合 (避免逐文件 DB 查询, 900+ 文件时性能关键)
    migrated_set = db.get_migrated_set(task["id"])
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
            for fn in files:
                full_p = os.path.join(root, fn)
                if os.path.islink(full_p):
                    continue
                base, ext = os.path.splitext(fn)
                if ext.lower() in VIDEO_EXTS and not os.path.isfile(os.path.join(root, base + ".nfo")):
                    try:
                        ensure_nfo(os.path.join(root, fn))
                    except OSError:
                        pass  # 生成失败不影响迁移
        # 重复下载检测：下载器把已迁移的文件重新下载回来 → 直接处置，不再入队
        # 注意：源路径的符号链接本身也会被 os.walk 枚举到，此处跳过软链
        # （软链是迁移成功后特意留下的占位，不是重复下载的文件）
        if re_download_action != "keep":
            for fn in files:
                full = os.path.join(root, fn)
                if os.path.islink(full):
                    _bump("skipped_symlink")
                    continue  # 软链占位跳过
                rel = os.path.relpath(full, src)
                try:
                    size = os.path.getsize(full)
                except OSError:
                    continue
                if size > 0 and (rel, size) in migrated_set:
                    if re_download_action == "delete":
                        try:
                            os.remove(full)
                        except OSError:
                            pass
                        _bump("dedup_deleted")
                        db.add_log(task["id"], None, full, "", size, 0,
                                   "success", "重复下载（此前已迁移过），已删除")
                        # 软链重建：保持源路径"文件存在"，dysync 对账不再重下
                        if task.get("symlink_enabled"):
                            try:
                                target = os.path.join(task["dst_dir"], rel)
                                if os.path.isfile(target):
                                    os.symlink(target, full)
                            except OSError:
                                pass
                    else:  # skip
                        _bump("dedup_skipped")
                        db.add_log(task["id"], None, full, "", size, 0,
                                   "skipped", "重复下载（此前已迁移过），保留跳过")
        for fn in files:
            full = os.path.join(root, fn)
            if os.path.islink(full):
                _bump("skipped_symlink")
                continue  # 软链占位跳过（已迁移留的软链不是迁移对象）
            if any(fn.endswith(s) for s in ignores) or fn.startswith("."):
                continue
            rel = os.path.relpath(full, src)
            if includes and not _match_any(rel, fn, includes):
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
            dst = compute_dst(task["dst_dir"], rel, task.get("path_rule", "keep_structure"), date_str)
            if db.enqueue(task["id"], full, rel, dst, size):
                _bump("enqueued")

    return stats
