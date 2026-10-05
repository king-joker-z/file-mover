"""扫描源目录 → 稳定性检测 → 入队"""
import os
import time
from typing import Optional, Tuple, List
import fnmatch
from . import db, config


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


def scan_task(task: dict):
    """扫描一个任务的源目录并入队新文件"""
    src = task["src_dir"]
    cfg = config.get()
    if not os.path.isdir(src):
        return
    includes = _parse_patterns(task.get("include_patterns"))
    excludes = _parse_patterns(task.get("exclude_patterns"))
    ignores = cfg.get("ignore_suffixes", [])
    date_str = time.strftime("%Y-%m-%d")

    for root, _dirs, files in os.walk(src):
        for fn in files:
            if any(fn.endswith(s) for s in ignores) or fn.startswith("."):
                continue
            full = os.path.join(root, fn)
            rel = os.path.relpath(full, src)
            if includes and not _match_any(rel, fn, includes):
                continue
            if excludes and _match_any(rel, fn, excludes):
                continue
            # 稳定性检测
            if cfg.get("stable_check") and not _stable(full, cfg.get("stable_check_seconds", 2)):
                continue
            try:
                size = os.path.getsize(full)
            except OSError:
                continue
            dst = compute_dst(task["dst_dir"], rel, task.get("path_rule", "keep_structure"), date_str)
            db.enqueue(task["id"], full, rel, dst, size)
