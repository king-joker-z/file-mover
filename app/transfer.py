"""单文件迁移执行：直接 move 或 copy+校验+删源"""
import os
import shutil
from typing import Optional, Tuple
import hashlib
import tempfile
from . import db, config


def _size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return -1


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _resolve_conflict(dst: str, policy: str) -> Optional[Tuple[str, str]]:
    """返回 (最终目标路径, 处理方式)；skip 策略冲突时返回 None"""
    if not os.path.lexists(dst):
        return dst, "none"
    if policy == "overwrite":
        return dst, "overwrite"
    if policy == "rename":
        base, ext = os.path.splitext(dst)
        i = 1
        while os.path.lexists(f"{base}({i}){ext}"):
            i += 1
        return f"{base}({i}){ext}", "renamed"
    return None  # skip


def transfer(item: dict, task: dict) -> Tuple[str, str]:
    """执行迁移，返回 (result, detail)。
    result: success / failed / conflict
    """
    src, dst = item["src_path"], item["dst_path"]
    cfg = config.get()
    policy = task.get("conflict_policy", "skip")

    if not os.path.isfile(src):
        return "failed", "源文件不存在（可能已被移动或删除）"
    src_size = _size(src)

    resolved = _resolve_conflict(dst, policy)
    if resolved is None:
        return "conflict", f"目标已存在: {dst}（策略 skip）"
    final_dst, how = resolved

    try:
        os.makedirs(os.path.dirname(final_dst), exist_ok=True)

        if cfg.get("safe_mode"):
            tmp = os.path.join(os.path.dirname(final_dst),
                               "." + os.path.basename(final_dst) + ".moving")
            shutil.copy2(src, tmp)
            if cfg.get("verify_size") and (_size(tmp) != src_size):
                raise IOError("校验失败：目标大小与源不一致")
            os.replace(tmp, final_dst)
            os.remove(src)
        else:
            if how == "overwrite":
                os.remove(final_dst)
            shutil.move(src, final_dst)
            if cfg.get("verify_size") and (_size(final_dst) != src_size):
                raise IOError("校验失败：目标大小与源不一致")

        detail = f"ok ({how})" if how != "none" else "ok"
        if cfg.get("remove_empty_dirs"):
            _remove_empty_parent_dirs(os.path.dirname(src), stop_at=task["src_dir"])
        return "success", detail
    except Exception as e:
        # 清理安全模式残留
        tmp = os.path.join(os.path.dirname(final_dst),
                           "." + os.path.basename(final_dst) + ".moving")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        return "failed", str(e)


def _remove_empty_parent_dirs(start: str, stop_at: str):
    """从 start 向上删除空目录，直到 stop_at（不含）"""
    stop = os.path.abspath(stop_at)
    cur = os.path.abspath(start)
    while cur.startswith(stop) and cur != stop:
        try:
            if os.path.isdir(cur) and not os.listdir(cur):
                os.rmdir(cur)
            else:
                break
        except OSError:
            break
        cur = os.path.dirname(cur)
