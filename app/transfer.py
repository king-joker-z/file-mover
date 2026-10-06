"""单文件迁移执行：直接 move 或 copy+校验+删源"""
import os
import shutil
from typing import Optional, Tuple
import hashlib
from . import nfo_fix
from . import config as app_config
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

    # nfo 预处理：迁移前改写内容（安全模式走 copy 流程天然支持；直接模式下就地改写源文件）
    detail_extra = ""
    if src.lower().endswith(".nfo") and app_config.get().get("nfo_fix_enabled"):
        try:
            with open(src, "r", encoding="utf-8", errors="replace") as f:
                original = f.read()
            fixed, applied = nfo_fix.fix_nfo(original)
            if applied:
                if cfg.get("safe_mode"):
                    # 安全模式：先写好改写内容，再 copy 这份新内容
                    with open(src, "w", encoding="utf-8") as f:
                        f.write(fixed)
                else:
                    with open(src, "w", encoding="utf-8") as f:
                        f.write(fixed)
                src_size = _size(src)
                detail_extra = " +nfo:" + ",".join(applied)
        except Exception as e:
            return "failed", f"nfo 预处理失败: {e}"

    resolved = _resolve_conflict(dst, policy)
    if resolved is None:
        return "conflict", f"目标已存在: {dst}（策略 skip）"
    final_dst, how = resolved

    try:
        # 目标目录预检：网盘挂载不稳定时 makedirs/move 会抛 EIO，
        # 先对目标父目录做一次轻量探测，给出更明确的错误信息
        parent = os.path.dirname(final_dst)
        if not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)

        if cfg.get("safe_mode"):
            tmp = os.path.join(parent,
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
        if detail_extra:
            detail += detail_extra
        if cfg.get("remove_empty_dirs"):
            _remove_empty_parent_dirs(os.path.dirname(src), stop_at=task["src_dir"])
        return "success", detail
    except OSError as e:
        # 清理安全模式残留
        tmp = os.path.join(os.path.dirname(final_dst),
                           "." + os.path.basename(final_dst) + ".moving")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        # 错误分类，帮助定位网盘挂载问题
        import errno as _errno
        eno = e.errno
        hints = {
            _errno.EIO: "（网盘挂载返回 I/O 错误：挂载超时/网盘API失败/缓存盘异常，请检查挂载工具日志）",
            _errno.EACCES: "（权限不足：检查 PUID/PGID 与挂载目录权限）",
            _errno.EPERM: "（权限不足：检查 PUID/PGID 与挂载目录权限）",
            _errno.ENOSPC: "（目标空间不足或挂载缓存盘已满）",
            _errno.ENOENT: "（路径不存在：挂载点可能已掉线）",
            _errno.EBUSY: "（挂载点忙：网盘可能正在同步）",
        }
        hint = hints.get(eno, "")
        return "failed", f"{e} [errno={eno}]{hint}"
    except Exception as e:
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
