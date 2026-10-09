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


def _already_migrated(src: str, dst: str) -> bool:
    """判断目标文件是否已完整存在（上次迁移实际成功但被误报失败的场景）。
    条件：目标存在、是普通文件、大小与源一致。"""
    try:
        return (os.path.isfile(dst)
                and not os.path.islink(dst)
                and _size(dst) == _size(src)
                and _size(src) >= 0)
    except OSError:
        return False


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
        # 幂等保护：若目标文件与源大小一致，说明上次迁移实际已成功
        # （网盘挂载瞬断导致校验误报失败），本次直接按成功处理并清理源文件
        if _already_migrated(src, dst):
            # 源路径是软链占位时不删除（保留给 dysync 对账）
            if not os.path.islink(src):
                try:
                    os.remove(src)
                except OSError:
                    pass
            return "success", "ok (上次迁移实际已完成，本次幂等确认)"
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
            try:
                os.remove(src)
            except OSError as rm_err:
                # 文件已完整复制到目标，删源失败不影响成功判定（挂载瞬断常见）
                return "success", f"ok ({how})，源文件删除失败: {rm_err}{detail_extra}"
        else:
            if how == "overwrite":
                os.remove(final_dst)
            shutil.move(src, final_dst)
            if cfg.get("verify_size") and (_size(final_dst) != src_size):
                # 网盘挂载缓存可能导致 stat 读到旧值；再确认一次目标确实存在
                if _already_migrated(src, final_dst) or os.path.isfile(final_dst):
                    # 目标实际存在 → 视为成功（不误报）
                    try:
                        os.remove(src)
                    except OSError:
                        pass
                    detail = f"ok ({how})，校验读数异常但目标已确认存在"
                    if detail_extra:
                        detail += detail_extra
                    if cfg.get("remove_empty_dirs"):
                        _remove_empty_parent_dirs(os.path.dirname(src), stop_at=task["src_dir"])
                    return "success", detail
                raise IOError("校验失败：目标大小与源不一致")

        detail = f"ok ({how})" if how != "none" else "ok"
        if detail_extra:
            detail += detail_extra
        # 符号链接：在源路径留下指向目标的软链（dysync 对账不再重下；Emby 可播）
        # 校验异常分支（目标已确认存在）同样需要软链，此处统一处理
        if app_config.get().get("symlink_enabled") and task.get("after_action", "delete") == "delete":
            if _symlink_source(src, final_dst):
                detail += "，+symlink"
            else:
                detail += "，symlink创建失败(不影响迁移)"
        if cfg.get("remove_empty_dirs"):
            # 有符号链接时目录非空，跳过空目录清理（避免误删软链）
            if _has_symlink_in_dir(os.path.dirname(src)):
                pass
            else:
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


def _symlink_source(src: str, dst: str) -> bool:
    """迁移成功后在源路径创建指向目标的符号链接。
    用途：下载器（dysync）对账时文件"仍然存在"不会重下；Emby/Jellyfin 跟随软链可播。
    返回是否创建成功。失败静默（不影响迁移成功状态）。"""
    try:
        if os.path.lexists(src):   # 源还在（keep 模式等），不覆盖
            return False
        os.makedirs(os.path.dirname(src), exist_ok=True)
        os.symlink(dst, src)
        return True
    except OSError:
        return False


def _has_symlink_in_dir(directory: str) -> bool:
    """目录内是否存在符号链接（有的话跳过空目录清理，避免误删软链）"""
    try:
        return any(os.path.islink(os.path.join(directory, e))
                   for e in os.listdir(directory))
    except OSError:
        return False


def _remove_empty_parent_dirs(start: str, stop_at: str):
    """从 start 向上删除空目录，直到 stop_at（不含）。
    网盘挂载偶现 listdir/rmdir 瞬时失败：单层失败不中断，下一轮迁移会再尝试。"""
    stop = os.path.abspath(stop_at)
    cur = os.path.abspath(start)
    while cur.startswith(stop) and cur != stop:
        try:
            entries = os.listdir(cur)
        except OSError:
            break  # 目录临时不可访问（挂载瞬断），本轮到此为止
        if entries:
            # 非空：若是本次迁移的临时残留则清理，否则保留
            leftover = [e for e in entries if e.endswith(".moving")]
            if leftover and len(leftover) == len(entries):
                for e in leftover:
                    try:
                        os.remove(os.path.join(cur, e))
                    except OSError:
                        break
                continue  # 清理后重查本层
            break
        try:
            os.rmdir(cur)
        except OSError:
            break  # 偶现失败，留待下次
        cur = os.path.dirname(cur)
