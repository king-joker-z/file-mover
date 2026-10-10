"""单文件迁移：先写入目标并读回校验，再按任务策略处理源文件。"""
import hashlib
import os
import shutil
import uuid
from contextlib import contextmanager
from typing import Optional, Tuple

from . import config, db, nfo_fix


def _size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return -1


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _same_content(src: str, dst: str) -> bool:
    try:
        return (os.path.isfile(dst) and not os.path.islink(dst)
                and _size(src) >= 0 and _size(src) == _size(dst)
                and _sha256(src) == _sha256(dst))
    except OSError:
        return False


def _already_migrated(src: str, dst: str) -> bool:
    return _same_content(src, dst)


def _verified_nfo(dst: str, expected: bytes) -> bool:
    try:
        if not os.path.isfile(dst) or os.path.islink(dst):
            return False
        with open(dst, "rb") as stream:
            return stream.read() == expected
    except OSError:
        return False


def _resolve_conflict(dst: str, policy: str) -> Optional[Tuple[str, str]]:
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
    return None


@contextmanager
def _target_parent(dst: str, root: str):
    """固定目标目录的文件描述符；拒绝任何软链目录和越界路径。"""
    base = os.path.abspath(root)
    target = os.path.abspath(dst)
    if os.path.commonpath((base, target)) != base or target == base:
        raise OSError("目标路径不在任务目标目录中")
    parts = os.path.relpath(os.path.dirname(target), base).split(os.sep)
    fd = os.open(base, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts:
            if part == ".":
                continue
            try:
                os.mkdir(part, dir_fd=fd)
            except FileExistsError:
                pass
            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                              dir_fd=fd)
            os.close(fd)
            fd = next_fd
        yield fd
    finally:
        os.close(fd)


def _fd_digest(fd: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with os.fdopen(fd, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _write_verified(dst: str, source: str = None, content: bytes = None, root: str = None):
    """在固定的目录描述符内原子发布并读回，避免父目录软链替换。"""
    if root is None:
        root = os.path.dirname(dst)  # 旧 NFO 软链源文件只在其所在目录内物化
    name = os.path.basename(dst)
    tmp = f".fm-{uuid.uuid4().hex}.moving"
    with _target_parent(dst, root) as parent_fd:
        created = False
        try:
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         0o600, dir_fd=parent_fd)
            created = True
            with os.fdopen(fd, "wb") as stream:
                if content is not None:
                    stream.write(content)
                else:
                    with open(source, "rb") as original:
                        shutil.copyfileobj(original, stream, 1 << 20)
                stream.flush()
                os.fsync(stream.fileno())
            expected_size = len(content) if content is not None else _size(source)
            expected_hash = (hashlib.sha256(content).hexdigest() if content is not None
                             else _sha256(source))
            size, digest = _fd_digest(os.open(tmp, os.O_RDONLY | os.O_NOFOLLOW,
                                              dir_fd=parent_fd))
            if expected_size < 0 or (size, digest) != (expected_size, expected_hash):
                raise IOError("临时文件内容校验失败，源文件已保留")
            os.replace(tmp, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            created = False
            size, digest = _fd_digest(os.open(name, os.O_RDONLY | os.O_NOFOLLOW,
                                              dir_fd=parent_fd))
            if (size, digest) != (expected_size, expected_hash):
                raise IOError("目标文件读回校验失败，源文件已保留")
            return expected_hash
        finally:
            if created:
                try:
                    os.unlink(tmp, dir_fd=parent_fd)
                except OSError:
                    pass


def _write_nfo(dst: str, content: bytes, root: str = None):
    _write_verified(dst, content=content, root=root)


def _materialize_nfo_source(src: str, content: bytes):
    """旧版本遗留的 NFO 软链先转为源目录独立副本，再改写网盘目标。"""
    if os.path.islink(src):
        _write_verified(src, content=content)


def _mount_identity(path: str):
    """校验要求的网盘挂载存在，并记录实际目标文件系统身份。"""
    root = os.environ.get("FILEMOVER_REQUIRED_MOUNT")
    if not root and os.path.commonpath((os.path.abspath(path), "/data/cloud")) == "/data/cloud":
        root = "/data/cloud"  # 现有容器默认的网盘卷：掉载后不得写到底层目录
    if root:
        if not os.path.isabs(root):
            raise OSError("FILEMOVER_REQUIRED_MOUNT 必须是绝对路径")
        root = os.path.abspath(root)
        if (os.path.commonpath((os.path.abspath(path), root)) != root or
                not os.path.ismount(root)):
            raise OSError(f"网盘挂载点不可用: {root}；源文件已保留")
    parent = os.path.realpath(path)
    if root and not os.path.isdir(path):
        raise OSError("目标目录不可访问，源文件已保留")
    while not os.path.exists(parent) and parent != os.path.dirname(parent):
        parent = os.path.dirname(parent)
    return os.stat(parent).st_dev


def _remove_verified_source(src: str, dst: str, expected_stat=None,
                            target_dir: str = None, target_device=None) -> Tuple[bool, str]:
    """暂存后再次核对身份和内容；不删除扫描/迁移期间被替换的源文件。"""
    staging = os.path.join(os.path.dirname(src), f".fm-source-{uuid.uuid4().hex}.moving")
    try:
        if expected_stat is None:
            expected_stat = os.lstat(src)
        if not os.path.isfile(src) or os.path.islink(src):
            return False, "源不是普通文件，已保留"
        os.rename(src, staging)
    except OSError as exc:
        return False, f"源文件暂存失败: {exc}"
    try:
        current = os.lstat(staging)
        identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
        if any(getattr(current, field) != getattr(expected_stat, field) for field in identity):
            raise IOError("源文件身份或时间已变化")
        if not _same_content(staging, dst):
            raise IOError("暂存源与目标内容不同")
        if target_dir is not None and (target_device is not None
                and _mount_identity(target_dir) != target_device):
            raise IOError("目标文件系统已切换")
        latest = os.lstat(staging)
        if any(getattr(latest, field) != getattr(current, field) for field in identity):
            raise IOError("校验期间源文件被改写")
        os.remove(staging)
        return True, ""
    except OSError as exc:
        # 硬链接排他恢复：与新下载文件并发时绝不覆盖；暂存始终保留供核查。
        try:
            os.link(staging, src, follow_symlinks=False)
            return False, f"删除前校验失败；源已恢复，暂存备份保留在 {staging}: {exc}"
        except OSError:
            return False, f"删除前校验失败；源备份保留在 {staging}: {exc}"


def _paths_overlap(first: str, second: str) -> bool:
    a, b = os.path.realpath(first), os.path.realpath(second)
    return os.path.commonpath((a, b)) in (a, b)


def transfer(item: dict, task: dict) -> Tuple[str, str]:
    """执行迁移，返回 (success/failed/conflict, detail)。"""
    src, dst = item["src_path"], item["dst_path"]
    try:
        _mount_identity(task["dst_dir"])
        if _paths_overlap(task["src_dir"], task["dst_dir"]):
            return "failed", "源目录与目标目录重叠，已拒绝迁移"
        if not os.path.isabs(src) or not os.path.isabs(dst):
            return "failed", "源路径或目标路径不是绝对路径"
        source_location = os.path.realpath(os.path.dirname(src)) if os.path.islink(src) else os.path.realpath(src)
        if os.path.commonpath((source_location, os.path.realpath(task["src_dir"]))) != os.path.realpath(task["src_dir"]):
            return "failed", "源路径不在任务源目录中"
        if os.path.commonpath((os.path.realpath(os.path.dirname(dst)), os.path.realpath(task["dst_dir"]))) != os.path.realpath(task["dst_dir"]):
            return "failed", "目标路径不在任务目标目录中"
        if (not os.path.islink(src) and os.path.exists(dst) and os.path.exists(src)
                and os.path.samefile(src, dst)):
            return "failed", "源与目标为同一文件，已拒绝迁移"
    except (OSError, ValueError) as exc:
        return "failed", f"路径校验失败: {exc}"

    cfg = config.get()
    policy = task.get("conflict_policy", "skip")
    if not os.path.isfile(src):
        return "failed", "源文件不存在（可能已被移动或删除）"
    is_nfo = src.lower().endswith(".nfo")
    if is_nfo and os.path.islink(src):
        linked = os.path.realpath(src)
        if (os.path.commonpath((linked, os.path.realpath(task["dst_dir"])))
                != os.path.realpath(task["dst_dir"])):
            return "failed", "NFO 软链不指向任务目标目录，已保留源文件"
    if not is_nfo and os.path.islink(src):
        return "failed", "源是符号链接，禁止作为普通文件迁移"
    detail_extra = ""
    content = None
    if is_nfo:
        try:
            with open(src, "rb") as stream:
                raw = stream.read()
            content, applied = nfo_fix.prepare_nfo(raw, cfg.get("nfo_fix_enabled"))
            if applied:
                detail_extra = " +nfo:" + ",".join(applied)
        except OSError as exc:
            return "failed", f"NFO 预处理失败: {exc}"

    resolved = _resolve_conflict(dst, policy)
    if resolved is None:
        if is_nfo and _verified_nfo(dst, content):
            try:
                _materialize_nfo_source(src, raw)
            except OSError as exc:
                return "failed", f"NFO 源备份失败: {exc}"
            return "success", "ok (NFO 已校验，源文件保留)" + detail_extra
        prior = db.get_migrated_dst(task["id"], item["rel_path"]) if is_nfo else None
        if (is_nfo and prior == dst
                and db.migrated_destination_matches(task["id"], item["rel_path"], dst)):
            resolved = (dst, "overwrite")
        elif not is_nfo and _same_content(src, dst):
            if task.get("after_action", "delete") == "delete":
                try:
                    _mount_identity(task["dst_dir"])
                    snapshot = os.lstat(src)
                    if not _same_content(src, dst):
                        return "failed", "源文件已变化或目标不可读，已保留源文件"
                    deleted, reason = _remove_verified_source(
                        src, dst, snapshot, task["dst_dir"], _mount_identity(task["dst_dir"]))
                    if not deleted:
                        return "failed", reason
                except OSError as exc:
                    return "failed", f"源文件删除失败: {exc}"
                if task.get("symlink_enabled"):
                    _symlink_source(src, dst)
            return "success", "ok (目标内容一致)"
        else:
            return "conflict", f"目标已存在: {dst}（策略 skip）"
    final_dst, how = resolved
    try:
        if is_nfo:
            _materialize_nfo_source(src, raw)
            if not _verified_nfo(final_dst, content):
                _write_nfo(final_dst, content, root=task["dst_dir"])
            if not _verified_nfo(final_dst, content):
                raise IOError("NFO 目标文件读回校验失败，源文件已保留")
            item["dst_path"] = final_dst
            return "success", f"ok (NFO 已读回校验，源文件保留){detail_extra}"

        # 普通文件也使用先复制、完整性校验、原子替换的流程。
        # 挂载可能在 close 后丢写，读回校验失败时绝不能删除源文件。
        target_device = _mount_identity(task["dst_dir"])
        source_size = _size(src)
        source_stat = os.stat(src)
        source_hash = _sha256(src)
        _write_verified(final_dst, source=src, root=task["dst_dir"])
        item["dst_path"] = final_dst
        current_stat = os.stat(src)
        if (current_stat.st_ino != source_stat.st_ino or current_stat.st_dev != source_stat.st_dev
                or current_stat.st_mtime_ns != source_stat.st_mtime_ns or _size(src) != source_size
                or _sha256(src) != source_hash):
            return "failed", "复制期间源文件变化，源文件已保留"
        detail = f"ok ({how})" if how != "none" else "ok"
        if task.get("after_action", "delete") == "delete":
            try:
                if _mount_identity(task["dst_dir"]) != target_device:
                    return "failed", "目标文件系统已切换，源文件已保留"
                current_stat = os.stat(src)
                if (current_stat.st_ino != source_stat.st_ino or current_stat.st_dev != source_stat.st_dev
                        or current_stat.st_mtime_ns != source_stat.st_mtime_ns
                        or not _same_content(src, final_dst)):
                    return "failed", "删除前源文件已变化或目标不可读，已保留源文件"
                deleted, reason = _remove_verified_source(
                    src, final_dst, current_stat, task["dst_dir"], target_device)
                if not deleted:
                    return "failed", reason
            except OSError as exc:
                return "failed", f"源文件删除失败: {exc}"
            if task.get("symlink_enabled"):
                if _symlink_source(src, final_dst):
                    detail += "，+symlink"
                else:
                    detail += "，symlink创建失败(不影响迁移)"
            if cfg.get("remove_empty_dirs") and not _has_symlink_in_dir(os.path.dirname(src)):
                _remove_empty_parent_dirs(os.path.dirname(src), stop_at=task["src_dir"])
        return "success", detail
    except OSError as exc:
        return "failed", f"迁移失败，源文件已保留: {exc}"
    except Exception as exc:
        return "failed", f"迁移失败，源文件已保留: {exc}"


def _symlink_source(src: str, dst: str) -> bool:
    try:
        if os.path.lexists(src):
            return False
        os.makedirs(os.path.dirname(src), exist_ok=True)
        os.symlink(dst, src)
        return True
    except OSError:
        return False


def _has_symlink_in_dir(directory: str) -> bool:
    try:
        return any(os.path.islink(os.path.join(directory, entry)) for entry in os.listdir(directory))
    except OSError:
        return False


def _remove_empty_parent_dirs(start: str, stop_at: str):
    """仅清理真正的空目录，不清理任何其他程序的临时文件。"""
    stop = os.path.realpath(stop_at)
    cur = os.path.realpath(start)
    while cur != stop and os.path.commonpath((cur, stop)) == stop:
        try:
            os.rmdir(cur)
        except OSError:
            break
        cur = os.path.dirname(cur)
