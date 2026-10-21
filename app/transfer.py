"""单文件迁移：按任务校验强度写入目标，再按任务策略处理源文件。

校验强度 verify_mode：
- strict: 发布后读回 + 删除前暂存源/目标再读回（最稳；发布后读回读的是转正后
  目标的同一 inode，全量内容覆盖写入校验能力，不再重复读回临时文件）。
- fast:   仅发布后读回目标一次；删除源前不再暂存、不再重读（推荐大文件）。
- off:    不读回内容，仅核对发布后大小；信任 fsync（最快）。
NFO 小文件始终按 strict 处理。
发布方式：默认 tmp + 原子/预检 rename 转正；全局设置 direct_publish 开启后，
新目标改为最终文件名 O_EXCL 独占创建直写（workaround：部分网盘挂载对
刚上传完的大文件执行 rename 会无限卡死）。
"""
import ctypes
import errno
import hashlib
import logging
import os
import stat
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Optional, Tuple

from . import config, db, nfo_fix

logger = logging.getLogger("uvicorn.error")

# 复制/校验的读写块大小。网盘挂载（极空间 115、CloudDrive2 等）上每次 write/read
# 都是一次跨网络的 RPC，大块能显著减少请求次数、提升吞吐；总流量不变，请求数
# 更少对网盘侧也更友好。内存占用：每个传输线程一个块缓冲（8MB）。
COPY_CHUNK_SIZE = 8 << 20

# 残留临时/暂存文件超过该时长才会被自动清理；
# 正在写入的临时文件 mtime 持续更新，不会被误删。
STALE_TEMP_SECONDS = 24 * 3600
# 同一目标目录两次残留临时文件扫描的最小间隔。网盘挂载上 scandir 是一次
# 列目录网络请求，逐文件迁移时不应每个文件都枚举一次目标目录；残留清理
# 本身针对的是 24 小时前的垃圾，延迟一个扫描间隔没有任何影响。
STALE_TEMP_SCAN_INTERVAL = 600
_stale_temp_scan_at: dict[str, float] = {}
_stale_temp_lock = threading.Lock()

# renameat2(RENAME_NOREPLACE) 仅 Linux（glibc ≥ 2.28 / musl）提供；
# macOS 等平台加载不到符号时回退排他复制。
_renameat2 = None


def _load_renameat2():
    global _renameat2
    if _renameat2 is None:
        try:
            libc = ctypes.CDLL(None, use_errno=True)
            fn = libc.renameat2
            fn.restype = ctypes.c_int
            fn.argtypes = [ctypes.c_int, ctypes.c_char_p,
                           ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
            _renameat2 = fn
        except (OSError, AttributeError):
            _renameat2 = False
    return _renameat2


def _rename_noreplace(src_fd: int, src_name: str, dst_fd: int, dst_name: str) -> bool:
    """renameat2(RENAME_NOREPLACE) 原子转正：绝不覆盖已存在的目标。

    True = 发布成功且临时文件已转正（无需也不能再清理）；
    False = 平台/文件系统不支持，调用方回退排他复制；
    目标被并发创建时抛 EEXIST，与排他创建语义一致。
    """
    fn = _load_renameat2()
    if fn is False:
        return False
    while True:
        result = fn(src_fd, os.fsencode(src_name), dst_fd, os.fsencode(dst_name), 1)
        if result == 0:
            return True
        err = ctypes.get_errno()
        if err == errno.EINTR:
            continue
        if err in (errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP,
                   getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)):
            return False
        raise OSError(err, os.strerror(err), dst_name)


def _report_stage(stage_cb, name: str) -> None:
    """阶段上报；回调异常不得影响迁移本身。"""
    if stage_cb is None:
        return
    try:
        stage_cb(name)
    except Exception:
        pass


def _size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return -1


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(COPY_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _matching_digest(src: str, dst: str) -> Optional[str]:
    """仅内容不同或目标缺失返回 None；I/O 错误上抛，禁止据此删源/覆盖。"""
    try:
        target = os.lstat(dst)
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(target.st_mode):
        return None
    source_size = os.stat(src).st_size
    if source_size != target.st_size:
        return None
    digest = _sha256(src)
    return digest if digest == _sha256(dst) else None


def _verified_nfo(dst: str, expected: bytes) -> bool:
    try:
        info = os.lstat(dst)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(info.st_mode):
        return False
    with open(dst, "rb") as stream:
        return stream.read() == expected


def _resolve_conflict(dst: str, policy: str) -> Optional[Tuple[str, str]]:
    """仅 ENOENT 表示可创建；EIO 等目录故障必须上报，绝不当作空位。"""
    def exists(path):
        try:
            os.lstat(path)
        except FileNotFoundError:
            return False
        return True

    if not exists(dst):
        return dst, "none"
    if policy == "overwrite":
        return dst, "overwrite"
    if policy == "rename":
        base, ext = os.path.splitext(dst)
        i = 1
        while exists(f"{base}({i}){ext}"):
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
        for chunk in iter(lambda: stream.read(COPY_CHUNK_SIZE), b""):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _report_progress(progress_cb, copied: int, total) -> None:
    """复制进度上报；回调异常不得影响迁移本身。"""
    if progress_cb is None:
        return
    try:
        progress_cb(copied, total)
    except Exception:
        pass


def _discard_published(parent_fd: int, name: str, written_identity):
    """发布后校验失败时清理已转正目标，避免残留损坏文件让后续重试落成冲突。

    仅当目标仍是本次写入的 inode 才删除：并发竞争者替换过的文件/软链不动。
    """
    try:
        current = os.lstat(name, dir_fd=parent_fd)  # lstat 本身不跟随软链
    except OSError:
        return
    if (current.st_dev, current.st_ino) != (written_identity.st_dev, written_identity.st_ino):
        return
    try:
        os.unlink(name, dir_fd=parent_fd)
    except OSError:
        pass


def _write_verified(dst: str, source: str = None, content: bytes = None, root: str = None,
                    overwrite: bool = False, verify: str = "strict", progress_cb=None,
                    stage_cb=None, direct: bool = False):
    """在固定的目录描述符内原子发布；verify 决定读回强度。

    strict: 发布后读回目标一次（读的是硬链接/rename 后的同一 inode，覆盖了临时
            文件读回的全部校验能力，不再重复读回），删除源前暂存再读回。
    fast:   仅发布后读回目标一次（仍可发现写入损坏/截断），无暂存文件。
    off:    不读回内容，仅核对发布后大小；信任 fsync，速度最快。
    direct: 直写发布（仅作用于新目标；覆盖流程仍需硬链接备份，不受影响）：
            目标名 O_EXCL 独占创建后直接写入，跳过 tmp 与 rename 转正。
    非覆盖策略不得抢占并发创建的目标。
    progress_cb(copied, total)：按 COPY_CHUNK_SIZE 块上报复制进度，供仪表盘展示速度/ETA。
    """
    if verify not in ("strict", "fast", "off"):
        verify = "strict"
    if root is None:
        root = os.path.dirname(dst)
    name = os.path.basename(dst)
    tmp = f".fm-{uuid.uuid4().hex}.moving"
    pending = tmp  # 失败时需要清理的半成品文件名（direct 模式下是最终目标名）
    with _target_parent(dst, root) as parent_fd:
        created = False
        backup = None
        try:
            if not overwrite and direct:
                # 直写发布：跳过 tmp 与 rename 转正，目标名 O_EXCL 独占创建后直接
                # 写入。适用场景：部分网盘挂载对刚上传完的大文件执行 rename 会
                # 长时间卡死。互斥语义由 O_EXCL 保留；写入中途失败会在 finally
                # 清理半成品，仅进程被硬杀时可能残留部分写入的目标文件。
                _report_stage(stage_cb, "直写发布（跳过 rename）")
                pending = name
            fd = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         0o600, dir_fd=parent_fd)
            created = True
            digest = hashlib.sha256()
            expected_size = 0
            with os.fdopen(fd, "wb") as stream:
                if content is not None:
                    stream.write(content)
                    digest.update(content)
                    expected_size = len(content)
                else:
                    try:
                        total = os.path.getsize(source)
                    except OSError:
                        total = None
                    copied = 0
                    try:
                        with open(source, "rb") as original:
                            for chunk in iter(lambda: original.read(COPY_CHUNK_SIZE), b""):
                                stream.write(chunk)
                                digest.update(chunk)
                                expected_size += len(chunk)
                                copied += len(chunk)
                                _report_progress(progress_cb, copied, total)
                    except OSError as exc:
                        exc._filemover_eio_origin = 'source'
                        raise
                stream.flush()
                _report_stage(stage_cb, "等待挂载确认落盘 (fsync)")
                os.fsync(stream.fileno())
                written_identity = os.fstat(stream.fileno())
            expected_hash = digest.hexdigest()
            if not overwrite:
                if not direct:
                    # 硬链接的 EEXIST 检查与发布不可分割；rename 也不得覆盖外部新文件。
                    try:
                        os.link(tmp, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd,
                                follow_symlinks=False)
                    except OSError as exc:
                        if exc.errno not in (errno.EPERM, errno.EOPNOTSUPP, errno.ENOSYS, errno.EXDEV):
                            raise
                        # 网盘挂载通常既不支持硬链接也不支持 renameat2(NOREPLACE)。
                        # 优先 renameat2 原子转正：零数据拷贝；不支持时回退预检 + 同目录
                        # rename——同样是零数据拷贝的元数据操作，避免把整个文件向挂载
                        # 重复写第二遍（网盘挂载上这意味着整整多传一遍、耗时翻倍）。
                        _report_stage(stage_cb, "原子转正发布 (rename)")
                        if _rename_noreplace(parent_fd, tmp, parent_fd, name):
                            created = False  # 临时文件已转正为目标，不再存在
                        else:
                            # rename 本身没有 NOREPLACE 语义，预检与 rename 之间存在极小
                            # 的并发创建窗口；该窗口与覆盖流程最后一次校验和 os.replace
                            # 之间的竞争同类，无法彻底消除。换取的收益是网盘挂载上少传
                            # 一整遍文件，且排他写入 tmp 已保证绝大部分互斥语义。
                            _report_stage(stage_cb, "转正发布 (rename 预检)")
                            try:
                                os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                            except FileNotFoundError:
                                pass
                            else:
                                raise FileExistsError(
                                    errno.EEXIST, os.strerror(errno.EEXIST), dst)
                            os.rename(tmp, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
                            created = False  # 临时文件已转正为目标，不再存在
                    # 发布成功后 tmp 不再需要（硬链接场景）；rename 场景 created 已为 False
                    if created:
                        os.unlink(tmp, dir_fd=parent_fd)
                        created = False
                # direct 模式的目标文件就是最终产物：校验成功则保留，失败由 finally 清理
                if verify == "off":
                    try:
                        info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                    except OSError:
                        _discard_published(parent_fd, name, written_identity)
                        raise
                    if info.st_size != expected_size:
                        _discard_published(parent_fd, name, written_identity)
                        raise IOError("目标文件大小与写入内容不符，源文件已保留")
                    created = False
                    return expected_hash
                _report_stage(stage_cb, "读回校验目标内容")
                try:
                    size, digest = _fd_digest(os.open(name, os.O_RDONLY | os.O_NOFOLLOW,
                                                      dir_fd=parent_fd))
                except OSError:
                    _discard_published(parent_fd, name, written_identity)
                    raise
                if (size, digest) != (expected_size, expected_hash):
                    # 已发布目标不可信时删除再重传（源仍在）；否则残留目标会让下次重试变冲突。
                    _discard_published(parent_fd, name, written_identity)
                    raise IOError("目标文件读回校验失败，源文件已保留")
                created = False
                return expected_hash
            # overwrite 时先为旧目标建立同目录硬链接；无法原子固定旧目标时拒绝覆盖。
            # 复制备份无法消除最后一次校验与 os.replace 间的竞争，不能作为安全回退。
            # 在发布后读回失败时保留备份，不能让旧目标和源文件同时失去保障。
            try:
                old_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                old_stat = None
            if old_stat is not None:
                backup_name = f".fm-{uuid.uuid4().hex}.backup"
                try:
                    os.link(name, backup_name, src_dir_fd=parent_fd,
                            dst_dir_fd=parent_fd, follow_symlinks=False)
                except OSError as exc:
                    if exc.errno in (errno.EPERM, errno.EOPNOTSUPP, errno.ENOSYS, errno.EXDEV):
                        raise OSError("目标文件系统不支持安全硬链接备份；已拒绝覆盖，源文件和旧目标均保留") from exc
                    raise
                backup = backup_name
                current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                saved = os.stat(backup, dir_fd=parent_fd, follow_symlinks=False)
                identity = lambda stat: (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
                if identity(current) != identity(old_stat) or identity(saved) != identity(old_stat):
                    raise IOError("覆盖前目标文件已变化，旧目标备份已保留")
            os.replace(tmp, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            created = False
            if verify == "off":
                info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                if info.st_size != expected_size:
                    raise IOError("目标文件大小与写入内容不符，源文件已保留")
            else:
                size, digest = _fd_digest(os.open(name, os.O_RDONLY | os.O_NOFOLLOW,
                                                  dir_fd=parent_fd))
                if (size, digest) != (expected_size, expected_hash):
                    raise IOError("目标文件读回校验失败，源文件已保留")
            if backup is not None:
                os.unlink(backup, dir_fd=parent_fd)
                backup = None
            return expected_hash
        except OSError as exc:
            if backup is not None:
                raise IOError(f"{exc}；旧目标备份保留在 {os.path.join(os.path.dirname(dst), backup)}") from exc
            raise
        finally:
            if created:
                try:
                    os.unlink(pending, dir_fd=parent_fd)
                except OSError:
                    pass


def _write_nfo(dst: str, content: bytes, root: str = None, overwrite: bool = False,
               stage_cb=None, verify: str = "strict", direct: bool = False):
    _write_verified(dst, content=content, root=root, overwrite=overwrite,
                    stage_cb=stage_cb, verify=verify, direct=direct)


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
                            target_dir: str = None, target_device=None,
                            expected_content: tuple[int, str] = None,
                            expected_target: tuple[int, str] = None) -> Tuple[bool, str]:
    """暂存后再次核对身份和内容；不删除扫描/迁移期间被替换的源文件。"""
    staging = os.path.join(os.path.dirname(src), f".fm-source-{uuid.uuid4().hex}.moving")
    try:
        current_source = os.lstat(src)
        if expected_stat is None:
            expected_stat = current_source
        if not stat.S_ISREG(current_source.st_mode):
            return False, "源不是普通文件，已保留"
        if any(getattr(current_source, field) != getattr(expected_stat, field)
               for field in ("st_dev", "st_ino", "st_size", "st_mtime_ns")):
            return False, "删除前源身份或时间已变化，已保留"
        os.rename(src, staging)
    except OSError as exc:
        return False, f"源文件暂存失败: {exc}"
    try:
        current = os.lstat(staging)
        identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
        if any(getattr(current, field) != getattr(expected_stat, field) for field in identity):
            raise IOError("源文件身份或时间已变化")
        target = os.lstat(dst)
        if not stat.S_ISREG(target.st_mode):
            raise IOError("目标不是普通文件")
        if expected_content is None:
            if _matching_digest(staging, dst) is None:
                raise IOError("暂存源与目标内容不同")
        else:
            # NFO 可以在迁移时修正内容，源和目标分别与各自读回凭证校验。
            if (_size(staging), _sha256(staging)) != expected_content or (
                    target.st_size, _sha256(dst)) != (expected_target or expected_content):
                raise IOError("暂存源或目标与复制内容不同")
        target_after = os.lstat(dst)
        if any(getattr(target_after, field) != getattr(target, field) for field in identity):
            raise IOError("校验期间目标文件被替换")
        if target_dir is not None and (target_device is not None
                and _mount_identity(target_dir) != target_device):
            raise IOError("目标文件系统已切换")
        latest = os.lstat(staging)
        if any(getattr(latest, field) != getattr(current, field) for field in identity):
            raise IOError("校验期间源文件被改写")
        target_final = os.lstat(dst)
        if any(getattr(target_final, field) != getattr(target, field) for field in identity):
            raise IOError("删除前目标文件被替换")
        os.remove(staging)
        return True, ""
    except OSError as exc:
        # 恢复采用排他硬链接，绝不覆盖并发下载的新文件。
        try:
            os.link(staging, src, follow_symlinks=False)
        except OSError:
            return False, f"删除前校验失败；源备份保留在 {staging}: {exc}"
        # 与下载器并发替换源路径时，暂存可能是旧文件最后一份，不能自动清理。
        return False, f"删除前校验失败；源已恢复，暂存备份保留在 {staging}: {exc}"


def _unlink_verified_source(src: str, expected_stat, dst: str,
                            expected_size: int) -> Tuple[bool, str]:
    """fast/off 模式删除源：不暂存不重读，核对身份与目标大小后直接删除。

    源目录不会出现 .fm-source-*.moving 暂存文件；身份核对与 unlink 之间存在
    极小的并发替换窗口（strict 模式通过暂存+内容读回消除了该窗口）。
    """
    try:
        current = os.lstat(src)
    except FileNotFoundError:
        return False, "源文件缺失，可能已被移动或删除"
    except OSError as exc:
        return False, f"删除前源状态读取失败，已保留: {exc}"
    if not stat.S_ISREG(current.st_mode):
        return False, "源不是普通文件，已保留"
    identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
    if any(getattr(current, field) != getattr(expected_stat, field) for field in identity):
        return False, "删除前源身份或时间已变化，已保留"
    try:
        target = os.lstat(dst)
        if not stat.S_ISREG(target.st_mode):
            return False, "目标不是普通文件，源文件已保留"
        if target.st_size != expected_size:
            return False, "目标大小与校验值不符，源文件已保留"
        os.unlink(src)
    except FileNotFoundError:
        return False, "目标文件缺失，源文件已保留"
    except OSError as exc:
        return False, f"删除前校验失败，源文件已保留: {exc}"
    return True, ""


def _cleanup_stale_temps(directory: str):
    """清理目标目录内进程被杀残留的 .fm-*.moving 写入缓冲。

    仅删除超过 24 小时未更新的临时文件；.fm-*.backup 是覆盖失败时保留的
    旧目标，属于用户数据，不在清理范围。
    网盘挂载上 scandir 是一次列目录网络请求（高频列目录是网盘风控敏感操作），
    因此按目录节流：同一目录一个扫描间隔内只枚举一次。
    """
    now = time.time()
    with _stale_temp_lock:
        if now - _stale_temp_scan_at.get(directory, 0.0) < STALE_TEMP_SCAN_INTERVAL:
            return
        _stale_temp_scan_at[directory] = now
    try:
        with os.scandir(directory) as entries:
            entries = list(entries)
    except OSError:
        return
    for entry in entries:
        name = entry.name
        if not (name.startswith(".fm-") and name.endswith(".moving")):
            continue
        try:
            info = entry.stat(follow_symlinks=False)
        except OSError:
            continue
        if not stat.S_ISREG(info.st_mode) or now - info.st_mtime < STALE_TEMP_SECONDS:
            continue
        try:
            os.unlink(os.path.join(directory, name))
            logger.warning("已清理超过24小时的残留临时文件: %s", os.path.join(directory, name))
        except OSError:
            pass


def cleanup_stale_source_staging(path: str) -> bool:
    """清理源目录残留的 .fm-source-*.moving 暂存文件。

    该暂存只在目标已完整校验并发布之后才会产生，因此超过 24 小时仍未被
    正常删除的，几乎必然是传输中断（进程被杀）留下的冗余副本。
    """
    try:
        info = os.lstat(path)
    except OSError:
        return False
    if not stat.S_ISREG(info.st_mode) or time.time() - info.st_mtime < STALE_TEMP_SECONDS:
        return False
    try:
        os.unlink(path)
    except OSError:
        return False
    logger.warning("已清理超过24小时的源暂存文件（产生时目标已校验完成）: %s", path)
    return True


def _paths_overlap(first: str, second: str) -> bool:
    a, b = os.path.realpath(first), os.path.realpath(second)
    return os.path.commonpath((a, b)) in (a, b)


_TRANSIENT_IO_ERRNOS = {errno.EIO, errno.ESTALE, errno.ENOTCONN, errno.ETIMEDOUT, errno.EBUSY}


def _failed_io(item: dict, detail: str, exc: OSError) -> Tuple[str, str]:
    """将挂载瞬时错误保留为结构化状态，供调度器暂停任务而不消耗重试次数。"""
    cause = exc
    while cause is not None:
        if isinstance(cause, OSError) and cause.errno in _TRANSIENT_IO_ERRNOS:
            item["_transient_io"] = True
        if isinstance(cause, OSError) and cause.errno == errno.EIO:
            filename = os.fspath(cause.filename) if cause.filename is not None else ''
            if (getattr(cause, '_filemover_eio_origin', None) == 'source'
                    or filename == item.get('src_path')):
                item['_eio_origin'] = 'source'
            elif os.path.basename(filename).startswith('.fm-') and filename.endswith('.moving'):
                item['_eio_origin'] = 'target'
            elif detail in ('源路径状态读取失败', 'NFO 预处理失败'):
                item['_eio_origin'] = 'source'
            elif detail in ('迁移失败，源文件已保留', '路径校验失败',
                            'NFO 目标读回失败，未覆盖'):
                item.setdefault('_eio_origin', 'target')
        cause = cause.__cause__
    return "failed", f"{detail}: {exc}"


def transfer(item: dict, task: dict, stage_cb=None, progress_cb=None) -> Tuple[str, str]:
    """执行迁移，返回 (success/failed/conflict, detail)。

    stage_cb 用于向调度器上报当前阶段（如“复制并校验目标”），
    便于仪表盘展示大文件长时间停留在校验环节的进度。
    progress_cb(copied, total) 用于上报复制进度字节，供仪表盘计算速度与 ETA。
    task.verify_mode 决定读回强度：strict（默认，发布后读回+删除前暂存）、
    fast（单次读回，无暂存文件）、off（不读回，仅核对大小）；NFO 与普通文件
    遵循同一组设置（含 direct_publish）。
    """
    def _stage(name: str):
        if stage_cb is None:
            return
        try:
            stage_cb(name)
        except Exception:
            pass

    src, dst = item["src_path"], item["dst_path"]
    try:
        _mount_identity(task["dst_dir"])
        if _paths_overlap(task["src_dir"], task["dst_dir"]):
            return "failed", "源目录与目标目录重叠，已拒绝迁移"
        if not os.path.isabs(src) or not os.path.isabs(dst):
            return "failed", "源路径或目标路径不是绝对路径"
        source_location = os.path.realpath(os.path.dirname(src))
        if os.path.commonpath((source_location, os.path.realpath(task["src_dir"]))) != os.path.realpath(task["src_dir"]):
            return "failed", "源路径不在任务源目录中"
        if os.path.commonpath((os.path.realpath(os.path.dirname(dst)), os.path.realpath(task["dst_dir"]))) != os.path.realpath(task["dst_dir"]):
            return "failed", "目标路径不在任务目标目录中"
        if os.path.exists(dst) and os.path.exists(src) and os.path.samefile(src, dst):
            return "failed", "源与目标为同一文件，已拒绝迁移"
    except OSError as exc:
        return _failed_io(item, "路径校验失败", exc)
    except ValueError as exc:
        return "failed", f"路径校验失败: {exc}"

    cfg = config.get()
    policy = task.get("conflict_policy", "skip")
    verify = task.get("verify_mode") or "strict"
    if verify not in ("strict", "fast", "off"):
        verify = "strict"  # 未知取值按最稳妥的历史行为执行
    direct_publish = bool(task.get("direct_publish"))
    try:
        source_info = os.lstat(src)
    except FileNotFoundError:
        return "failed", "源文件不存在（可能已被移动或删除）"
    except OSError as exc:
        return _failed_io(item, "源路径状态读取失败", exc)
    is_nfo = src.lower().endswith(".nfo")
    if not stat.S_ISREG(source_info.st_mode):
        return "failed", "源不是普通文件，已保留"
    detail_extra = ""
    content = None
    if is_nfo:
        _stage("读取 NFO 内容")
        try:
            with open(src, "rb") as stream:
                raw = stream.read()
            content, applied = nfo_fix.prepare_nfo(raw, cfg.get("nfo_fix_enabled"))
            if applied:
                detail_extra = " +nfo:" + ",".join(applied)
        except OSError as exc:
            return _failed_io(item, "NFO 预处理失败", exc)

    try:
        _stage("检查目标文件")
        resolved = _resolve_conflict(dst, policy)
        # 目标已经是相同内容时不再覆盖或重命名；直接进入读回并删源流程。
        if resolved is not None and resolved[1] != "none":
            same = (_verified_nfo(dst, content) if is_nfo else
                    _matching_digest(src, dst) is not None)
            if same:
                resolved = None
        # 故障期间不应把“源可读、目标暂时不可读”当成可覆盖。
        if resolved is not None and resolved[1] == "overwrite":
            try:
                existing = os.lstat(dst)
            except FileNotFoundError:
                existing = None
            if existing is not None and stat.S_ISREG(existing.st_mode):
                if is_nfo:
                    _verified_nfo(dst, content)
                else:
                    _sha256(dst)
    except OSError as exc:
        return _failed_io(item, "源或目标读取失败，未写入", exc)
    if resolved is None:
        try:
            _stage("比对目标内容")
            target_matches = is_nfo and _verified_nfo(dst, content)
        except OSError as exc:
            return _failed_io(item, "NFO 目标读回失败，未覆盖", exc)
        if target_matches:
            target_hash = hashlib.sha256(content).hexdigest()
            source_hash = hashlib.sha256(raw).hexdigest()
            if task.get("after_action", "delete") == "delete":
                _stage("校验并删除源文件")
                try:
                    target_device = _mount_identity(task["dst_dir"])
                    if verify == "strict":
                        deleted, reason = _remove_verified_source(
                            src, dst, source_info, task["dst_dir"], target_device,
                            (len(raw), source_hash), (len(content), target_hash))
                    else:
                        # fast/off：目标内容刚经全量比对一致，直接删除源，不产生暂存文件。
                        deleted, reason = _unlink_verified_source(
                            src, source_info, dst, len(content))
                except OSError as exc:
                    return _failed_io(item, "NFO 源文件删除失败", exc)
                if not deleted:
                    return "failed", reason
                if cfg.get("remove_empty_dirs"):
                    _remove_empty_parent_dirs(os.path.dirname(src), stop_at=task["src_dir"])
            item["_verified_target"] = (len(content), target_hash,
                                         task.get("after_action", "delete") == "delete",
                                         len(raw), source_hash)
            return "success", "ok (NFO 目标内容一致)" + detail_extra
        prior = db.get_migrated_dst(task["id"], item["rel_path"]) if is_nfo else None
        if is_nfo and prior == dst:
            if not db.migrated_destination_matches(task["id"], item["rel_path"], dst):
                return "conflict", f"NFO 目标可能被外部改写: {dst}；需人工确认，不自动覆盖"
            resolved = (dst, "overwrite")
        elif not is_nfo:
            try:
                _stage("比对目标与源内容")
                matching_hash = _matching_digest(src, dst)
            except OSError as exc:
                return _failed_io(item, "目标或源文件读回失败，未删除或覆盖", exc)
            if matching_hash is None:
                return "conflict", f"目标已存在: {dst}（策略 skip）"
            if task.get("after_action", "delete") == "delete":
                _stage("校验并删除源文件")
                try:
                    target_device = _mount_identity(task["dst_dir"])
                    source_stat = os.lstat(src)
                    if verify == "strict":
                        deleted, reason = _remove_verified_source(
                            src, dst, source_stat, task["dst_dir"], target_device,
                            (source_stat.st_size, matching_hash))
                    else:
                        # fast/off：目标内容刚经过全量比对一致，直接删除源，不产生暂存文件。
                        deleted, reason = _unlink_verified_source(
                            src, source_stat, dst, source_stat.st_size)
                    if not deleted:
                        return "failed", reason
                except OSError as exc:
                    return _failed_io(item, "源文件删除失败", exc)
                if cfg.get("remove_empty_dirs"):
                    _remove_empty_parent_dirs(os.path.dirname(src), stop_at=task["src_dir"])
            item["_verified_target"] = (_size(dst), matching_hash, True,
                                         _size(dst), matching_hash)
            return "success", "ok (目标内容一致)"
        else:
            if is_nfo:
                return "conflict", f"NFO 目标已存在且内容不同: {dst}；需人工确认，不自动覆盖（策略 skip）"
            return "conflict", f"目标已存在: {dst}（策略 skip）"
    final_dst, how = resolved
    _cleanup_stale_temps(os.path.dirname(final_dst))
    try:
        if is_nfo:
            _stage("复制并校验目标")
            target_device = _mount_identity(task["dst_dir"])
            wrote_target = not _verified_nfo(final_dst, content)
            if wrote_target:
                _write_nfo(final_dst, content, root=task["dst_dir"], overwrite=how == "overwrite",
                           stage_cb=_stage, verify=verify, direct=direct_publish)
            # strict 已在写入路径读回一次，此处是并发替换的最后防线；fast/off
            # 的读回强度由 _write_verified 按模式执行，不再叠加第二次读回。
            if verify == "strict" and not _verified_nfo(final_dst, content):
                raise IOError("NFO 目标文件读回校验失败，源文件已保留")
            item["dst_path"] = final_dst
            target_hash = hashlib.sha256(content).hexdigest()
            source_hash = hashlib.sha256(raw).hexdigest()
            if task.get("after_action", "delete") == "delete":
                _stage("校验并删除源文件")
                if verify == "strict":
                    deleted, reason = _remove_verified_source(
                        src, final_dst, source_info, task["dst_dir"], target_device,
                        (len(raw), source_hash), (len(content), target_hash))
                else:
                    # fast/off：写入路径已按模式读回/核对大小，删除源不再暂存重读。
                    deleted, reason = _unlink_verified_source(
                        src, source_info, final_dst, len(content))
                if not deleted:
                    return "failed", reason
                if cfg.get("remove_empty_dirs"):
                    _remove_empty_parent_dirs(os.path.dirname(src), stop_at=task["src_dir"])
            item["_verified_target"] = (len(content), target_hash,
                                         task.get("after_action", "delete") == "delete",
                                         len(raw), source_hash)
            return "success", f"ok (NFO 已读回校验){detail_extra}"

        # 普通文件也使用先复制、完整性校验、原子替换的流程。
        # 挂载可能在 close 后丢写，读回校验失败时绝不能删除源文件。
        _stage("复制并校验目标")
        target_device = _mount_identity(task["dst_dir"])
        source_stat = os.lstat(src)
        source_hash = _write_verified(final_dst, source=src, root=task["dst_dir"],
                                      overwrite=how == "overwrite", verify=verify,
                                      progress_cb=progress_cb, stage_cb=_stage,
                                      direct=direct_publish)
        item["dst_path"] = final_dst
        current_stat = os.lstat(src)
        identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
        if (not stat.S_ISREG(current_stat.st_mode)
                or any(getattr(current_stat, field) != getattr(source_stat, field)
                       for field in identity)):
            return "failed", "复制期间源文件变化，源文件已保留"
        # 保留源文件时，严格模式仍须完整读回，防止同大小、同时间戳的原位改写被忽略；
        # fast/off 模式仅依赖复制前后的一致性核对。
        if task.get("after_action", "delete") != "delete" and verify == "strict":
            _stage("复核源文件内容")
            if _sha256(src) != source_hash:
                return "failed", "复制期间源文件变化，源文件已保留"
        detail = f"ok ({how})" if how != "none" else "ok"
        if task.get("after_action", "delete") == "delete":
            try:
                if _mount_identity(task["dst_dir"]) != target_device:
                    return "failed", "目标文件系统已切换，源文件已保留"
                _stage("校验并删除源文件")
                if verify == "strict":
                    deleted, reason = _remove_verified_source(
                        src, final_dst, source_stat, task["dst_dir"], target_device,
                        (source_stat.st_size, source_hash))
                else:
                    deleted, reason = _unlink_verified_source(
                        src, source_stat, final_dst, source_stat.st_size)
                if not deleted:
                    return "failed", reason
            except OSError as exc:
                return _failed_io(item, "源文件删除失败", exc)
            if cfg.get("remove_empty_dirs"):
                _remove_empty_parent_dirs(os.path.dirname(src), stop_at=task["src_dir"])
        item["_verified_target"] = (source_stat.st_size, source_hash, True,
                                     source_stat.st_size, source_hash)
        return "success", detail
    except OSError as exc:
        return _failed_io(item, "迁移失败，源文件已保留", exc)
    except Exception as exc:
        return "failed", f"迁移失败，源文件已保留: {exc}"


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
