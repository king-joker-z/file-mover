"""后台线程：扫描循环 + 迁移主循环（节流逐个迁移）"""
import errno
import hashlib
import os
import stat
import threading
import time
import traceback
from . import db, config, scanner, transfer


class Scheduler:
    FULL_SCAN_INTERVAL = 24 * 60 * 60  # 每日补齐历史目标视频的 NFO

    def __init__(self):
        self._stop = threading.Event()
        self._threads = []
        self._scan_wake = threading.Event()
        self._work_wake = threading.Event()
        self._sync_requests = set()
        self._scan_lock = threading.Lock()
        self._requests_lock = threading.Lock()
        self.last_scan_ts = {1: time.time()}  # 防启动即触发; 各任务扫描时自动覆盖
        self._last_full_scan = {}
        self._scan_retry_at = {}  # task_id -> 扫描失败后允许再次尝试的墙上时间
        self.sync_progress = {}  # task_id -> 扫描进度
        self.current_file = None
        self.moved_today = 0
        self._today = time.strftime("%Y-%m-%d")
        self.started_at = time.time()
        self.last_scan_error = None
        self.last_scan_error_at = None
        self._last_scan_error_task_id = None
        self._last_scan_error_path = None
        self.last_scan_success_at = None
        self.last_worker_error = None
        self._next_eligible = {}  # task_id -> monotonic deadline
        self.last_accounting_error = None
        self._accounting_blocked = set()  # 安全校验失败不在每次轮询重读网盘
        self._accounting_retry_at = {}  # 数据库暂不可用时限速重试

    # ---------- 生命周期 ----------

    def start(self):
        db.reset_stuck_transferring()
        self._stop.clear()
        self.last_scan_success_at = None
        self._last_full_scan.clear()
        self._scan_retry_at.clear()
        now = time.time()
        for task in db.list_tasks():
            self.last_scan_ts[task["id"]] = now
        for name, target in (("scanner", self._scan_loop),
                             ("worker", self._work_loop)):
            t = threading.Thread(target=target, name=f"fm-{name}", daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self):
        self._stop.set()
        self._scan_wake.set()
        self._work_wake.set()
        for thread in self._threads:
            thread.join()  # 正在传输的文件完成验证后才允许服务退出
        self._threads.clear()

    def wake_scan(self):
        self._scan_wake.set()
        self._work_wake.set()

    def sync_now(self, task_id: int):
        """请求立即全量扫描：由唯一的 _scan_loop 线程优先执行（无并发扫描）。
        进度通过 self.sync_progress /api/sync-progress 暴露给 UI 轮询。"""
        with self._requests_lock:
            if self._stop.is_set() or (self._threads and not any(
                    t.name == "fm-scanner" and t.is_alive() for t in self._threads)):
                return {"ok": False, "message": "扫描线程已退出，请重启服务"}
            prog = self.sync_progress.get(task_id)
            if prog and prog.get("running"):
                return {"ok": True, "message": "扫描进行中"}
            self.sync_progress[task_id] = {"running": True, "scanned": 0, "enqueued": 0,
                                           "current": "", "done": False,
                                           "error": None}
            self._sync_requests.add(task_id)
        self._scan_wake.set()
        return {"ok": True, "message": "全量扫描已触发"}

    def _process_sync_requests(self):
        """在 _scan_loop 线程内执行：处理待处理的立即同步请求"""
        while True:
            with self._requests_lock:
                if not self._sync_requests:
                    return
                task_id = self._sync_requests.pop()
            prog = self.sync_progress.get(task_id)
            if not prog:
                continue
            try:
                if time.time() < self._scan_retry_at.get(task_id, 0):
                    prog["error"] = "目录故障后冷却中，请至少等待 60 秒再同步"
                    continue
                task = db.get_task(task_id)
                if task and task.get("enabled"):
                    if not self._task_paths_safe(task):
                        raise ValueError("任务目录重叠或与其他任务冲突，已拒绝扫描")
                    prog["current"] = ""
                    with self._scan_lock:
                        scanner.scan_task(task, skip_stable_check=True,
                                          progress=prog,
                                          on_dir=lambda root, n: prog.__setitem__(
                                              "scanned", prog.get("scanned", 0) + n),
                                          force_full=True)
                        self._last_full_scan[task_id] = time.time()
                        self.last_scan_success_at = time.time()
                        self._scan_succeeded(task_id)
                        self._scan_retry_at.pop(task_id, None)
                else:
                    prog["error"] = "任务不存在或已停用"
            except Exception as e:
                self._scan_failed(e, task_id)
                self._scan_retry_at[task_id] = time.time() + 60
                prog["error"] = str(e)
            finally:
                prog["done"] = True
                prog["running"] = False

    # ---------- 统计 ----------

    def stats(self) -> dict:
        today = time.strftime("%Y-%m-%d")
        if today != self._today:
            self._today = today
            self.moved_today = 0
        try:
            cfg = config.get()
            q = db.queue_stats()
            stats_error = None
        except Exception as exc:
            cfg, q = None, {}
            stats_error = str(exc)
        return {
            "moved_today": self.moved_today,
            "pending": q.get("pending", 0),
            "transferring": q.get("transferring", 0),
            "failed": q.get("failed", 0),
            "conflict": q.get("conflict", 0),
            "done": q.get("done", 0),
            "current_file": self.current_file,
            "uptime_seconds": int(time.time() - self.started_at),
            "settings": cfg,
            "scanner_alive": any(t.name == "fm-scanner" and t.is_alive() for t in self._threads),
            "worker_alive": any(t.name == "fm-worker" and t.is_alive() for t in self._threads),
            "last_scan_error": self.last_scan_error,
            "last_scan_error_at": self.last_scan_error_at,
            "last_scan_error_task_id": self._last_scan_error_task_id,
            "last_scan_error_path": self._last_scan_error_path,
            "last_scan_success_at": self.last_scan_success_at,
            "last_worker_error": self.last_worker_error,
            "accounting_pending": q.get("accounting_pending", 0),
            "last_accounting_error": self.last_accounting_error,
            "stats_error": stats_error,
        }

    # ---------- 扫描循环 ----------

    def _scan_loop(self):
        try:
            while not self._stop.is_set():
                timeout = 2
                try:
                    # 即使配置或任务表暂时不可用，也先完成已请求的立即扫描。
                    self._process_sync_requests()
                    cfg = config.get()
                    now = time.time()
                    for task in db.list_tasks():
                        if self._sync_requests:
                            break
                        if not task.get("enabled") or not self._task_paths_safe(task):
                            continue
                        interval = task.get("scan_interval") or cfg["scan_interval"]
                        if (now - self.last_scan_ts.get(task["id"], 0) < interval
                                or now < self._scan_retry_at.get(task["id"], 0)):
                            continue
                        if self._sync_requests or not self._scan_lock.acquire(timeout=0.1):
                            break
                        try:
                            full = (time.time() - self._last_full_scan.get(task["id"], 0)
                                    >= self.FULL_SCAN_INTERVAL)
                            scanner.scan_task(task, force_full=full)
                            if full:
                                self._last_full_scan[task["id"]] = time.time()
                            self.last_scan_success_at = time.time()
                            self._scan_succeeded(task["id"])
                        except Exception as exc:
                            self._scan_failed(exc, task["id"])
                            # 不依赖用户设置的扫描间隔，持续 EIO 最多每分钟重试一次。
                            self._scan_retry_at[task["id"]] = time.time() + 60
                        else:
                            self._scan_retry_at.pop(task["id"], None)
                        finally:
                            self.last_scan_ts[task["id"]] = time.time()
                            self._scan_lock.release()
                    self._process_sync_requests()
                    timeout = min(5, max(1, cfg["scan_interval"] / 6))
                except Exception as exc:
                    self._scan_failed(exc)
                self._scan_wake.wait(timeout=timeout)
                self._scan_wake.clear()
        finally:
            with self._requests_lock:
                for task_id, prog in self.sync_progress.items():
                    if prog.get("running"):
                        prog.update(running=False, done=True, error="扫描线程已退出，请重启服务")
                self._sync_requests.clear()

    def _scan_failed(self, exc, task_id=None):
        traceback.print_exception(type(exc), exc, exc.__traceback__)
        self.last_scan_error = (f"任务 {task_id}: {exc}" if task_id is not None else str(exc))
        self.last_scan_error_at = time.time()
        self._last_scan_error_task_id = task_id
        self._last_scan_error_path = exc.filename if isinstance(exc, OSError) else None

    def _scan_succeeded(self, task_id):
        # 仅同一任务的成功扫描可解除它的旧告警，其他任务的成功不能掩盖故障。
        if self._last_scan_error_task_id == task_id:
            self.last_scan_error = None
            self.last_scan_error_at = None
            self._last_scan_error_task_id = None
            self._last_scan_error_path = None

    def _task_paths_safe(self, task):
        from .main import validate_task_paths
        try:
            validate_task_paths(task, db.list_tasks(), task["id"])
            return True
        except (ValueError, RuntimeError, OSError):
            return False

    # ---------- 运行窗口 ----------

    @staticmethod
    def _in_run_window(task: dict, now=None) -> bool:
        """run_windows 为空 = 全天运行；否则 "HH:MM-HH:MM" 多段逗号分隔，窗口内才运行"""
        raw = (task.get("run_windows") or "").strip()
        if not raw:
            return True
        t = now or time.localtime()
        cur = t.tm_hour * 60 + t.tm_min
        for seg in raw.split(","):
            seg = seg.strip()
            if not seg:
                continue
            try:
                a, b = seg.split("-")
                h1, m1 = map(int, a.strip().split(":"))
                h2, m2 = map(int, b.strip().split(":"))
            except ValueError:
                return False  # 非法窗口绝不按全天执行
            if not (0 <= h1 <= 23 and 0 <= m1 <= 59 and 0 <= m2 <= 59
                    and (0 <= h2 <= 23 or (h2 == 24 and m2 == 0))):
                return False
            start, end = h1 * 60 + m1, h2 * 60 + m2
            if start <= end:
                if start <= cur < end:
                    return True
            else:  # 跨午夜，如 23:00-06:00
                if cur >= start or cur < end:
                    return True
        return False

    # ---------- 迁移主循环 ----------

    def _work_loop(self):
        while not self._stop.is_set():
            try:
                self._work_once()
            except Exception as exc:
                traceback.print_exc()
                self.last_worker_error = str(exc)
                self.current_file = None
                self._stop.wait(timeout=2)

    def _log(self, item, duration_ms, result, detail):
        try:
            db.add_log(item["task_id"], item["id"], item["src_path"], item["dst_path"],
                       item.get("size", 0), duration_ms, result, detail)
        except Exception as exc:
            self.last_worker_error = f"日志写入失败 ({item['id']}): {exc}; {detail}"
            print(self.last_worker_error, flush=True)

    @staticmethod
    def _verify_accounting(item, task, rehash=True):
        """凭传输阶段的读回凭证复核目标，绝不根据存在性或旧迁移记录授权。"""
        src, dst = item['src_path'], item['dst_path']
        src_root, dst_root = os.path.realpath(task['src_dir']), os.path.realpath(task['dst_dir'])
        if (not os.path.isabs(src) or not os.path.isabs(dst)
                or os.path.commonpath((src_root, os.path.realpath(os.path.dirname(src)))) != src_root
                or os.path.commonpath((dst_root, os.path.realpath(os.path.dirname(dst)))) != dst_root
                or src == src_root or dst == dst_root
                or transfer._paths_overlap(src_root, dst_root)):
            raise ValueError('路径越界或目录重叠，需人工核查')
        transfer._mount_identity(dst_root)
        # 不创建目录，不跟随目标路径中任何软链；用 fd 固定读回对象。
        parts = os.path.relpath(os.path.abspath(dst), os.path.abspath(task['dst_dir'])).split(os.sep)
        if any(part in ('', '.', '..') for part in parts):
            raise ValueError('目标路径含非规范段，需人工核查')
        fd = os.open(dst_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in parts[:-1]:
                next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                  dir_fd=fd)
                os.close(fd)
                fd = next_fd
            target_fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW, dir_fd=fd)
            try:
                before = os.fstat(target_fd)
                if not stat.S_ISREG(before.st_mode) or before.st_size != item['verified_size']:
                    raise OSError('目标大小或类型已变化，需人工核查')
                digest = hashlib.sha256() if rehash else None
                if rehash:
                    with os.fdopen(os.dup(target_fd), 'rb') as stream:
                        for chunk in iter(lambda: stream.read(1 << 20), b''):
                            digest.update(chunk)
                after = os.fstat(target_fd)
                named = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
                identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns)
                if ((rehash and digest.hexdigest() != item['verified_sha256'])
                        or identity(before) != identity(after)
                        or identity(after) != identity(named)):
                    raise OSError('目标完整读回与已校验哈希不一致，需人工核查')
            finally:
                os.close(target_fd)
        finally:
            os.close(fd)
        try:
            source_stat = os.lstat(src)
        except FileNotFoundError:
            if not item['verified_trusted']:
                raise OSError('未授权目标且源文件缺失，需人工核查')
        else:
            expected = ((item['source_size'], item['source_sha256'])
                        if src.lower().endswith('.nfo') else
                        (item['verified_size'], item['verified_sha256']))
            if (not stat.S_ISREG(source_stat.st_mode)
                    or os.path.commonpath((src_root, os.path.realpath(src))) != src_root
                    or expected[1] is None
                    or (source_stat.st_size, transfer._sha256(src)) != expected):
                raise OSError('源文件身份不匹配，需人工核查')

    def _retry_accounting(self):
        for item in db.list_accounting_pending():
            if (item['id'] in self._accounting_blocked
                    or time.monotonic() < self._accounting_retry_at.get(item['id'], 0)):
                continue
            try:
                task = db.get_task(item['task_id'])
                if task is None:
                    db.cleanup_orphan_queue(item['id'])
                    self._accounting_blocked.discard(item['id'])
                    self._accounting_retry_at.pop(item['id'], None)
                    continue
                if not self._task_paths_safe(task):
                    raise ValueError('任务路径冲突，需人工核查')
                self._verify_accounting(item, task)
            except (OSError, ValueError) as exc:
                if isinstance(exc, OSError) and exc.errno in (
                        errno.EIO, errno.ESTALE, errno.ENOTCONN, errno.ETIMEDOUT,
                        errno.EINTR, errno.EBUSY):
                    self._accounting_retry_at[item['id']] = time.monotonic() + 60
                    self.last_accounting_error = f"记账目标读取失败 ({item['id']}): {exc}；60s 后重试"
                else:
                    self._accounting_blocked.add(item['id'])
                    self.last_accounting_error = f"记账待人工核查 ({item['id']}): {exc}"
                print(self.last_accounting_error, flush=True)
                continue
            try:
                db.complete_accounting(item)
            except Exception as exc:
                self._accounting_retry_at[item['id']] = time.monotonic() + 30
                self.last_accounting_error = f"记账待补偿 ({item['id']}): {exc}；30s 后重试"
                print(self.last_accounting_error, flush=True)
                continue
            self._accounting_retry_at.pop(item['id'], None)
            self.last_accounting_error = None
            self._log(item, 0, 'success', '目标哈希复核通过，记账补偿完成')

    def _work_once(self):
        self._retry_accounting()
        task_map = {t["id"]: t for t in db.list_tasks()}
        now = time.monotonic()
        # 扫描已发现该任务目录故障时，不仅延迟下次扫描，也暂停其迁移写入。
        # 即使冷却时间到了，也须先完成一次成功扫描才能解除暂停。
        eligible = [tid for tid, task in task_map.items() if task.get("enabled")
                    and tid not in self._scan_retry_at
                    and now >= self._next_eligible.get(tid, 0)
                    and self._in_run_window(task) and self._task_paths_safe(task)]
        item = db.take_next_pending(eligible)
        if item is None:
            self.current_file = None
            deadlines = [deadline for tid, deadline in self._next_eligible.items()
                         if tid in task_map and task_map[tid].get("enabled") and deadline > now]
            self._work_wake.wait(timeout=min(1, max(0, min(deadlines) - now)) if deadlines else 1)
            self._work_wake.clear()
            return
        task = task_map.get(item["task_id"])
        if task is None or db.get_task(item["task_id"]) is None:
            db.cleanup_orphan_queue(item["id"])
            return
        if not task.get("enabled") or not self._in_run_window(task):
            db.set_status(item["id"], "pending")
            return

        self.current_file = item["rel_path"]
        started = time.monotonic()
        try:
            # 无持久化凭证的 transferring 重启后回退；已删源不可重传或盲目补记。
            transient_source_io = False
            try:
                source_stat = os.lstat(item["src_path"])
            except FileNotFoundError:
                result, detail = "conflict", "源文件缺失，可能迁移成功但记账丢失；需人工核查目标与记录"
            except OSError as exc:
                transient_source_io = exc.errno in (
                    errno.EIO, errno.ESTALE, errno.ENOTCONN, errno.ETIMEDOUT, errno.EBUSY)
                result, detail = "failed", f"源路径读取失败，已保留待重试: {exc}"
            else:
                if not stat.S_ISREG(source_stat.st_mode):
                    result, detail = "conflict", "源不是普通文件，可能是历史残留；需人工核查目标与记录"
                else:
                    try:
                        result, detail = transfer.transfer(item, task)
                    except Exception as exc:
                        traceback.print_exc()
                        result, detail = "failed", f"迁移异常: {exc}"
            duration_ms = int((time.monotonic() - started) * 1000)
            if result == "success":
                self.moved_today += 1
                try:
                    db.persist_accounting_pending(item['id'], item, item.get('_verified_target'))
                except Exception as exc:
                    self.last_accounting_error = f"凭证落盘失败 ({item['id']}): {exc}；保留 transferring 供人工核查"
                    print(self.last_accounting_error, flush=True)
                    detail += f"；{self.last_accounting_error}"
                else:
                    try:
                        # 正常路径已有传输阶段全量读回，无需再读网盘。
                        pending = db.get_accounting_pending(item['id'])
                        self._verify_accounting(pending, task, rehash=False)
                        db.complete_accounting(pending)
                    except Exception as exc:
                        self.last_accounting_error = f"记账待补偿 ({item['id']}): {exc}"
                        print(self.last_accounting_error, flush=True)
                        detail += f"；{self.last_accounting_error}"
                    else:
                        self.last_accounting_error = None
            elif result == "conflict":
                db.set_status(item["id"], "conflict")
            elif result == "failed":
                if transient_source_io:
                    # 源目录 EIO 不消耗迁移重试额度，需成功扫描后才恢复同任务写入。
                    self._scan_retry_at[task["id"]] = time.time() + 60
                    db.set_status(item["id"], "failed", next_retry_at=time.time() + 60)
                    detail += "；源目录故障，60s 后限速探测恢复"
                    self._log(item, duration_ms, "failed", detail)
                    return
                retries = item.get("retries", 0) + 1
                cfg = config.get()
                detail = f"[第{retries}次尝试] {detail}"
                if retries <= cfg["max_retries"]:
                    backoff = cfg["retry_backoff_seconds"]
                    delay = backoff[min(retries - 1, len(backoff) - 1)]
                    db.set_status(item["id"], "failed", retries=retries,
                                  next_retry_at=time.time() + delay)
                    detail += f"，{delay}s 后自动重试"
                else:
                    db.set_status(item["id"], "failed", retries=retries)
                    detail += f"，已达最大重试次数({cfg['max_retries']})，等待手动重试"
            # DB 将 success 日志索引成迁移凭证；未记账时不能授予该凭证。
            log_result = "accounting_pending" if result == "success" and self.last_accounting_error else result
            self._log(item, duration_ms, log_result, detail)
            db.cleanup_orphan_queue(item["id"])
        finally:
            self.current_file = None
            interval = max(0.0, float(task.get("interval_seconds", 5)))
            self._next_eligible[item["task_id"]] = time.monotonic() + interval


scheduler = Scheduler()
