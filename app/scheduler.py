"""后台线程：扫描循环 + 迁移主循环（节流逐个迁移）"""
import errno
import hashlib
import logging
import os
import stat
import threading
import time
from uuid import uuid4
from . import db, config, scanner, transfer

logger = logging.getLogger("uvicorn.error")


class Scheduler:
    HEARTBEAT_INTERVAL = 15  # 慢挂载无返回时持续报告所在阶段

    def __init__(self):
        self._stop = threading.Event()
        self._threads = []
        self._scan_wake = threading.Event()
        self._work_wake = threading.Event()
        self._sync_requests = set()
        self._scan_lock = threading.Lock()
        # 慢挂载上避免枚举目录与文件迁移同时进行；扫描请求在当前文件结束后优先执行。
        self._io_lock = threading.Lock()
        self._scan_io_waiting = threading.Event()
        self._requests_lock = threading.Lock()
        self.last_scan_ts = {1: time.time()}  # 防启动即触发; 各任务扫描时自动覆盖
        self._scan_retry_at = {}  # task_id -> 扫描失败后允许再次尝试的墙上时间
        self._path_warning_at = {}  # 对每秒调用的路径校验故障限速，防止刷满容器日志
        self._last_scan_phase = None  # 扫描线程被其他前置步骤阻塞时的心跳阶段
        self._last_work_phase = None  # 迁移线程在取队列/记账时的心跳阶段
        self._active_scan_task_id = None
        self._active_scan_state = None
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
        self._unstarted_claims = set()  # DB 故障期间待释放的迁移前认领

    # ---------- 生命周期 ----------

    def start(self):
        logger.info("服务启动：初始化队列与后台线程")
        db.reset_stuck_transferring()
        self._stop.clear()
        self.last_scan_success_at = None
        self._scan_retry_at.clear()
        self._scan_retry_at.update(db.list_scan_failures())
        # 重启时旧队列必须先经过一次成功扫描，不能让 worker 抢在 scanner 前迁移。
        for task_id in db.tasks_requiring_startup_scan():
            self._scan_retry_at.setdefault(task_id, 0)
        self._path_warning_at.clear()
        self._last_scan_phase = None
        self._last_work_phase = None
        self._active_scan_task_id = None
        self._active_scan_state = None
        self._scan_io_waiting.clear()
        now = time.time()
        tasks = db.list_tasks()
        for task in tasks:
            self.last_scan_ts[task["id"]] = now
        for name, target in (("scanner", self._scan_loop),
                             ("worker", self._work_loop)):
            t = threading.Thread(target=target, name=f"fm-{name}", daemon=True)
            t.start()
            self._threads.append(t)
        logger.info("后台线程已启动：任务数=%d", len(tasks))

    def stop(self):
        logger.info("服务停止：等待后台任务退出")
        self._stop.set()
        self._scan_wake.set()
        self._work_wake.set()
        for thread in self._threads:
            thread.join()  # 正在传输的文件完成验证后才允许服务退出
        self._threads.clear()
        logger.info("后台线程已退出")

    def wake_scan(self):
        self._scan_wake.set()
        self._work_wake.set()

    def sync_now(self, task_id: int, request_id: str = None, *, backfill_nfo: bool = False):
        """请求立即扫描源目录；历史目标 NFO 巡检仅由显式操作触发。"""
        with self._requests_lock:
            if self._stop.is_set() or (self._threads and not any(
                    t.name == "fm-scanner" and t.is_alive() for t in self._threads)):
                logger.warning("立即同步拒绝：task_id=%s 扫描线程未运行", task_id)
                return {"ok": False, "message": "扫描线程已退出，请重启服务"}
            prog = self.sync_progress.get(task_id)
            if prog and prog.get("running"):
                logger.info("立即同步已在执行：task_id=%s 阶段=%s", task_id, prog.get("current"))
                return {"ok": True, "message": "扫描进行中", "request_id": prog["request_id"]}
            request_id = request_id or str(uuid4())
            self.sync_progress[task_id] = {"running": True, "scanned": 0, "enqueued": 0,
                                           "current": "等待扫描线程", "done": False,
                                           "error": None, "request_id": request_id,
                                           "backfill_nfo": backfill_nfo}
            self._sync_requests.add(task_id)
        logger.info("立即同步已排队：task_id=%s", task_id)
        self._scan_wake.set()
        message = "历史 NFO 补齐已触发" if backfill_nfo else "源目录扫描已触发"
        return {"ok": True, "message": message, "request_id": request_id}

    def _run_scan(self, task, *, manual=False, force_full=False, progress=None):
        """统一记录扫描生命周期；慢挂载的阻塞操作每 15 秒报告一次当前阶段。"""
        task_id = task["id"]
        state = progress if progress is not None else {"current": "扫描准备中", "scanned": 0}
        finished = threading.Event()
        started = time.monotonic()
        mode = "手动" if manual else "定时"

        def heartbeat():
            while not finished.wait(self.HEARTBEAT_INTERVAL):
                logger.warning("扫描仍在进行：task_id=%s 模式=%s 阶段=%s 已发现=%s 新入队=%s 耗时=%.1fs",
                               task_id, mode, state.get("current"), state.get("scanned", 0),
                               state.get("enqueued", 0), time.monotonic() - started)

        reporter = threading.Thread(target=heartbeat, name=f"fm-scan-report-{task_id}", daemon=True)
        logger.info("扫描开始：task_id=%s 模式=%s 源=%s 目标=%s 全量=%s",
                    task_id, mode, task["src_dir"], task["dst_dir"], force_full)
        self._active_scan_task_id = task_id
        self._active_scan_state = state
        reporter.start()
        try:
            self._scan_io_waiting.set()
            state["current"] = "等待当前文件迁移完成"
            with self._io_lock:
                try:
                    state["current"] = "检查任务目录"
                    if not self._task_paths_safe(task):
                        raise ValueError("任务目录重叠或与其他任务冲突，已拒绝扫描")
                    state["current"] = "扫描准备中"
                    result = scanner.scan_task(
                        task, skip_stable_check=manual, progress=state,
                        on_dir=lambda root, n: state.__setitem__("scanned", state.get("scanned", 0) + n),
                        force_full=force_full)
                except Exception:
                    # 故障熔断必须在释放 I/O 锁前生效，否则迁移线程可能抢到下一文件。
                    self._pause_task_for_scan(task_id)
                    raise
            enqueued = result.get("enqueued", 0) if isinstance(result, dict) else state.get("enqueued", 0)
            logger.info("扫描完成：task_id=%s 模式=%s 已发现=%s 新入队=%s 耗时=%.1fs",
                        task_id, mode, state.get("scanned", 0), enqueued,
                        time.monotonic() - started)
            return result
        except Exception:
            logger.error("扫描中断：task_id=%s 模式=%s 阶段=%s 已发现=%s 新入队=%s 耗时=%.1fs",
                         task_id, mode, state.get("current"), state.get("scanned", 0),
                         state.get("enqueued", 0), time.monotonic() - started)
            raise
        finally:
            self._scan_io_waiting.clear()
            finished.set()
            self._active_scan_task_id = None
            self._active_scan_state = None

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
            self._last_scan_phase = (task_id, "立即同步准备中", time.monotonic())
            try:
                if time.time() < self._scan_retry_at.get(task_id, 0):
                    prog["error"] = "目录故障后冷却中，请至少等待 60 秒再同步"
                    logger.warning("立即同步冷却中：task_id=%s 剩余=%.1fs", task_id,
                                   self._scan_retry_at[task_id] - time.time())
                    continue
                prog["current"] = "读取任务配置"
                self._last_scan_phase = (task_id, "读取任务配置", time.monotonic())
                task = db.get_task(task_id)
                if task and task.get("enabled"):
                    with self._scan_lock:
                        self._last_scan_phase = None  # 扫描自身的心跳会报告更准确的目录阶段
                        self._run_scan(task, manual=True, force_full=prog["backfill_nfo"],
                                       progress=prog)
                        self.last_scan_success_at = time.time()
                        self._scan_succeeded(task_id)
                else:
                    prog["error"] = "任务不存在或已停用"
                    logger.warning("立即同步跳过：task_id=%s 任务不存在或已停用", task_id)
            except Exception as e:
                logger.error("立即同步失败：task_id=%s 阶段=%s", task_id, prog.get("current"))
                self._scan_failed(e, task_id)
                self._pause_task_for_scan(task_id)
                prog["error"] = str(e)
            finally:
                self.last_scan_ts[task_id] = time.time()
                prog["done"] = True
                prog["running"] = False
                prog["current"] = ""

    # ---------- 统计 ----------

    def stats(self) -> dict:
        today = time.strftime("%Y-%m-%d")
        if today != self._today:
            self._today = today
            self.moved_today = 0
        errors = []
        try:
            cfg = config.get()
        except Exception as exc:
            cfg = None
            errors.append(f"配置读取失败: {exc}")
        try:
            q = db.queue_stats()
        except Exception as exc:
            q = None
            errors.append(f"队列统计失败: {exc}")
        try:
            tasks = db.list_tasks()
        except Exception as exc:
            tasks = None
            errors.append(f"任务列表失败: {exc}")
        stats_error = "；".join(errors) if errors else None
        now = time.time()
        schedules = []
        for task in tasks or []:
            task_id = task["id"]
            interval = task.get("scan_interval") or (cfg or config.DEFAULTS)["scan_interval"]
            last = self.last_scan_ts.get(task_id)
            retry_at = self._scan_retry_at.get(task_id, 0)
            progress = self.sync_progress.get(task_id, {})
            running = bool(progress.get("running"))
            if not task.get("enabled"):
                status = "disabled"
            elif running:
                status = "manual"
            elif self._active_scan_task_id == task_id:
                status = "scanning"
            elif now < retry_at:
                status = "cooldown"
            else:
                status = "scheduled"
            schedules.append({
                "id": task_id, "name": task["name"], "enabled": bool(task.get("enabled")),
                "src_dir": task["src_dir"], "dst_dir": task["dst_dir"],
                "interval_seconds": interval, "last_scan_at": last,
                "next_scan_at": (retry_at if task_id in self._scan_retry_at else
                                 (last + interval) if last is not None else now)
                if task.get("enabled") else None,
                "status": status, "request_id": progress.get("request_id") if running else None,
                "backfill_nfo": bool(progress.get("backfill_nfo")) if running else False,
                "backfill_checked": progress.get("backfill_checked", 0) if running else 0,
                "backfill_total": progress.get("backfill_total", 0) if running else 0,
                "current": progress.get("current") if running else
                (self._active_scan_state.get("current") if status == "scanning"
                 and self._active_scan_state else None),
                "scanned": progress.get("scanned", 0) if running else
                (self._active_scan_state.get("scanned", 0) if status == "scanning"
                 and self._active_scan_state else 0),
                "enqueued": progress.get("enqueued", 0) if running else
                (self._active_scan_state.get("enqueued", 0) if status == "scanning"
                 and self._active_scan_state else 0),
            })
        return {
            "server_time": now,
            "scan_schedules": schedules if tasks is not None else None,
            "moved_today": self.moved_today,
            "pending": q.get("pending", 0) if q is not None else None,
            "transferring": q.get("transferring", 0) if q is not None else None,
            "failed": q.get("failed", 0) if q is not None else None,
            "conflict": q.get("conflict", 0) if q is not None else None,
            "done": q.get("done", 0) if q is not None else None,
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
            "accounting_pending": q.get("accounting_pending", 0) if q is not None else None,
            "last_accounting_error": self.last_accounting_error,
            "stats_error": stats_error,
        }

    # ---------- 扫描循环 ----------

    def _scan_loop(self):
        def heartbeat():
            while not self._stop.wait(self.HEARTBEAT_INTERVAL):
                state = self._last_scan_phase
                if state:
                    task_id, phase, started = state
                    logger.warning("扫描线程仍在处理：task_id=%s 阶段=%s 耗时=%.1fs",
                                   task_id, phase, time.monotonic() - started)

        threading.Thread(target=heartbeat, name="fm-scan-loop-report", daemon=True).start()
        try:
            while not self._stop.is_set():
                timeout = 2
                try:
                    # 即使配置或任务表暂时不可用，也先完成已请求的立即扫描。
                    self._last_scan_phase = (None, "处理立即同步请求", time.monotonic())
                    self._process_sync_requests()
                    self._last_scan_phase = (None, "读取全局配置", time.monotonic())
                    cfg = config.get()
                    now = time.time()
                    self._last_scan_phase = (None, "读取任务列表", time.monotonic())
                    tasks = db.list_tasks()
                    for task in tasks:
                        if self._sync_requests:
                            break
                        if not task.get("enabled"):
                            continue
                        interval = task.get("scan_interval") or cfg["scan_interval"]
                        retry_at = self._scan_retry_at.get(task["id"])
                        if ((retry_at is not None and now < retry_at) or
                                (retry_at is None and now - self.last_scan_ts.get(task["id"], 0) < interval)):
                            continue
                        if self._sync_requests or not self._scan_lock.acquire(timeout=0.1):
                            break
                        try:
                            self._last_scan_phase = None  # 由扫描自身的心跳报告具体阶段
                            self._run_scan(task)
                            self.last_scan_success_at = time.time()
                            self._scan_succeeded(task["id"])
                        except Exception as exc:
                            self._scan_failed(exc, task["id"])
                            # 不依赖用户设置的扫描间隔，持续 EIO 最多每分钟重试一次。
                            self._pause_task_for_scan(task["id"])
                        finally:
                            self.last_scan_ts[task["id"]] = time.time()
                            self._scan_lock.release()
                            self._last_scan_phase = None
                    self._last_scan_phase = (None, "处理立即同步请求", time.monotonic())
                    self._process_sync_requests()
                    timeout = min(5, max(1, cfg["scan_interval"] / 6))
                except Exception as exc:
                    self._scan_failed(exc)
                finally:
                    self._last_scan_phase = None
                self._scan_wake.wait(timeout=timeout)
                self._scan_wake.clear()
        finally:
            self._last_scan_phase = None
            with self._requests_lock:
                for task_id, prog in self.sync_progress.items():
                    if prog.get("running"):
                        prog.update(running=False, done=True, error="扫描线程已退出，请重启服务")
                self._sync_requests.clear()

    def _pause_task_for_scan(self, task_id):
        retry_at = time.time() + 60
        self._scan_retry_at[task_id] = retry_at
        db.set_scan_failure(task_id, retry_at)

    def _scan_failed(self, exc, task_id=None):
        logger.error("扫描故障：task_id=%s 错误=%s", task_id, exc,
                     exc_info=(type(exc), exc, exc.__traceback__))
        self.last_scan_error = (f"任务 {task_id}: {exc}" if task_id is not None else str(exc))
        self.last_scan_error_at = time.time()
        self._last_scan_error_task_id = task_id
        self._last_scan_error_path = exc.filename if isinstance(exc, OSError) else None

    def _scan_succeeded(self, task_id):
        # 只有成功扫描且清理凭证已落盘，重启后才能继续迁移。
        db.clear_scan_failure(task_id)
        self._scan_retry_at.pop(task_id, None)
        # 仅同一任务的成功扫描可解除它的旧告警，其他任务的成功不能掩盖故障。
        if self._last_scan_error_task_id == task_id:
            logger.info("扫描故障已恢复：task_id=%s", task_id)
            self.last_scan_error = None
            self.last_scan_error_at = None
            self._last_scan_error_task_id = None
            self._last_scan_error_path = None

    def _task_paths_safe(self, task):
        from .main import validate_task_paths
        try:
            validate_task_paths(task, db.list_tasks(), task["id"])
            self._path_warning_at.pop(task["id"], None)
            return True
        except (ValueError, RuntimeError, OSError) as exc:
            now = time.monotonic()
            if now - self._path_warning_at.get(task["id"], float("-inf")) >= 60:
                logger.warning("任务路径校验失败：task_id=%s 错误=%s", task["id"], exc)
                self._path_warning_at[task["id"]] = now
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
        def heartbeat():
            while not self._stop.wait(self.HEARTBEAT_INTERVAL):
                state = self._last_work_phase
                if state:
                    phase, started = state
                    logger.warning("迁移线程仍在处理：阶段=%s 耗时=%.1fs",
                                   phase, time.monotonic() - started)

        threading.Thread(target=heartbeat, name="fm-work-loop-report", daemon=True).start()
        while not self._stop.is_set():
            try:
                self._work_once()
            except Exception as exc:
                logger.exception("迁移线程异常：当前文件=%s", self.current_file)
                self.last_worker_error = str(exc)
                self.current_file = None
                self._stop.wait(timeout=2)
            finally:
                self._last_work_phase = None

    def _log(self, item, duration_ms, result, detail):
        level = logging.INFO if result == "success" else logging.WARNING
        logger.log(level, "迁移结果：task_id=%s queue_id=%s 文件=%s 结果=%s 耗时=%dms 详情=%s",
                   item["task_id"], item["id"], item["rel_path"], result, duration_ms, detail)
        try:
            db.add_log(item["task_id"], item["id"], item["src_path"], item["dst_path"],
                       item.get("size", 0), duration_ms, result, detail)
        except Exception as exc:
            self.last_worker_error = f"日志写入失败 ({item['id']}): {exc}; {detail}"
            logger.exception("迁移日志入库失败：task_id=%s queue_id=%s", item["task_id"], item["id"])

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
            self._last_work_phase = (f"补偿队列项 {item['id']}", time.monotonic())
            logger.info("记账补偿开始：task_id=%s queue_id=%s 文件=%s",
                        item['task_id'], item['id'], item['rel_path'])
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
                logger.warning("%s", self.last_accounting_error)
                continue
            try:
                db.complete_accounting(item)
            except Exception as exc:
                self._accounting_retry_at[item['id']] = time.monotonic() + 30
                self.last_accounting_error = f"记账待补偿 ({item['id']}): {exc}；30s 后重试"
                logger.warning("%s", self.last_accounting_error)
                continue
            self._accounting_retry_at.pop(item['id'], None)
            self.last_accounting_error = None
            logger.info("记账补偿完成：task_id=%s queue_id=%s", item['task_id'], item['id'])
            self._log(item, 0, 'success', '目标哈希复核通过，记账补偿完成')

    def _work_once(self):
        if self._scan_io_waiting.is_set():
            self._stop.wait(0.1)
            return
        with self._io_lock:
            if self._scan_io_waiting.is_set():
                return
            idle_timeout = self._work_once_locked()
        if idle_timeout is not None:
            self._work_wake.wait(timeout=idle_timeout)
            self._work_wake.clear()

    def _work_once_locked(self):
        for queue_id in tuple(self._unstarted_claims):
            self._last_work_phase = (f"回退迁移前认领 {queue_id}", time.monotonic())
            restored = db.release_unstarted_claim(queue_id)
            self._unstarted_claims.discard(queue_id)
            logger.info("迁移前认领已处理：queue_id=%s 回退数=%s", queue_id, restored)
        self._last_work_phase = ("检查待补偿记账", time.monotonic())
        self._retry_accounting()
        self._last_work_phase = ("读取任务列表", time.monotonic())
        task_map = {t["id"]: t for t in db.list_tasks()}
        now = time.monotonic()
        # 扫描已发现该任务目录故障时，不仅延迟下次扫描，也暂停其迁移写入。
        # 即使冷却时间到了，也须先完成一次成功扫描才能解除暂停。
        self._last_work_phase = ("筛选可迁移任务", time.monotonic())
        eligible = [tid for tid, task in task_map.items() if task.get("enabled")
                    and tid not in self._scan_retry_at
                    and now >= self._next_eligible.get(tid, 0)
                    and self._in_run_window(task) and self._task_paths_safe(task)]
        self._last_work_phase = ("领取待迁移文件", time.monotonic())
        item = db.take_next_pending(eligible)
        if item is None:
            self._last_work_phase = None
            self.current_file = None
            deadlines = [deadline for tid, deadline in self._next_eligible.items()
                         if tid in task_map and task_map[tid].get("enabled") and deadline > now]
            return min(1, max(0, min(deadlines) - now)) if deadlines else 1
        self._last_work_phase = (f"检查队列项 {item['id']}", time.monotonic())
        try:
            task = task_map.get(item["task_id"])
            if task is None or db.get_task(item["task_id"]) is None:
                logger.warning("孤儿队列清理：task_id=%s queue_id=%s", item["task_id"], item["id"])
                db.cleanup_orphan_queue(item["id"])
                return
            if not task.get("enabled") or not self._in_run_window(task):
                db.set_status(item["id"], "pending")
                return
        except Exception:
            # 尚未开始传输，可安全释放认领；传输后的异常不可用同一策略处理。
            self._unstarted_claims.add(item["id"])
            try:
                db.release_unstarted_claim(item["id"])
            except Exception:
                logger.exception("迁移前认领回退失败：queue_id=%s，待数据库恢复后重试", item["id"])
            else:
                self._unstarted_claims.discard(item["id"])
            raise

        self._last_work_phase = None
        self.current_file = item["rel_path"]
        started = time.monotonic()
        logger.info("迁移开始：task_id=%s queue_id=%s 文件=%s 大小=%s 源=%s 目标=%s",
                    item["task_id"], item["id"], item["rel_path"], item["size"],
                    item["src_path"], item["dst_path"])
        finished = threading.Event()
        stage = ["检查源文件"]

        def heartbeat():
            while not finished.wait(self.HEARTBEAT_INTERVAL):
                logger.warning("迁移仍在进行：task_id=%s queue_id=%s 文件=%s 阶段=%s 耗时=%.1fs",
                               item["task_id"], item["id"], item["rel_path"], stage[0],
                               time.monotonic() - started)

        threading.Thread(target=heartbeat, name=f"fm-work-report-{item['id']}", daemon=True).start()
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
                        stage[0] = "复制并校验目标"
                        result, detail = transfer.transfer(item, task)
                    except Exception as exc:
                        logger.exception("迁移执行异常：task_id=%s queue_id=%s 文件=%s",
                                         item["task_id"], item["id"], item["rel_path"])
                        result, detail = "failed", f"迁移异常: {exc}"
            duration_ms = int((time.monotonic() - started) * 1000)
            stage[0] = "更新队列与记账"
            if result == "success":
                self.moved_today += 1
                try:
                    db.persist_accounting_pending(item['id'], item, item.get('_verified_target'))
                except Exception as exc:
                    self.last_accounting_error = f"凭证落盘失败 ({item['id']}): {exc}；保留 transferring 供人工核查"
                    logger.exception("%s", self.last_accounting_error)
                    detail += f"；{self.last_accounting_error}"
                else:
                    try:
                        # 正常路径已有传输阶段全量读回，无需再读网盘。
                        pending = db.get_accounting_pending(item['id'])
                        self._verify_accounting(pending, task, rehash=False)
                        db.complete_accounting(pending)
                    except Exception as exc:
                        self.last_accounting_error = f"记账待补偿 ({item['id']}): {exc}"
                        logger.exception("%s", self.last_accounting_error)
                        detail += f"；{self.last_accounting_error}"
                    else:
                        self.last_accounting_error = None
            elif result == "conflict":
                db.set_status(item["id"], "conflict")
            elif result == "failed":
                if transient_source_io or item.get("_transient_io"):
                    # 源/目标挂载 EIO 不消耗迁移重试额度，需成功扫描后才恢复写入。
                    logger.warning("迁移暂停并等待扫描恢复：task_id=%s queue_id=%s 源=%s",
                                   task["id"], item["id"], item["src_path"])
                    self._pause_task_for_scan(task["id"])
                    db.set_status(item["id"], "failed", next_retry_at=time.time() + 60)
                    detail += "；挂载 I/O 故障，60s 后限速探测恢复"
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
            finished.set()
            self.current_file = None
            interval = max(0.0, float(task.get("interval_seconds", 5)))
            self._next_eligible[item["task_id"]] = time.monotonic() + interval


scheduler = Scheduler()
