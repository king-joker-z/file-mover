"""后台线程：扫描循环 + 迁移主循环（节流逐个迁移）"""
import contextlib
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
    RETENTION_INTERVAL = 3600  # 数据保留清理周期；启动后首次延迟执行
    STAGE_LOG_MIN_SECONDS = 10  # 阶段切换时上一阶段达到该耗时才记 INFO（过滤快速阶段噪音）
    STAGE_WARN_SECONDS = 300  # 单阶段停留超过该时长判定疑似卡住
    STAGE_WARN_REPEAT_SECONDS = 600  # 疑似卡住 WARNING 的重发间隔
    # 不超过该大小的文件按目标挂载串行传输：小文件单耗时极短，并发几乎不提升
    # 吞吐，却把每秒 RPC 数量放大 N 倍，网盘挂载（CloudDrive2/115 等）对小请求
    # 的限流阈值最低，表现为 EIO 与读回不一致。串行化只针对小文件，大文件仍
    # 按 transfer_workers 并发。
    SMALL_TRANSFER_SERIAL_BYTES = 8 << 20
    # 挂载级瞬时 I/O 防护：任一传输遇挂载瞬时 I/O 故障（网盘限流的主要表现）时
    # 打开降级窗口——窗口内该挂载的全部传输（含大文件）串行，避免多线程同时
    # 撞限流把一次抖动放大成整批失败；窗口到期自动恢复并发。失败项不再加
    # 挂载冷却：工人线程绝不空等挂载，瞬时抖动由内联重试就地吸收，持续故障
    # 快速落失败队列走 60s 常规重试 + 任务暂停恢复（与用户只见一套倒计时）。
    MOUNT_DEGRADED_WINDOW_SECONDS = 120
    # 单文件因挂载瞬时 I/O 放弃前最多尝试次数（含首次）；尝试之间短暂停顿，
    # 给读回缓存/限流一个恢复窗口。内联吸收成功则不计失败。
    MOUNT_TRANSIENT_ATTEMPTS = 3
    MOUNT_TRANSIENT_RETRY_DELAY = 2
    # 多个手动同步排队时，两次扫描之间的让位时长：迁移线程可趁机领取新文件，
    # 冷却到期的恢复扫描也能插队，避免连续整树扫描把上传长时间完全饿死。
    SCAN_QUEUE_YIELD_SECONDS = 3

    def __init__(self):
        self._stop = threading.Event()
        self._threads = []
        self._async_threads = []  # 异步迁移线程池：仅领取开启异步的任务
        self._lifecycle_lock = threading.Lock()
        self._scan_wake = threading.Event()
        self._work_wake = threading.Event()
        self._sync_requests = set()
        self._scan_lock = threading.Lock()
        # _io_lock 只互斥「扫描枚举目录」与「领取队列项」这两个瞬时操作；
        # 迁移本身在锁外执行（与异步在途传输同等语义），否则单个慢/卡死的
        # 传输会永久持有 _io_lock，阻塞所有扫描（含手动同步）并冻结全局。
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
        self._busy_tasks = {}  # task_id -> 在途迁移数；同一任务最多并发 transfer_workers 个
        self._small_locks = {}  # 挂载 realpath -> 传输互斥锁（小文件常串行；降级窗口内全部串行）
        self._mount_breaker = {}  # 挂载 realpath -> 降级串行窗口截止时间
        self._mount_transient_absorbed = 0  # 内联重试吸收的挂载瞬时 I/O 次数（供仪表盘解释串行保护）
        self._stats_lock = threading.Lock()
        self._current_transfers = {}  # queue_id -> {"rel_path","task_id","stage"}
        self._next_retention_at = time.monotonic() + 60  # 启动后稍作延迟再首次清理
        self.last_retention_result = None
        self.last_retention_error = None

    @property
    def current_file(self):
        """当前正在迁移的文件（串行 + 异步全部在途项，分号连接）。"""
        with self._stats_lock:
            entries = list(self._current_transfers.values())
        return "; ".join(entry["rel_path"] for entry in entries) or None

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
                             ("worker", self._work_loop),
                             ("stage-watchdog", self._stage_watchdog_loop)):
            t = threading.Thread(target=target, name=f"fm-{name}", daemon=True)
            t.start()
            self._threads.append(t)
        self._ensure_async_workers()
        logger.info("后台线程已启动：任务数=%d 异步迁移线程=%d",
                    len(tasks), len(self._async_threads))

    def stop(self):
        logger.info("服务停止：等待后台任务退出")
        self._stop.set()
        self._scan_wake.set()
        self._work_wake.set()
        with self._lifecycle_lock:
            threads = list(self._threads) + list(self._async_threads)
        for thread in threads:
            thread.join()  # 正在传输的文件完成验证后才允许服务退出
        self._threads.clear()
        self._async_threads.clear()
        logger.info("后台线程已退出")

    def _stage_watchdog_loop(self):
        """周期巡检在途迁移：单阶段停留过久时记 WARNING，让卡点从日志可查。

        慢挂载上长阶段（如大文件上传）属正常现象，日志中同时给出复制进度与
        进度最后更新时间，便于区分「慢」与「卡」。
        """
        while not self._stop.wait(30):
            now = time.time()
            with self._stats_lock:
                entries = [dict(entry) for entry in self._current_transfers.values()]
            for entry in entries:
                since = entry.get("stage_since") or entry.get("started_at") or now
                elapsed = now - since
                if elapsed < self.STAGE_WARN_SECONDS:
                    continue
                if now - entry.get("last_stage_warn_at", 0.0) < self.STAGE_WARN_REPEAT_SECONDS:
                    continue
                with self._stats_lock:
                    current = self._current_transfers.get(entry["id"])
                    if current is None or current.get("stage") != entry.get("stage"):
                        continue
                    current["last_stage_warn_at"] = now
                total = entry.get("total_bytes") or 0
                copied = entry.get("copied_bytes") or 0
                progress_age = now - (entry.get("updated_at") or since)
                progress = (f"{copied / 1e9:.2f}/{total / 1e9:.2f}GB" if total >= 1e9
                            else f"{copied}/{total} 字节") if total else "无复制进度"
                logger.warning(
                    "迁移阶段疑似卡住：task_id=%s queue_id=%s 文件=%s 阶段=%s 已持续 %.0f 秒"
                    "（进度 %s，进度最后更新于 %.0f 秒前）；挂载无响应、网盘风控或 rename"
                    "卡死都会表现为此状态，请结合网络流量判断",
                    entry.get("task_id"), entry.get("id"), entry.get("rel_path"),
                    entry.get("stage"), elapsed, progress, progress_age)

    def _desired_async_workers(self, configured) -> int:
        """异步线程池目标大小：全局设置与已启用异步任务并发迁移数的最大值。

        任务内并发（transfer_workers）依赖异步线程池：线程不够时任务并发
        会被隐式限制，因此按任务需求自动补足，避免两个设置需要手动对齐。
        """
        want = max(1, min(8, int(configured)))
        try:
            need = max((db.normalize_transfer_workers(task.get("transfer_workers"))
                        for task in db.list_tasks()
                        if task.get("enabled") and task.get("async_transfer")),
                       default=0)
        except Exception:
            need = 0
        return max(want, min(8, need))

    def _ensure_async_workers(self, want=None):
        """按全局设置与任务并发需求补足异步迁移线程；调大即时生效，调小重启后生效。"""
        if want is None:
            try:
                want = int(config.get().get("max_async_transfers", 2))
            except Exception:
                want = 2
        want = self._desired_async_workers(want)
        with self._lifecycle_lock:
            while len(self._async_threads) < want:
                if self._stop.is_set():
                    return
                thread = threading.Thread(
                    target=self._async_work_loop,
                    name=f"fm-async-{len(self._async_threads) + 1}", daemon=True)
                thread.start()
                self._async_threads.append(thread)

    def wake_scan(self):
        self._scan_wake.set()
        self._work_wake.set()

    def retry_accounting(self, queue_id: int) -> bool:
        """人工重试记账：解除待核查/限速标记，迁移线程将立即重新校验目标并补记账。

        待记账项的源文件通常已删除，不能按普通迁移重传；只能重跑目标校验。
        """
        try:
            db.get_accounting_pending(queue_id)
        except ValueError:
            return False
        self._accounting_blocked.discard(queue_id)
        self._accounting_retry_at.pop(queue_id, None)
        self._work_wake.set()
        logger.info("人工重试记账：queue_id=%s", queue_id)
        return True

    def forget_queue_item(self, queue_id: int) -> None:
        """人工移除队列项后清理内存跟踪，避免补偿/限速/回退认领状态残留。"""
        self._accounting_blocked.discard(queue_id)
        self._accounting_retry_at.pop(queue_id, None)
        self._unstarted_claims.discard(queue_id)

    def request_retention(self):
        """请求尽快执行一次数据保留清理（如设置页刚改过保留天数）。"""
        self._next_retention_at = 0.0
        self._scan_wake.set()

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
        db.mark_round_scanning(task_id)
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
                except Exception as exc:
                    # 故障熔断必须在释放 I/O 锁前生效，否则迁移线程可能抢到下一文件。
                    self._pause_task_for_scan(task_id)
                    if (isinstance(exc, OSError) and exc.errno == errno.EIO
                            and exc.filename and getattr(exc, '_filemover_eio_path_type', None)
                            in ('file', 'directory')):
                        rel = os.path.relpath(exc.filename, task['src_dir'])
                        if rel != '.' and not rel.startswith('..' + os.sep) and rel != '..':
                            origin = exc._filemover_eio_path_type
                            excluded = db.record_eio_failure(task_id, None, rel, origin, str(exc))
                            if excluded:
                                logger.warning('扫描 EIO 路径已自动排除：task_id=%s 路径=%s 类型=%s',
                                               task_id, rel, origin)
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
            try:
                db.finish_round_scan(task_id)
            finally:
                # 收尾自身抛错（如 DB 故障）时也必须复位，否则 stats() 永远显示 scanning。
                self._scan_io_waiting.clear()
                finished.set()
                self._active_scan_task_id = None
                self._active_scan_state = None

    def _process_sync_requests(self):
        """在 _scan_loop 线程内执行：每次处理一个立即同步请求。

        多个任务同时触发手动同步时逐个处理，且两次扫描之间短暂让出
        （此时 _scan_io_waiting 已清除）：迁移线程可趁机领取新文件，
        避免背靠背的整树扫描把上传长时间完全饿死。
        """
        with self._requests_lock:
            if not self._sync_requests:
                return
            task_id = self._sync_requests.pop()
        prog = self.sync_progress.get(task_id)
        if not prog:
            return
        self._last_scan_phase = (task_id, "立即同步准备中", time.monotonic())
        try:
            if time.time() < self._scan_retry_at.get(task_id, 0):
                prog["error"] = "目录故障后冷却中，请至少等待 60 秒再同步"
                logger.warning("立即同步冷却中：task_id=%s 剩余=%.1fs", task_id,
                               self._scan_retry_at[task_id] - time.time())
            else:
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
        if self._sync_requests and not self._stop.is_set():
            self._stop.wait(self.SCAN_QUEUE_YIELD_SECONDS)

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
        try:
            rounds = db.latest_upload_rounds() if tasks is not None else {}
        except Exception as exc:
            rounds = {}
            errors.append(f"轮次统计失败: {exc}")
        stats_error = "；".join(errors) if errors else None
        now = time.time()
        with self._stats_lock:
            transfers = [dict(entry) for entry in self._current_transfers.values()]
            mount_transient_absorbed = self._mount_transient_absorbed
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
            elif task.get("paused"):
                status = "paused"  # 迁移已暂停；扫描继续，只是不领取新文件
            elif now < retry_at:
                status = "cooldown"
            elif task.get("manual_only"):
                status = "manual_wait"  # 仅手动扫描：不参与自动调度，等待手动触发
            else:
                status = "scheduled"
            if not task.get("enabled"):
                next_scan = None
            elif task.get("manual_only"):
                # 仅手动扫描无自动计划；故障冷却时展示剩余冷却时间。
                next_scan = retry_at if task_id in self._scan_retry_at else None
            else:
                next_scan = (retry_at if task_id in self._scan_retry_at else
                             (last + interval) if last is not None else now)
            schedules.append({
                "id": task_id, "name": task["name"], "enabled": bool(task.get("enabled")),
                "round": rounds.get(task_id),
                "src_dir": task["src_dir"], "dst_dir": task["dst_dir"],
                "interval_seconds": interval, "last_scan_at": last,
                "next_scan_at": next_scan,
                "manual_only": bool(task.get("manual_only")),
                "paused": bool(task.get("paused")),
                "async_transfer": bool(task.get("async_transfer")),
                "transfer_workers": db.normalize_transfer_workers(task.get("transfer_workers")),
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
            "current_files": transfers,
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
            "last_retention_error": self.last_retention_error,
            "mount_breakers": self._mount_breaker_snapshot(),
            "mount_transient_absorbed": mount_transient_absorbed,
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
                    # 异步迁移线程数调大后即时补足；复用已读取的 cfg，不额外访问配置。
                    self._ensure_async_workers(cfg.get("max_async_transfers", 2))
                    now = time.time()
                    self._last_scan_phase = (None, "读取任务列表", time.monotonic())
                    tasks = db.list_tasks()
                    self._scan_due_tasks(cfg, tasks, now)
                    self._last_scan_phase = (None, "处理立即同步请求", time.monotonic())
                    self._process_sync_requests()
                    now_monotonic = time.monotonic()
                    if now_monotonic >= self._next_retention_at:
                        # 清理只碰数据库，不访问挂载；失败不影响扫描节奏。
                        self._last_scan_phase = (None, "数据保留清理", time.monotonic())
                        try:
                            self._run_retention()
                        except Exception as exc:
                            self.last_retention_error = str(exc)
                            logger.warning("数据保留清理失败：%s", exc)
                        self._next_retention_at = now_monotonic + self.RETENTION_INTERVAL
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

    def _scan_due_tasks(self, cfg, tasks, now):
        """扫描到期任务；手动同步排队期间仅放行冷却到期的恢复扫描。

        传输 EIO 熔断的任务需要一次成功扫描才能恢复；若恢复扫描被排队的
        手动请求完全挡住，熔断会随着排队时长被无限拉长，因此让冷却到期的
        任务插队，其余自动扫描仍让位于手动请求。
        """
        for task in tasks:
            if self._sync_requests and task["id"] not in self._scan_retry_at:
                continue
            # 仅手动扫描的任务不参与自动调度，等待「立即扫描」触发。
            if not task.get("enabled") or task.get("manual_only"):
                continue
            interval = task.get("scan_interval") or cfg["scan_interval"]
            retry_at = self._scan_retry_at.get(task["id"])
            if ((retry_at is not None and now < retry_at) or
                    (retry_at is None and now - self.last_scan_ts.get(task["id"], 0) < interval)):
                continue
            if not self._scan_lock.acquire(timeout=0.1):
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

    # ---------- 数据保留清理 ----------

    def _run_retention(self):
        cfg = config.get()
        result = db.cleanup_expired(cfg.get("log_retention_days", 0),
                                    cfg.get("round_retention_days", 0))
        if any(result.values()):
            logger.info("数据保留清理完成：日志=%d 轮次=%d 轮次文件=%d",
                        result["logs"], result["rounds"], result["round_files"])
        self.last_retention_result = result
        self.last_retention_error = None

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
                if rehash:
                    with os.fdopen(os.dup(target_fd), 'rb') as stream:
                        if item['verified_trusted']:
                            digest = hashlib.sha256()
                            for chunk in iter(lambda: stream.read(transfer.COPY_CHUNK_SIZE), b''):
                                digest.update(chunk)
                            actual = digest.hexdigest()
                        else:
                            # 采样凭证（trusted=False）按同等强度做头尾采样复核
                            actual = transfer._sampled_stream_digest(stream, before.st_size)
                else:
                    actual = None
                after = os.fstat(target_fd)
                named = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
                identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns)
                if ((rehash and actual != item['verified_sha256'])
                        or identity(before) != identity(after)
                        or identity(after) != identity(named)):
                    raise OSError('目标读回复核与已校验哈希不一致，需人工核查')
            finally:
                os.close(target_fd)
        finally:
            os.close(fd)
        try:
            source_stat = os.lstat(src)
        except FileNotFoundError:
            # 只有留源任务（NFO 内容修正须与源核对）才要求源文件仍在场；
            # 删源任务的源已在传输阶段按任务校验强度删除，凭证本身即删除
            # 授权——采样凭证（trusted=False）同样适用，否则 fast/off 去重
            # 命中会全部误落「未授权」人工核查。
            if not item['verified_trusted'] and task.get("after_action", "delete") == "keep":
                raise OSError('未授权目标且源文件缺失，需人工核查')
        else:
            if src.lower().endswith('.nfo'):
                expected, source_digest = ((item['source_size'], item['source_sha256']),
                                           transfer._sha256(src))
            elif item['verified_trusted']:
                expected, source_digest = ((item['verified_size'], item['verified_sha256']),
                                           transfer._sha256(src))
            else:
                # 采样凭证：源同样按头尾采样复核，与凭证判定强度一致
                expected, source_digest = ((item['verified_size'], item['verified_sha256']),
                                           transfer._sampled_digest(src))
            if (not stat.S_ISREG(source_stat.st_mode)
                    or os.path.commonpath((src_root, os.path.realpath(src))) != src_root
                    or expected[1] is None
                    or (source_stat.st_size, source_digest) != expected):
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
        # 记账补偿含目标/源全量哈希读回，属于慢挂载 I/O；必须在 _io_lock 外执行，
        # 保证扫描随时能拿到 _io_lock（否则挂载卡死会连带阻塞所有扫描）。
        self._last_work_phase = ("检查待补偿记账", time.monotonic())
        try:
            self._retry_accounting()
        finally:
            self._last_work_phase = None
        with self._io_lock:
            if self._scan_io_waiting.is_set():
                return
            item, task_map, idle_timeout = self._claim_next_locked()
        if item is None:
            if idle_timeout is not None:
                self._work_wake.wait(timeout=idle_timeout)
                self._work_wake.clear()
            return
        # 迁移在 _io_lock 外执行，与异步在途传输同等语义：扫描等待期间只停止
        # 领取新文件，不等待在途文件完成；单个慢/卡住的传输不再阻塞扫描。
        self._execute_transfer(item, task_map)

    def _claim_next_locked(self):
        """在 _io_lock 内领取一个待迁移队列项；返回 (item, task_map, idle_timeout)。"""
        for queue_id in tuple(self._unstarted_claims):
            self._last_work_phase = (f"回退迁移前认领 {queue_id}", time.monotonic())
            restored = db.release_unstarted_claim(queue_id)
            self._unstarted_claims.discard(queue_id)
            logger.info("迁移前认领已处理：queue_id=%s 回退数=%s", queue_id, restored)
        self._last_work_phase = ("读取任务列表", time.monotonic())
        task_map = {t["id"]: t for t in db.list_tasks()}
        now = time.monotonic()
        # 扫描已发现该任务目录故障时，不仅延迟下次扫描，也暂停其迁移写入。
        # 即使冷却时间到了，也须先完成一次成功扫描才能解除暂停。
        # 开启异步的任务由异步线程池处理；串行线程只处理同步任务，
        # 这样超大文件（异步）不会阻塞其它任务的逐个迁移。
        self._last_work_phase = ("筛选可迁移任务", time.monotonic())
        workers = {tid: db.normalize_transfer_workers(task.get("transfer_workers"))
                   for tid, task in task_map.items()}
        eligible = [tid for tid, task in task_map.items() if task.get("enabled")
                    and not task.get("paused")
                    and not task.get("async_transfer")
                    and tid not in self._scan_retry_at
                    and self._busy_tasks.get(tid, 0) < workers[tid]
                    and now >= self._next_eligible.get(tid, 0)
                    and self._in_run_window(task) and self._task_paths_safe(task)]
        self._last_work_phase = ("领取待迁移文件", time.monotonic())
        item = self._claim_item(eligible, workers)
        if item is None:
            self._last_work_phase = None
            deadlines = [deadline for tid, deadline in self._next_eligible.items()
                         if tid in task_map and task_map[tid].get("enabled") and deadline > now]
            return None, None, min(1, max(0, min(deadlines) - now)) if deadlines else 1
        return item, task_map, None

    def _claim_item(self, eligible_task_ids, workers=None):
        """原子认领一条队列项并登记任务占用；同一任务最多 transfer_workers 个在途迁移。"""
        if workers is None:
            workers = {}
        with self._stats_lock:
            item = db.take_next_pending(eligible_task_ids)
            if item is None:
                return None
            if self._busy_tasks.get(item["task_id"], 0) >= workers.get(item["task_id"], 1):
                conflict = True
            else:
                self._busy_tasks[item["task_id"]] = self._busy_tasks.get(item["task_id"], 0) + 1
                conflict = False
        if conflict:
            # 另一个线程抢先开始了同一任务的迁移；回退本次认领，稍后重试。
            try:
                db.release_unstarted_claim(item["id"])
            except Exception:
                self._unstarted_claims.add(item["id"])
            return None
        return item

    def _async_work_loop(self):
        """异步迁移线程：只领取开启异步的任务，与串行迁移线程并行执行。"""
        while not self._stop.is_set():
            executed = False
            try:
                executed = self._async_work_once()
            except Exception as exc:
                logger.exception("异步迁移线程异常")
                self.last_worker_error = str(exc)
            if not executed:
                self._work_wake.wait(timeout=1)
                self._work_wake.clear()

    def _async_work_once(self) -> bool:
        # 扫描等待/进行中时暂停领取新文件，避免与目录枚举争抢慢挂载；
        # 已在传输的异步文件不中断，会与扫描并行完成。
        if self._scan_io_waiting.is_set():
            return False
        task_map = {t["id"]: t for t in db.list_tasks()}
        now = time.monotonic()
        workers = {tid: db.normalize_transfer_workers(task.get("transfer_workers"))
                   for tid, task in task_map.items()}
        eligible = [tid for tid, task in task_map.items() if task.get("enabled")
                    and task.get("async_transfer")
                    and not task.get("paused")
                    and tid not in self._scan_retry_at
                    and self._busy_tasks.get(tid, 0) < workers[tid]
                    and now >= self._next_eligible.get(tid, 0)
                    and self._in_run_window(task) and self._task_paths_safe(task)]
        if not eligible:
            return False
        item = self._claim_item(eligible, workers)
        if item is None:
            return False
        self._execute_transfer(item, task_map, track_phase=False)
        return True

    # ---------- 挂载级瞬时 I/O 防护 ----------

    @staticmethod
    def _mount_key(path: str) -> str:
        """挂载标识：realpath 归一化后按挂载聚合限流状态。"""
        return os.path.realpath(path)

    def _mount_degraded(self, mount_key) -> bool:
        """降级窗口内返回 True：该挂载的所有传输串行，遏制并发放大限流。"""
        with self._stats_lock:
            state = self._mount_breaker.get(mount_key)
            return bool(state and time.monotonic() < state["degraded_until"])

    def _mount_report_failure(self, mount_key) -> None:
        """传输遇挂载瞬时 I/O 故障：打开降级串行窗口，遏制并发放大限流。

        不再加冷却退避、也不让工人线程等待：瞬时抖动由内联重试就地吸收
        （秒级停顿），持续故障快速落失败队列走 60s 常规重试 + 任务暂停恢复。
        """
        with self._stats_lock:
            self._mount_breaker.setdefault(mount_key, {})["degraded_until"] = \
                time.monotonic() + self.MOUNT_DEGRADED_WINDOW_SECONDS

    def _mount_serial_lock(self, mount_key) -> threading.Lock:
        """按挂载返回传输互斥锁；不同挂载互不阻塞。"""
        with self._stats_lock:
            lock = self._small_locks.get(mount_key)
            if lock is None:
                lock = self._small_locks[mount_key] = threading.Lock()
            return lock

    def _mount_breaker_snapshot(self) -> list:
        """当前处于降级串行窗口的挂载：让「串行保护」在仪表盘上可解释。

        瞬时 I/O 被内联重试吸收后队列与日志页只显示成功；窗口若不可见，
        传输变慢就成了「没报错却在等」的谜团，这里把状态暴露出来。
        """
        now = time.monotonic()
        with self._stats_lock:
            snapshot = []
            for mount, state in self._mount_breaker.items():
                degraded = max(0.0, state.get("degraded_until", 0.0) - now)
                if degraded > 0:
                    snapshot.append({"mount": mount,
                                     "degraded_seconds": int(degraded + 0.5)})
            return snapshot

    def _transfer_with_mount_state(self, item, task, dst_key, src_key,
                                   stage_cb, progress_cb):
        """按小文件规则与降级窗口决定传输串行或并发。

        小文件始终按目标挂载串行（RPC 密度高，并发只放大限流）；降级
        窗口内该挂载的所有传输（含大文件）也串行；多把锁按固定顺序获取，
        避免源/目标挂载同时降级时交叉等待死锁。
        """
        keys = []
        if (item.get("size") or 0) <= self.SMALL_TRANSFER_SERIAL_BYTES:
            keys.append(dst_key)
        for key in (dst_key, src_key):
            if key not in keys and self._mount_degraded(key):
                keys.append(key)
        keys.sort()  # 单锁无影响；多锁固定顺序防交叉等待
        if not keys:
            return transfer.transfer(item, task, stage_cb=stage_cb,
                                     progress_cb=progress_cb)
        with contextlib.ExitStack() as stack:
            for key in keys:
                stack.enter_context(self._mount_serial_lock(key))
            return transfer.transfer(item, task, stage_cb=stage_cb,
                                     progress_cb=progress_cb)

    def _execute_transfer(self, item, task_map, track_phase=True):
        """执行已认领队列项的迁移；串行与异步迁移线程共用。

        认领阶段（_claim_item）已登记任务占用计数，本函数退出时递减。
        """
        try:
            if track_phase:
                self._last_work_phase = (f"检查队列项 {item['id']}", time.monotonic())
            try:
                task = task_map.get(item["task_id"])
                if task is None or db.get_task(item["task_id"]) is None:
                    logger.warning("孤儿队列清理：task_id=%s queue_id=%s", item["task_id"], item["id"])
                    db.cleanup_orphan_queue(item["id"])
                    return
                if not task.get("enabled") or task.get("paused") or not self._in_run_window(task):
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

            stage = ["检查源文件"]
            with self._stats_lock:
                self._current_transfers[item["id"]] = {
                    "id": item["id"], "rel_path": item["rel_path"],
                    "task_id": item["task_id"], "stage": stage[0],
                    "total_bytes": item.get("size") or 0, "copied_bytes": 0,
                    "started_at": time.time(), "stage_since": time.time(),
                    "last_stage_warn_at": 0.0}

            def report_stage(name):
                now = time.time()
                prev_stage = None
                prev_elapsed = 0.0
                with self._stats_lock:
                    entry = self._current_transfers.get(item["id"])
                    if entry is not None:
                        prev_stage = entry.get("stage")
                        prev_elapsed = now - (entry.get("stage_since") or entry.get("started_at") or now)
                        entry["stage"] = name
                        entry["stage_since"] = now
                        entry["last_stage_warn_at"] = 0.0
                stage[0] = name  # 迁移中心跳读该局部列表，必须同步更新
                # 慢阶段切换记录耗时，形成每个文件的阶段时间线；快速阶段不产生噪音
                if prev_stage is not None and prev_elapsed >= self.STAGE_LOG_MIN_SECONDS:
                    logger.info("迁移阶段完成：task_id=%s 文件=%s 阶段=%s 耗时=%.1fs → %s",
                                item["task_id"], item["rel_path"], prev_stage, prev_elapsed, name)

            def report_progress(copied, total):
                with self._stats_lock:
                    entry = self._current_transfers.get(item["id"])
                    if entry is not None:
                        entry["copied_bytes"] = copied
                        if total:
                            entry["total_bytes"] = total
                        entry["updated_at"] = time.time()

            if track_phase:
                self._last_work_phase = None
            started = time.monotonic()
            logger.info("迁移开始：task_id=%s queue_id=%s 文件=%s 大小=%s 源=%s 目标=%s",
                        item["task_id"], item["id"], item["rel_path"], item["size"],
                        item["src_path"], item["dst_path"])
            finished = threading.Event()

            def heartbeat():
                while not finished.wait(self.HEARTBEAT_INTERVAL):
                    logger.warning("迁移仍在进行：task_id=%s queue_id=%s 文件=%s 阶段=%s 耗时=%.1fs",
                                   item["task_id"], item["id"], item["rel_path"], stage[0],
                                   time.monotonic() - started)

            threading.Thread(target=heartbeat, name=f"fm-work-report-{item['id']}", daemon=True).start()
            try:
                # 无持久化凭证的 transferring 重启后回退；已删源不可重传或盲目补记。
                mount_key = self._mount_key(task["dst_dir"])
                source_mount_key = self._mount_key(task["src_dir"])
                result, detail = "failed", "迁移未执行"
                for attempt in range(1, self.MOUNT_TRANSIENT_ATTEMPTS + 1):
                    transient_source_io = False
                    failed_mount = None
                    # 每次尝试重置瞬时故障标记，只有最后一次尝试的结果进入失败分支。
                    item.pop("_transient_io", None)
                    item.pop("_eio_origin", None)
                    try:
                        source_stat = os.lstat(item["src_path"])
                    except FileNotFoundError:
                        result, detail = "conflict", "源文件缺失，可能迁移成功但记账丢失；需人工核查目标与记录"
                        break
                    except OSError as exc:
                        transient_source_io = exc.errno in (
                            errno.EIO, errno.ESTALE, errno.ENOTCONN, errno.ETIMEDOUT, errno.EBUSY)
                        if transient_source_io:
                            failed_mount = source_mount_key
                        if exc.errno == errno.EIO:
                            item['_eio_origin'] = 'source'
                        result, detail = "failed", f"源路径读取失败，已保留待重试: {exc}"
                    else:
                        if not stat.S_ISREG(source_stat.st_mode):
                            result, detail = "conflict", "源不是普通文件，可能是历史残留；需人工核查目标与记录"
                            break
                        try:
                            report_stage("复制并校验目标")
                            result, detail = self._transfer_with_mount_state(
                                item, task, mount_key, source_mount_key,
                                stage_cb=report_stage, progress_cb=report_progress)
                        except Exception as exc:
                            logger.exception("迁移执行异常：task_id=%s queue_id=%s 文件=%s",
                                             item["task_id"], item["id"], item["rel_path"])
                            result, detail = "failed", f"迁移异常: {exc}"
                    if not (transient_source_io or item.get("_transient_io")):
                        break
                    # 挂载瞬时 I/O 故障（网盘限流的主要表现）：打开降级串行窗口，
                    # 短暂停顿后内联重试，把一次限流抖动就地吸收；只有内联重试
                    # 耗尽才落失败状态与 EIO 记录，避免刷爆失败/EIO 队列。
                    if failed_mount is None:
                        failed_mount = (source_mount_key
                                        if item.get("_eio_origin") == "source" else mount_key)
                    self._mount_report_failure(failed_mount)
                    if attempt < self.MOUNT_TRANSIENT_ATTEMPTS:
                        report_stage(f"挂载瞬时 I/O，{self.MOUNT_TRANSIENT_RETRY_DELAY}s 后重试")
                        self._stop.wait(self.MOUNT_TRANSIENT_RETRY_DELAY)
                        logger.warning("迁移遇挂载 I/O 故障，短暂停顿后内联重试（%d/%d）："
                                       "task_id=%s queue_id=%s 源=%s",
                                       attempt, self.MOUNT_TRANSIENT_ATTEMPTS - 1,
                                       task["id"], item["id"], item["src_path"])
                duration_ms = int((time.monotonic() - started) * 1000)
                report_stage("更新队列与记账")
                accounted = False
                if result == "success":
                    with self._stats_lock:
                        self.moved_today += 1
                        if attempt > 1:
                            self._mount_transient_absorbed += attempt - 1
                    if attempt > 1:
                        # 瞬时 I/O 被内联重试吸收：队列/日志页只显示成功，串行窗口
                        # 却会体现在迁移速度上；把吸收次数写进详情，避免「无报错之谜」。
                        detail += f"；挂载瞬时 I/O 内联重试 {attempt - 1} 次后成功"
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
                            accounted = True
                elif result == "conflict":
                    db.set_status(item["id"], "conflict")
                elif result == "failed":
                    if transient_source_io or item.get("_transient_io"):
                        # 源/目标挂载 EIO 不消耗迁移重试额度。普通任务需成功扫描后才
                        # 恢复写入；仅手动扫描的任务没有自动扫描来解除冷却，若同样
                        # 熔断会永久冻结直到人工再触发，因此只做 60s 限速重试探测，
                        # 由重试前的源 lstat 与传输校验充当挂载恢复的安全检查。
                        if task.get("manual_only"):
                            logger.warning("迁移遇挂载 I/O 故障，等待限速重试：task_id=%s queue_id=%s 源=%s",
                                           task["id"], item["id"], item["src_path"])
                        else:
                            logger.warning("迁移暂停并等待扫描恢复：task_id=%s queue_id=%s 源=%s",
                                           task["id"], item["id"], item["src_path"])
                            self._pause_task_for_scan(task["id"])
                        db.set_status(item["id"], "failed", next_retry_at=time.time() + 60)
                        detail += "；挂载 I/O 故障，60s 后限速探测恢复"
                        if item.get('_eio_origin'):
                            excluded = db.record_eio_failure(task['id'], item['id'],
                                                             item['rel_path'], item['_eio_origin'], detail)
                            if excluded:
                                detail += '；EIO 已自动加入精确排除项（源文件保留）'
                                logger.warning("EIO 文件已自动排除：task_id=%s 文件=%s 类型=%s",
                                               task['id'], item['rel_path'], item['_eio_origin'])
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
                # 只看本项的 accounted 标记：全局 last_accounting_error 会被并发传输互相覆盖，误标本条日志。
                log_result = "accounting_pending" if result == "success" and not accounted else result
                self._log(item, duration_ms, log_result, detail)
                db.cleanup_orphan_queue(item["id"])
            finally:
                finished.set()
                with self._stats_lock:
                    self._current_transfers.pop(item["id"], None)
                interval = max(0.0, float(task.get("interval_seconds", 5)))
                self._next_eligible[item["task_id"]] = time.monotonic() + interval
        finally:
            with self._stats_lock:
                self._current_transfers.pop(item["id"], None)
                remaining = self._busy_tasks.get(item["task_id"], 1) - 1
                if remaining > 0:
                    self._busy_tasks[item["task_id"]] = remaining
                else:
                    self._busy_tasks.pop(item["task_id"], None)


scheduler = Scheduler()
