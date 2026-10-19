"""FastAPI 入口 + REST API"""
import hmac
import ipaddress
import logging
import os
import stat
from contextlib import asynccontextmanager
from time import monotonic
from uuid import UUID
from pathlib import Path
from typing import Literal, Optional
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, field_validator

from . import db, config
from .scheduler import scheduler

logger = logging.getLogger("uvicorn.error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    config.get()  # 损坏/非法配置必须在启动后台线程之前显式失败
    scheduler.start()
    yield
    scheduler.stop()


app = FastAPI(title="File Mover", lifespan=lifespan)


@app.get("/healthz", include_in_schema=False)
async def healthz():
    """仅供容器探针确认 HTTP 服务可响应，不访问配置或数据库。"""
    return {"status": "ok"}


@app.get("/readyz", include_in_schema=False)
def readyz():
    """公开最少就绪状态；详细错误只留在受保护的 dashboard 中。"""
    scanner_alive = any(t.name == "fm-scanner" and t.is_alive() for t in scheduler._threads)
    worker_alive = any(t.name == "fm-worker" and t.is_alive() for t in scheduler._threads)
    try:
        config.get()
    except Exception:
        return JSONResponse({"status": "not_ready", "reason": "config_unavailable"}, status_code=503)
    try:
        db.queue_stats()
    except Exception:
        return JSONResponse({"status": "not_ready", "reason": "database_unavailable"}, status_code=503)
    if not scanner_alive:
        return JSONResponse({"status": "not_ready", "reason": "scanner_unavailable"}, status_code=503)
    if not worker_alive:
        return JSONResponse({"status": "not_ready", "reason": "worker_unavailable"}, status_code=503)
    return {"status": "ok"}


# ---------- schemas ----------

class TaskIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    src_dir: str
    dst_dir: str
    interval_seconds: float = Field(default=5, ge=0, le=86400, allow_inf_nan=False)
    scan_interval: int = Field(default=0, ge=0, le=86400)  # 0=用全局
    include_patterns: str = ""
    exclude_patterns: str = ""
    conflict_policy: Literal["skip", "overwrite", "rename"] = "skip"
    path_rule: Literal["keep_structure", "flatten", "by_date"] = "keep_structure"
    after_action: str = "delete"
    enabled: bool = True
    # 运行周期：空 = 全天运行；否则窗口内运行，如 "01:00-07:00"，可多段逗号分隔
    run_windows: str = ""
    filename_nfo_enabled: bool = False
    # 仅手动扫描：开启后任务不参与自动定时扫描，只能通过「立即扫描」手动触发
    manual_only: bool = False
    # 异步迁移：开启后该任务的文件与其它任务并行迁移，适合超大文件不堵塞其它任务
    async_transfer: bool = False
    # 任务内并发迁移数：同一任务同时迁移的文件数（需开启异步迁移），
    # 大量中小文件或网盘单连接限速时，2-4 并发可显著提升总吞吐
    transfer_workers: int = Field(default=1, ge=1, le=4)
    # 校验强度：strict=多轮读回+删除前暂存（最稳）；fast=单次读回无暂存（大文件推荐）；
    # off=不读回仅核对大小（最快，信任网盘写入）。NFO 小文件始终按 strict 处理。
    verify_mode: Literal["strict", "fast", "off"] = "strict"

    @field_validator("run_windows")
    @classmethod
    def validate_run_windows(cls, value: str) -> str:
        if not value.strip():
            return ""
        for segment in value.split(","):
            parts = segment.strip().split("-")
            if len(parts) != 2:
                raise ValueError("运行窗口必须为 HH:MM-HH:MM")
            try:
                if any(len(part.strip().split(":")) != 2 for part in parts):
                    raise ValueError("运行窗口必须为 HH:MM-HH:MM")
                start_hour, start_minute = (int(n) for n in parts[0].strip().split(":"))
                end_hour, end_minute = (int(n) for n in parts[1].strip().split(":"))
            except ValueError as exc:
                raise ValueError("运行窗口必须为 HH:MM-HH:MM") from exc
            if not (0 <= start_hour <= 23 and 0 <= start_minute <= 59
                    and 0 <= end_minute <= 59 and (0 <= end_hour <= 23
                    or (end_hour == 24 and end_minute == 0))
                    and (start_hour, start_minute) != (end_hour, end_minute)):
                raise ValueError("运行窗口时间非法")
        return value


class SettingsIn(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    scan_interval: Optional[int] = Field(default=None, ge=1, le=86400)
    stable_check: Optional[bool] = None
    stable_check_seconds: Optional[int] = Field(default=None, ge=0, le=3600)
    stable_quiet_seconds: Optional[int] = Field(default=None, ge=0, le=86400)
    max_retries: Optional[int] = Field(default=None, ge=0, le=100)
    # 兼容旧客户端的设置字段；迁移流程始终复制并进行 hash/读回校验。
    safe_mode: Optional[bool] = None
    verify_size: Optional[bool] = None
    remove_empty_dirs: Optional[bool] = None
    default_interval_seconds: Optional[float] = Field(default=None, ge=0, le=86400, allow_inf_nan=False)
    nfo_fix_enabled: Optional[bool] = None
    douyin_nfo_enabled: Optional[bool] = None
    # 异步迁移线程数：开启异步的任务可与其它任务并行迁移的最大线程数
    max_async_transfers: Optional[int] = Field(default=None, ge=1, le=8)


class ExcludePatternIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pattern: str = Field(min_length=1, max_length=512)


class EioRetryIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: int = Field(ge=1)
    rel_path: str = Field(min_length=1, max_length=1024)


def _split_exclude_patterns(raw: str) -> list[str]:
    return [p.strip() for p in (raw or "").split(",") if p.strip()]


def _validate_exclude_pattern(value: str) -> str:
    """单条规则校验：逗号是规则分隔符，换行/空字符一律拒绝。"""
    pattern = value.strip()
    if not pattern:
        raise HTTPException(422, "排除规则不能为空")
    if any(ch in pattern for ch in (",", "\n", "\r", "\x00")):
        raise HTTPException(422, "排除规则不能包含逗号或换行，请逐条添加")
    if len(pattern) > 512:
        raise HTTPException(422, "排除规则过长（上限 512 字符）")
    return pattern


def _task_path(value: str, field: str) -> Path:
    """归一化路径并解析现有软链；不存在的尾部仍可校验包含关系。"""
    if not value or not os.path.isabs(value) or "\x00" in value:
        raise ValueError(f"{field} 必须是绝对路径")
    path = Path(value).resolve(strict=False)
    if path == Path("/"):
        raise ValueError(f"{field} 不能是文件系统根目录")
    return path


def _overlaps(a: Path, b: Path) -> bool:
    return a == b or a in b.parents or b in a.parents


def validate_task_paths(task: dict, other_tasks: list[dict] = None, task_id: int = None) -> None:
    """纯校验：包括自身嵌套及跨任务读写重叠，供 API/调度器复用。"""
    src = _task_path(task["src_dir"], "src_dir")
    dst = _task_path(task["dst_dir"], "dst_dir")
    if _overlaps(src, dst):
        raise ValueError("源目录与目标目录不能相同或互相包含")
    for other in other_tasks or []:
        if other.get("id") == task_id:
            continue
        other_src = _task_path(other["src_dir"], "已有任务 src_dir")
        other_dst = _task_path(other["dst_dir"], "已有任务 dst_dir")
        if any(_overlaps(a, b) for a in (src, dst) for b in (other_src, other_dst)):
            raise ValueError(f"目录与已有任务 {other.get('id', '')} 重叠，可能导致重复迁移或循环")


def _validate_task(data: TaskIn, task_id: int = None) -> None:
    if data.after_action not in ("delete", "keep"):
        raise HTTPException(400, "after_action 非法")
    try:
        validate_task_paths(data.model_dump(), db.list_tasks(), task_id)
    except (ValueError, RuntimeError, OSError) as exc:
        raise HTTPException(400, str(exc)) from exc


def _local_request(request: Request) -> bool:
    """不信任代理地址；代理必须设置 FILEMOVER_API_TOKEN。"""
    host = request.headers.get("host", "")
    try:
        hostname = urlsplit("//" + host).hostname
        local_host = hostname == "localhost" or ipaddress.ip_address(hostname).is_loopback
        local_client = ipaddress.ip_address(request.client.host).is_loopback
    except (ValueError, TypeError, AttributeError):
        return False
    if not local_host or not local_client:
        return False
    # 反向代理即使抹去 Forwarded 头，也不能借 loopback 客户端身份绕过鉴权。
    # 无 Token 时只开放显式绑定在回环地址的本机服务。
    bind_host = os.environ.get("FILEMOVER_BIND_HOST", "")
    if bind_host not in ("127.0.0.1", "::1", "localhost"):
        return False
    if any(h in request.headers for h in ("forwarded", "x-forwarded-for", "x-real-ip", "x-forwarded-host")):
        return False
    origin = request.headers.get("origin")
    if origin:
        parsed = urlsplit(origin)
        if parsed.scheme not in ("http", "https") or parsed.netloc.lower() != host.lower():
            return False
    return True


@app.middleware("http")
async def authorize_api(request: Request, call_next):
    if request.url.path == "/api" or request.url.path.startswith("/api/"):
        token = os.environ.get("FILEMOVER_API_TOKEN")
        if token:
            supplied = request.headers.get("authorization", "")
            if not supplied.startswith("Bearer ") or not hmac.compare_digest(
                    supplied[7:].encode("utf-8"), token.encode("utf-8")):
                return JSONResponse({"detail": "需要有效的 API Token"}, status_code=401,
                                    headers={"WWW-Authenticate": "Bearer"})
        elif not _local_request(request):
            return JSONResponse({"detail": "远程访问 API 必须配置 FILEMOVER_API_TOKEN"}, status_code=403)
    return await call_next(request)


# ---------- 目录浏览（供任务表单下拉选择） ----------

# 挂载根目录：容器内即 /（compose 中 /data/local、/data/cloud 等都挂在这里）
MOUNT_ROOT = os.environ.get("FILEMOVER_MOUNT_ROOT", "/")

@app.get("/api/dirs")
def api_dirs(rel: Optional[str] = None):
    """列出浏览根下指定相对路径的子目录。
    rel 为空 = 浏览根；否则是相对浏览根的相对路径（如 "local/子目录"）。
    返回 {rel: 相对路径, path: 绝对路径(仅展示), dirs: [子目录名], root: 浏览根}"""
    import pathlib
    root = pathlib.Path(MOUNT_ROOT).resolve()
    if str(root) == "/":
        data_dir = pathlib.Path("/data")
        if data_dir.is_dir():
            root = data_dir.resolve()  # 浏览根=/data，rel 顶层即 local/cloud
    if rel:
        rel = rel.strip("/")
        if rel in ("", "."):
            rel = ""
        # 防穿越：分段校验，不允许 ".." 和绝对片段
        parts = [p for p in rel.split("/") if p]
        if any(p in ("..", "") for p in parts):
            raise HTTPException(400, "路径非法")
        target = root.joinpath(*parts)
    else:
        target = root
        rel = ""
    if not target.resolve().is_relative_to(root):
        raise HTTPException(400, "路径非法")
    if not target.is_dir():
        raise HTTPException(404, "目录不存在")
    dirs = sorted([p.name for p in target.iterdir()
                   if p.is_dir() and not p.name.startswith(".")])
    return {"rel": rel, "path": str(target), "dirs": dirs, "root": str(root)}


@app.get("/api/browse")
def api_browse(rel: Optional[str] = None, limit: int = 500):
    """文件级浏览：列出目录下条目并标注类型。"""
    if not 1 <= limit <= 1000:
        raise HTTPException(422, "limit 必须在 1-1000")
    import pathlib
    root = pathlib.Path(MOUNT_ROOT).resolve()
    if str(root) == "/":
        data_dir = pathlib.Path("/data")
        if data_dir.is_dir():
            root = data_dir.resolve()
    if rel:
        rel = rel.strip("/")
        parts = [p for p in rel.split("/") if p]
        if any(p in ("..", "") for p in parts):
            raise HTTPException(400, "路径非法")
        target = root.joinpath(*parts) if parts else root
    else:
        target = root
        rel = ""
    try:
        if not target.resolve().is_relative_to(root):
            raise HTTPException(400, "路径非法")
        info = target.stat()
        if not stat.S_ISDIR(info.st_mode):
            raise HTTPException(404, "不是目录")
        # 一次性枚举，避免目录读取失败被误当作空目录。
        children = sorted(target.iterdir(), key=lambda x: x.name)
    except FileNotFoundError as exc:
        raise HTTPException(404, f"目录不存在: {target}") from exc
    except OSError as exc:
        raise HTTPException(503, f"读取目录失败: {target} ({exc})") from exc
    items = []
    for p in children[:limit]:
        try:
            info = p.lstat()
            t = "dir" if stat.S_ISDIR(info.st_mode) and not p.is_symlink() else (
                "file" if stat.S_ISREG(info.st_mode) and not p.is_symlink() else "other")
            size = info.st_size if t == "file" else 0
        except OSError:
            t, size = "other", 0
        items.append({"name": p.name, "type": t, "size": size})
    return {"rel": rel, "path": str(target), "root": str(root), "items": items}


# ---------- dashboard ----------

@app.get("/api/dashboard")
def dashboard():
    return scheduler.stats()


# ---------- upload rounds ----------

@app.get("/api/rounds")
def api_upload_rounds(task_id: Optional[int] = None, limit: int = 50, offset: int = 0):
    if not 1 <= limit <= 200 or offset < 0 or (task_id is not None and task_id < 1):
        raise HTTPException(422, "limit 必须在 1-200，offset 不得为负数，task_id 必须为正整数")
    return {"items": db.list_upload_rounds(task_id, limit, offset),
            "total": db.count_upload_rounds(task_id)}


@app.get("/api/rounds/{round_id}/files")
def api_round_files(round_id: int, status: Optional[str] = None,
                    limit: int = 50, offset: int = 0):
    """轮次文件明细：逐文件展示成功/失败/冲突/取消/待处理等状态。"""
    if not 1 <= limit <= 200 or offset < 0:
        raise HTTPException(422, "limit 必须在 1-200，offset 不得为负数")
    round_info = db.get_round(round_id)
    if not round_info:
        raise HTTPException(404, "轮次不存在")
    return {"round": round_info,
            "items": db.list_round_files(round_id, status, limit, offset),
            "total": db.count_round_files(round_id, status)}


# ---------- EIO failures ----------

@app.get("/api/eio-failures")
def api_eio_failures(task_id: Optional[int] = None, limit: int = 200, offset: int = 0):
    if not 1 <= limit <= 500 or offset < 0 or (task_id is not None and task_id < 1):
        raise HTTPException(422, "limit 必须在 1-500，offset 不得为负数，task_id 必须为正整数")
    return {"items": db.list_eio_failures(task_id, limit, offset),
            "total": db.count_eio_failures(task_id)}


@app.post("/api/eio-failures/retry")
def api_retry_eio_failure(data: EioRetryIn):
    """一键重试：自动移除该条自动排除规则并立即重新扫描源目录。"""
    task = db.get_task(data.task_id)
    if not task:
        raise HTTPException(404, "任务已删除，无法重试")
    if not task.get("enabled"):
        raise HTTPException(400, "任务已停用，请先启用后再重试")
    if db.get_eio_failure(data.task_id, data.rel_path) is None:
        raise HTTPException(404, "故障记录不存在")
    exact = "./" + data.rel_path
    patterns = _split_exclude_patterns(task.get("exclude_patterns"))
    removed_rule = exact in patterns
    if removed_rule:
        try:
            db.update_exclude_patterns(data.task_id, [p for p in patterns if p != exact])
        except (ValueError, RuntimeError, OSError) as exc:
            raise HTTPException(409, str(exc)) from exc
    result = scheduler.sync_now(data.task_id)
    if not result.get("ok"):
        raise HTTPException(503, result.get("message", "重新扫描触发失败"))
    logger.info("EIO 故障重试：task_id=%s 路径=%s 移除排除=%s",
                data.task_id, data.rel_path, removed_rule)
    return {"ok": True, "removed_rule": removed_rule,
            "message": ("已移除排除项并" if removed_rule else "已") + "触发重新扫描"}


# ---------- tasks ----------

@app.get("/api/tasks")
def api_list_tasks():
    return db.list_tasks()


@app.post("/api/tasks")
def api_create_task(data: TaskIn):
    _validate_task(data)
    try:
        task_id = db.create_task(data.model_dump())
    except (ValueError, RuntimeError, OSError) as exc:
        raise HTTPException(409, str(exc)) from exc
    logger.info("任务创建：task_id=%s 名称=%s 源=%s 目标=%s", task_id, data.name,
                data.src_dir, data.dst_dir)
    scheduler.wake_scan()
    return {"id": task_id}


@app.put("/api/tasks/{task_id}")
def api_update_task(task_id: int, data: TaskIn):
    existing = db.get_task(task_id)
    if not existing:
        raise HTTPException(404, "任务不存在")
    _validate_task(data, task_id)
    payload = data.model_dump()
    if "enabled" not in data.model_fields_set:
        payload.pop("enabled")  # 编辑表单不提交启停状态，必须经 toggle 显式切换
    try:
        db.update_task(task_id, payload)
    except (ValueError, RuntimeError, OSError) as exc:
        raise HTTPException(409, str(exc)) from exc
    logger.info("任务更新：task_id=%s 名称=%s 源=%s 目标=%s", task_id, data.name,
                data.src_dir, data.dst_dir)
    scheduler.wake_scan()
    return {"ok": True}


@app.delete("/api/tasks/{task_id}")
def api_delete_task(task_id: int):
    try:
        db.delete_task(task_id)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
    logger.info("任务删除：task_id=%s", task_id)
    return {"ok": True}


@app.post("/api/tasks/{task_id}/toggle")
def api_toggle_task(task_id: int):
    enabled = db.toggle_task(task_id)
    if enabled is None:
        raise HTTPException(404, "任务不存在")
    logger.info("任务启停变更：task_id=%s 启用=%s", task_id, enabled)
    scheduler.wake_scan()
    return {"ok": True, "enabled": enabled}


@app.post("/api/tasks/{task_id}/exclude-patterns")
def api_add_exclude_pattern(task_id: int, data: ExcludePatternIn):
    """追加一条排除规则；队列联动与编辑任务保存一致。"""
    pattern = _validate_exclude_pattern(data.pattern)
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    patterns = _split_exclude_patterns(task.get("exclude_patterns"))
    if pattern in patterns:
        return {"ok": True, "duplicate": True, "patterns": patterns}
    try:
        db.update_exclude_patterns(task_id, [*patterns, pattern])
    except (ValueError, RuntimeError, OSError) as exc:
        raise HTTPException(409, str(exc)) from exc
    logger.info("排除规则新增：task_id=%s 规则=%s", task_id, pattern)
    scheduler.wake_scan()
    return {"ok": True, "duplicate": False, "patterns": [*patterns, pattern]}


@app.delete("/api/tasks/{task_id}/exclude-patterns")
def api_remove_exclude_pattern(task_id: int, pattern: str):
    """移除单条排除规则（精确匹配整条规则，不做通配展开）。"""
    pattern = _validate_exclude_pattern(pattern)
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    patterns = _split_exclude_patterns(task.get("exclude_patterns"))
    if pattern not in patterns:
        return {"ok": True, "removed": False, "patterns": patterns}
    try:
        db.update_exclude_patterns(task_id, [p for p in patterns if p != pattern])
    except (ValueError, RuntimeError, OSError) as exc:
        raise HTTPException(409, str(exc)) from exc
    logger.info("排除规则移除：task_id=%s 规则=%s", task_id, pattern)
    scheduler.wake_scan()
    return {"ok": True, "removed": True, "patterns": [p for p in patterns if p != pattern]}


@app.get("/api/tasks/{task_id}/sync-progress")
def api_sync_progress(task_id: int):
    """轮询立即同步的扫描进度；没有扫描记录时不报告为成功。"""
    return scheduler.sync_progress.get(task_id, {"running": False, "done": False})


@app.post("/api/tasks/{task_id}/sync-now")
def api_sync_now(task_id: int, request_id: UUID = None, backfill_nfo: bool = False):
    """立即扫描源目录；仅显式请求时附带历史目标 NFO 巡检。"""
    started = monotonic()
    logger.info("立即同步请求：task_id=%s", task_id)
    task = db.get_task(task_id)
    if not task:
        logger.warning("立即同步请求拒绝：task_id=%s 任务不存在", task_id)
        raise HTTPException(404, "任务不存在")
    if not task.get("enabled"):
        logger.warning("立即同步请求拒绝：task_id=%s 任务已停用", task_id)
        raise HTTPException(400, "任务已停用，请先启用")
    # 路径校验可能访问响应缓慢的挂载目录，交由扫描线程执行并通过进度报告错误。
    request_key = str(request_id) if request_id else None
    result = (scheduler.sync_now(task_id, request_key, backfill_nfo=True) if backfill_nfo
              else scheduler.sync_now(task_id, request_key))
    if not result["ok"]:
        raise HTTPException(503, result["message"])
    logger.info("立即同步请求已受理：task_id=%s 耗时=%.1fs 状态=%s",
                task_id, monotonic() - started, result.get("message"))
    return {"ok": True, "message": result.get("message", "源目录扫描已触发"),
            "request_id": result.get("request_id")}


# ---------- queue ----------

@app.get("/api/queue")
def api_queue(status: Optional[str] = None, limit: int = 200,
              offset: int = 0, search: Optional[str] = None,
              with_total: bool = False):
    if not 1 <= limit <= 1000 or offset < 0:
        raise HTTPException(422, "limit 必须在 1-1000，offset 不得为负数")
    if search is not None and len(search) > 256:
        raise HTTPException(422, "搜索词不能超过 256 字符")
    search = search.strip() if search else None
    items = db.list_queue(status, limit, offset, search=search)
    if with_total:
        return {"items": items, "total": db.count_queue(status, search)}
    return items


@app.post("/api/queue/{queue_id}/retry")
def api_retry(queue_id: int):
    db.retry_queue([queue_id])
    logger.info("手动重试队列：queue_id=%s", queue_id)
    return {"ok": True}


@app.post("/api/queue/retry-batch")
def api_retry_batch(ids: list[int]):
    db.retry_queue(ids)
    logger.info("批量重试队列：数量=%d", len(ids))
    return {"ok": True}


@app.post("/api/queue/retry-failed")
def api_retry_failed():
    # 单事务处理所有失败项，不使用变化中的 offset 分页。
    count = db.retry_failed_batch()
    logger.info("重试全部失败队列：数量=%d", count)
    scheduler.wake_scan()
    return {"ok": True, "retried": count}


@app.delete("/api/queue")
def api_clear_queue(status: Optional[str] = None):
    """清空队列。默认只清 done；传 status=failed/conflict/all 按需清除"""
    n = db.clear_queue(status)
    logger.warning("清空队列：状态=%s 删除=%d", status, n)
    return {"ok": True, "removed": n}


# ---------- logs ----------

@app.get("/api/logs")
def api_logs(task_id: Optional[int] = None, status: Optional[str] = None,
             limit: int = 100, offset: int = 0, search: Optional[str] = None,
             with_total: bool = False):
    if not 1 <= limit <= 1000 or offset < 0:
        raise HTTPException(422, "limit 必须在 1-1000，offset 不得为负数")
    if search is not None and len(search) > 256:
        raise HTTPException(422, "搜索词不能超过 256 字符")
    search = search.strip() if search else None
    items = db.list_logs(task_id, status, limit, offset, search=search)
    if with_total:
        return {"items": items, "total": db.count_logs(task_id, status, search)}
    return items


@app.delete("/api/logs")
def api_clear_logs():
    n = db.clear_logs()
    logger.warning("清空操作日志：删除=%d", n)
    return {"ok": True, "removed": n}


# ---------- settings ----------

@app.get("/api/settings")
def api_get_settings():
    return config.get()


@app.put("/api/settings")
def api_update_settings(data: SettingsIn):
    data_dict = data.model_dump(exclude_unset=True)
    if any(value is None for value in data_dict.values()):
        raise HTTPException(422, "设置项不可为 null")
    result = config.update(data_dict)
    logger.info("设置更新：字段=%s", sorted(data_dict))
    return result


# ---------- 前端静态托管 ----------

WEB_DIST = os.environ.get("FILEMOVER_WEB", "/app/web/dist")

class NoCacheStaticFiles(StaticFiles):
    """静态资源使用协商缓存：文件更新后浏览器立即拿到新版本，未变则 304 复用。"""

    def file_response(self, *args, **kwargs):
        resp = super().file_response(*args, **kwargs)
        resp.headers["Cache-Control"] = "no-cache"
        return resp

if os.path.isdir(WEB_DIST):
    assets_dir = os.path.join(WEB_DIST, "assets")
    if os.path.isdir(assets_dir):
        app.mount("/assets", NoCacheStaticFiles(directory=assets_dir), name="assets")

    @app.get("/{path:path}")
    async def spa(path: str):
        full = os.path.join(WEB_DIST, path)
        if path and os.path.isfile(full) and os.path.commonpath([os.path.abspath(full), os.path.abspath(WEB_DIST)]) == os.path.abspath(WEB_DIST):
            return FileResponse(full, headers={"Cache-Control": "no-cache"})
        return FileResponse(os.path.join(WEB_DIST, "index.html"), headers={"Cache-Control": "no-cache"})
