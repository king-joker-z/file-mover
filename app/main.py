"""FastAPI 入口 + REST API"""
import hmac
import ipaddress
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal, Optional
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, field_validator

from . import db, config
from .scheduler import scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    config.get()  # 损坏/非法配置必须在启动后台线程之前显式失败
    scheduler.start()
    yield
    scheduler.stop()


app = FastAPI(title="File Mover", lifespan=lifespan)


# ---------- schemas ----------

class TaskIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    src_dir: str
    dst_dir: str
    interval_seconds: float = Field(default=5, ge=0, allow_inf_nan=False)
    scan_interval: int = Field(default=0, ge=0)  # 0=用全局
    include_patterns: str = ""
    exclude_patterns: str = ""
    conflict_policy: Literal["skip", "overwrite", "rename"] = "skip"
    path_rule: Literal["keep_structure", "flatten", "by_date"] = "keep_structure"
    after_action: str = "delete"
    enabled: bool = True
    # 运行周期：空 = 全天运行；否则窗口内运行，如 "01:00-07:00"，可多段逗号分隔
    run_windows: str = ""
    symlink_enabled: bool = False
    filename_nfo_enabled: bool = False

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
    max_retries: Optional[int] = Field(default=None, ge=0, le=100)
    safe_mode: Optional[bool] = None
    verify_size: Optional[bool] = None
    remove_empty_dirs: Optional[bool] = None
    default_interval_seconds: Optional[float] = Field(default=None, ge=0, le=86400, allow_inf_nan=False)
    nfo_fix_enabled: Optional[bool] = None
    douyin_nfo_enabled: Optional[bool] = None
    re_download_action: Optional[Literal["delete", "skip", "keep"]] = None
    symlink_enabled: Optional[bool] = None


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
            if not supplied.startswith("Bearer ") or not hmac.compare_digest(supplied[7:], token):
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
    """文件级浏览（含软链识别）：列出目录下所有条目并标注类型。
    用于 Web UI 检查软链是否生成，无需 SSH。"""
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
    if not target.resolve().is_relative_to(root):
        raise HTTPException(400, "路径非法")
    if not target.is_dir():
        raise HTTPException(404, "目录不存在")
    items = []
    for p in sorted(target.iterdir(), key=lambda x: x.name):
        is_link = p.is_symlink()
        try:
            size = 0 if is_link else (p.stat().st_size if p.exists() else 0)
            t = "symlink" if is_link else ("dir" if p.is_dir() else "file")
            link_target = os.readlink(p) if is_link else ""
        except OSError:
            t, size, link_target = "file", 0, ""
        items.append({"name": p.name, "type": t, "size": size, "target": link_target})
    return {"rel": rel, "path": str(target), "items": items[:limit]}


# ---------- dashboard ----------

@app.get("/api/dashboard")
def dashboard():
    return scheduler.stats()


# ---------- tasks ----------

@app.get("/api/tasks")
def api_list_tasks():
    return db.list_tasks()


@app.post("/api/tasks")
def api_create_task(data: TaskIn):
    _validate_task(data)
    task_id = db.create_task(data.model_dump())
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
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
    scheduler.wake_scan()
    return {"ok": True}


@app.delete("/api/tasks/{task_id}")
def api_delete_task(task_id: int):
    db.delete_task(task_id)
    return {"ok": True}


@app.post("/api/tasks/{task_id}/toggle")
def api_toggle_task(task_id: int):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    db.update_task(task_id, {"enabled": 0 if task["enabled"] else 1})
    scheduler.wake_scan()
    return {"ok": True, "enabled": not task["enabled"]}


@app.get("/api/tasks/{task_id}/sync-progress")
def api_sync_progress(task_id: int):
    """轮询立即同步的扫描进度"""
    return scheduler.sync_progress.get(task_id, {"running": False, "done": True})


@app.post("/api/tasks/{task_id}/sync-now")
def api_sync_now(task_id: int):
    """立即全量扫描指定任务：跳过周期调度，立刻扫描源目录。
    软链自动跳过，非软链文件正常入队迁移（含重复下载检测）。返回扫描统计。"""
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    if not task.get("enabled"):
        raise HTTPException(400, "任务已停用，请先启用")
    if not scheduler._task_paths_safe(task):
        raise HTTPException(400, "任务路径不安全，已拒绝扫描")
    result = scheduler.sync_now(task_id)
    return {"ok": True, "message": result.get("message", "全量扫描已触发")}


# ---------- queue ----------

@app.get("/api/queue")
def api_queue(status: Optional[str] = None, limit: int = 200, offset: int = 0):
    return db.list_queue(status, limit, offset)


@app.post("/api/queue/{queue_id}/retry")
def api_retry(queue_id: int):
    db.retry_queue([queue_id])
    return {"ok": True}


@app.post("/api/queue/retry-batch")
def api_retry_batch(ids: list[int]):
    db.retry_queue(ids)
    return {"ok": True}


@app.delete("/api/queue")
def api_clear_queue(status: Optional[str] = None):
    """清空队列。默认只清 done；传 status=failed/conflict/all 按需清除"""
    n = db.clear_queue(status)
    return {"ok": True, "removed": n}


# ---------- logs ----------

@app.get("/api/logs")
def api_logs(task_id: Optional[int] = None, status: Optional[str] = None,
             limit: int = 100, offset: int = 0):
    return db.list_logs(task_id, status, limit, offset)


@app.delete("/api/logs")
def api_clear_logs():
    n = db.clear_logs()
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
    return config.update(data_dict)


# ---------- 前端静态托管 ----------

WEB_DIST = os.environ.get("FILEMOVER_WEB", "/app/web/dist")

if os.path.isdir(WEB_DIST):
    assets_dir = os.path.join(WEB_DIST, "assets")
    if os.path.isdir(assets_dir):
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    @app.get("/{path:path}")
    async def spa(path: str):
        full = os.path.join(WEB_DIST, path)
        if path and os.path.isfile(full) and os.path.commonpath([os.path.abspath(full), os.path.abspath(WEB_DIST)]) == os.path.abspath(WEB_DIST):
            return FileResponse(full)
        return FileResponse(os.path.join(WEB_DIST, "index.html"))
