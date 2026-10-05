"""FastAPI 入口 + REST API"""
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import Optional

from . import db, config, scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    yield
    scheduler.stop()


app = FastAPI(title="File Mover", lifespan=lifespan)


# ---------- schemas ----------

class TaskIn(BaseModel):
    name: str
    src_dir: str
    dst_dir: str
    interval_seconds: float = Field(default=5, ge=0)
    scan_interval: int = Field(default=0, ge=0)  # 0=用全局
    include_patterns: str = ""
    exclude_patterns: str = ""
    conflict_policy: str = "skip"
    path_rule: str = "keep_structure"
    after_action: str = "delete"
    enabled: bool = True


class SettingsIn(BaseModel):
    scan_interval: Optional[int] = None
    stable_check: Optional[bool] = None
    stable_check_seconds: Optional[int] = None
    max_retries: Optional[int] = None
    safe_mode: Optional[bool] = None
    verify_size: Optional[bool] = None
    remove_empty_dirs: Optional[bool] = None
    default_interval_seconds: Optional[float] = None


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
    if data.conflict_policy not in ("skip", "overwrite", "rename"):
        raise HTTPException(400, "conflict_policy 非法")
    if data.path_rule not in ("keep_structure", "flatten", "by_date"):
        raise HTTPException(400, "path_rule 非法")
    if data.after_action not in ("delete", "keep") and not data.after_action.startswith("keep_days:"):
        raise HTTPException(400, "after_action 非法")
    return {"id": db.create_task(data.model_dump())}


@app.put("/api/tasks/{task_id}")
def api_update_task(task_id: int, data: TaskIn):
    if not db.get_task(task_id):
        raise HTTPException(404, "任务不存在")
    db.update_task(task_id, data.model_dump())
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


# ---------- logs ----------

@app.get("/api/logs")
def api_logs(task_id: Optional[int] = None, status: Optional[str] = None,
             limit: int = 100, offset: int = 0):
    return db.list_logs(task_id, status, limit, offset)


# ---------- settings ----------

@app.get("/api/settings")
def api_get_settings():
    return config.get()


@app.put("/api/settings")
def api_update_settings(data: SettingsIn):
    data_dict = {k: v for k, v in data.model_dump().items() if v is not None}
    return config.update(data_dict)


# ---------- 前端静态托管 ----------

WEB_DIST = os.environ.get("FILEMOVER_WEB", "/app/web/dist")

if os.path.isdir(WEB_DIST):
    app.mount("/assets", StaticFiles(directory=os.path.join(WEB_DIST, "assets")), name="assets")

    @app.get("/{path:path}")
    async def spa(path: str):
        full = os.path.join(WEB_DIST, path)
        if path and os.path.isfile(full):
            return FileResponse(full)
        return FileResponse(os.path.join(WEB_DIST, "index.html"))
