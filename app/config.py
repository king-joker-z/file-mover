"""全局设置（config.json 持久化，热更新）"""
import json
import os
import threading

CONFIG_PATH = os.environ.get("FILEMOVER_CONFIG", "/app/config/settings.json")

DEFAULTS = {
    "scan_interval": 30,          # 全局默认扫描间隔（秒）
    "stable_check": True,         # 稳定性检测开关
    "stable_check_seconds": 2,    # 两次采样间隔
    "max_retries": 3,
    "retry_backoff_seconds": [10, 60, 300],
    "safe_mode": False,           # copy+校验+删源
    "verify_size": True,
    "remove_empty_dirs": True,
    "ignore_suffixes": [".tmp", ".part", ".partial", ".downloading", ".!ut", ".crdownload"],
    "default_interval_seconds": 5,
    "nfo_fix_enabled": True,   # 迁移前自动修正 nfo（actor 数字UID name→role 等）
    "douyin_nfo_enabled": False,  # 对无 nfo 的抖音视频自动生成简易 nfo
}

_lock = threading.Lock()
_cache = None


def _load() -> dict:
    global _cache
    if _cache is None:
        data = {}
        if os.path.exists(CONFIG_PATH):
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = {}
        cfg = dict(DEFAULTS)
        cfg.update({k: v for k, v in data.items() if k in DEFAULTS})
        _cache = cfg
    return _cache


def get() -> dict:
    with _lock:
        return dict(_load())


def update(data: dict) -> dict:
    global _cache
    with _lock:
        cfg = _load()
        cfg.update({k: v for k, v in data.items() if k in DEFAULTS})
        os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        _cache = cfg
        return dict(cfg)
