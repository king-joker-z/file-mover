"""抖音视频 nfo 生成器
优先级：同目录 meta.json（Douyin_TikTok_Download_API v5 产物）> DouK 文件名解析 > 简易兜底
"""
import os
import json
import re
from typing import Optional
from datetime import datetime

# 抖音作品 ID：10~20 位纯数字
_ID_RE = re.compile(r"\b(\d{10,20})\b")
# 日期时间: 2026-05-11 12-30-45 / 2026-05-11 / 20260511 等
_DATE_RE = re.compile(r"(\d{4}[-/年]\d{1,2}[-/月]\d{1,2}(?:[ 日时]*\d{1,2}[:\-时]\d{1,2}(?:[:\-]\d{1,2})?)?|\d{8})")
# 常见分隔符（DouK split 默认 "-"，也容忍空格/下划线）
_SPLIT_RE = re.compile(r"\s*[-—_ ]\s*")

VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".flv", ".webm"}


def _meta_from_metajson(video_path: str) -> Optional[dict]:
    """从同目录 meta.json（Douyin_TikTok_Download_API v5 下载产物）读取元数据。
    meta.json 字段: platform, post_id, kind, web_url, title, description,
    publish_time, duration, author_uid, author_nickname, tags, ...
    """
    meta_path = os.path.join(os.path.dirname(video_path), "meta.json")
    if not os.path.isfile(meta_path):
        return None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            mj = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(mj, dict):
        return None
    # 兼容嵌套结构（顶层可能包 {post: {...}, author: {...}}）
    post = mj.get("post") if isinstance(mj.get("post"), dict) else mj
    author = mj.get("author") if isinstance(mj.get("author"), dict) else {}

    post_id = str(post.get("post_id") or post.get("id") or mj.get("post_id") or "") or None
    title = post.get("title") or post.get("description") or ""
    desc = post.get("description") or title
    pub = post.get("publish_time") or ""
    # publish_time 可能是 iso 字符串或时间戳
    date_str = None
    if pub:
        try:
            if isinstance(pub, (int, float)):
                date_str = datetime.fromtimestamp(pub).strftime("%Y-%m-%d")
            else:
                s = str(pub).replace("T", " ").split(".")[0].split("+")[0]
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                    try:
                        date_str = datetime.strptime(s.strip(), fmt).strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue
        except (OSError, ValueError, OverflowError):
            pass
    nickname = (author.get("nickname") or author.get("name")
                or mj.get("author_nickname") or "")
    return {
        "aweme_id": post_id,
        "date": date_str,
        "desc": desc or None,
        "nickname": nickname or None,
        "title": title or None,
        "source": "meta.json",
    }


def parse_filename(filename: str, parent_dir: Optional[str] = None) -> Optional[dict]:
    """从文件名解析元数据。返回 None 表示解析失败（交给简易模式）"""
    stem = os.path.splitext(filename)[0]
    # 作品 ID：取最长的纯数字串
    ids = _ID_RE.findall(stem)
    aweme_id = max(ids, key=len) if ids else None

    # 发布时间
    date_str = None
    dm = _DATE_RE.search(stem)
    if dm:
        raw = dm.group(1)
        for fmt in ("%Y-%m-%d %H-%M-%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                    "%Y/%m/%d", "%Y年%m月%d日", "%Y%m%d"):
            try:
                dt = datetime.strptime(raw.strip(), fmt)
                date_str = dt.strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

    # 描述: 去掉 ID 与日期后的剩余片段
    desc_part = stem
    if aweme_id:
        desc_part = desc_part.replace(aweme_id, " ")
    if dm:
        desc_part = desc_part.replace(dm.group(1), " ")
    parts = [p.strip() for p in _SPLIT_RE.split(desc_part) if p.strip() and len(p.strip()) >= 2]
    # 过滤纯数字残片
    parts = [p for p in parts if not p.isdigit()]
    desc = " ".join(parts) if parts else None

    # 昵称: 优先父目录名（folder_mode/按博主归档），其次文件名片段
    nickname = parent_dir if parent_dir else None

    return {
        "aweme_id": aweme_id,
        "date": date_str,
        "desc": desc,
        "nickname": nickname,
        "title": desc or (os.path.splitext(filename)[0]),
    }


def build_nfo(meta: dict, nickname_fallback: str = "未知博主") -> str:
    """生成简易 movie nfo（与用户样例结构一致）"""
    title = meta.get("title") or "未知标题"
    plot = meta.get("desc") or title
    outline = plot
    year = (meta.get("date") or "")[:4] or ""
    premiered = meta.get("date") or ""
    nickname = meta.get("nickname") or nickname_fallback
    uid = meta.get("aweme_id")

    esc = lambda x: (x or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<movie>",
             f"  <title>{esc(title)}</title>",
             f"  <plot>{esc(plot)}</plot>",
             f"  <outline>{esc(outline)}</outline>"]
    if year:
        lines.append(f"  <year>{esc(year)}</year>")
    if premiered:
        lines.append(f"  <premiered>{esc(premiered)}</premiered>")
    lines.append(f"  <studio>{esc(nickname)}</studio>")
    lines.append("  <runtime>1</runtime>")
    if uid:
        lines.append(f'  <uniqueid type="douyin" default="true">{esc(uid)}</uniqueid>')
        lines.append(f"  <website>https://www.douyin.com/video/{esc(uid)}</website>")
    lines.append("  <actor>")
    lines.append(f"    <name>{esc(nickname)}</name>")
    lines.append(f"    <role>{esc(nickname)}</role>")
    lines.append("  </actor>")
    lines.append("</movie>")
    return "\n".join(lines) + "\n"


def ensure_nfo(video_path: str, name_format_hint: str = "create_time uid id") -> tuple[bool, str]:
    """若视频无伴生 nfo 则生成。优先 meta.json，其次文件名解析。
    返回 (是否生成, nfo路径或原因)"""
    base, ext = os.path.splitext(video_path)
    if ext.lower() not in VIDEO_EXTS:
        return False, "非视频文件"
    nfo_path = base + ".nfo"
    if os.path.isfile(nfo_path):
        return False, "已存在 nfo"
    filename = os.path.basename(video_path)
    parent = os.path.basename(os.path.dirname(video_path))

    # 1) meta.json（Douyin_TikTok_Download_API v5）优先——字段准确
    meta = _meta_from_metajson(video_path)
    if meta:
        content = build_nfo(meta, nickname_fallback=parent or "未知博主")
        with open(nfo_path, "w", encoding="utf-8") as f:
            f.write(content)
        return True, nfo_path

    # 2) DouK 文件名解析
    meta = parse_filename(filename, parent_dir=parent)
    content = build_nfo(meta or {}, nickname_fallback=parent or "未知博主")
    with open(nfo_path, "w", encoding="utf-8") as f:
        f.write(content)
    return True, nfo_path
