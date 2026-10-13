"""抖音视频 nfo 生成器
优先级：同目录 meta.json（Douyin_TikTok_Download_API v5 产物）> DouK 文件名解析 > 简易兜底
"""
import os
import json
import re
import stat
import errno
import hashlib
import tempfile
from xml.sax.saxutils import escape
from typing import Optional
from datetime import datetime

# 抖音作品 ID：10~20 位纯数字
_ID_RE = re.compile(r"\b(\d{10,20})\b")
# 日期时间: 2026-05-11 12-30-45 / 2026-05-11 / 20260511 等
_DATE_RE = re.compile(r"(\d{4}[-/年]\d{1,2}[-/月]\d{1,2}(?:[ 日时]*\d{1,2}[:\-时]\d{1,2}(?:[:\-]\d{1,2})?)?|\d{8})")
# 常见分隔符（DouK split 默认 "-"，也容忍空格/下划线）
_SPLIT_RE = re.compile(r"\s*[-—_ ]\s*")

VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".flv", ".webm"}
POSTER_EXTS = {".jpg", ".jpeg", ".png", ".webp"}


def _publish_date(value) -> Optional[str]:
    """接受合理的秒/毫秒时间戳和原有 ISO 日期格式。"""
    if not value or isinstance(value, bool):
        return None
    try:
        numeric = (isinstance(value, (int, float)) or
                   isinstance(value, str) and re.fullmatch(r"\d{10}|\d{13}", value.strip()))
        if numeric:
            timestamp = float(value)
            if timestamp >= 1e12:
                timestamp /= 1000
            date = datetime.fromtimestamp(timestamp)
            return date.strftime("%Y-%m-%d") if 2000 <= date.year <= 2100 else None
        s = str(value).replace("T", " ").split(".")[0].split("+")[0]
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(s.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    except (OSError, ValueError, OverflowError):
        pass
    return None


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
    date_str = _publish_date(pub)
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

    esc = lambda x: _xml_text(str(x))
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


def _copy_nfo_exclusive(tmp: str, path: str) -> bool:
    """不支持 hardlink 的挂载上的回退；写入期间目标可能被其他进程看到。"""
    flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o644)
    except FileExistsError:
        return False
    identity = os.fstat(fd)
    try:
        with os.fdopen(fd, "w+b") as target, open(tmp, "rb") as source:
            expected = hashlib.sha256()
            while chunk := source.read(64 * 1024):
                expected.update(chunk)
                target.write(chunk)
            target.flush()
            os.fsync(target.fileno())
            target.seek(0)
            actual = hashlib.sha256()
            while chunk := target.read(64 * 1024):
                actual.update(chunk)
            if actual.digest() != expected.digest():
                raise OSError(errno.EIO, "NFO 写入后校验失败", path)
            published = os.lstat(path)
            if (published.st_dev, published.st_ino) != (identity.st_dev, identity.st_ino):
                raise OSError(errno.EAGAIN, "NFO 目标在写入期间被替换", path)
        return True
    except BaseException:
        # 只清理由本次排他创建的 inode；不要删除竞争者替换后的文件/软链。
        try:
            current = os.lstat(path)
        except FileNotFoundError:
            pass
        else:
            if (current.st_dev, current.st_ino) == (identity.st_dev, identity.st_ino):
                os.unlink(path)
        raise


def _create_nfo(path: str, content: str) -> bool:
    """先完整写入同目录临时文件，再尽可能原子地排他发布。"""
    fd, tmp = tempfile.mkstemp(prefix=".fm-nfo-", suffix=".moving",
                                dir=os.path.dirname(path) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
            os.fchmod(stream.fileno(), 0o644)
        try:
            if _has_nfo(path):
                return False
            os.link(tmp, path, follow_symlinks=False)
        except FileExistsError:
            return False
        except OSError as error:
            if error.errno not in (errno.EPERM, errno.EOPNOTSUPP, errno.ENOSYS, errno.EXDEV):
                raise
            # 重新检查同名 NFO；O_EXCL 是最后一道防线，不依赖预检查防竞态。
            if _has_nfo(path):
                return False
            return _copy_nfo_exclusive(tmp, path)
        return True
    finally:
        os.unlink(tmp)


def _has_nfo(video_path: str) -> bool:
    """从实际目录检查大小写不敏感的同名 NFO。"""
    directory = os.path.dirname(video_path) or "."
    stem = os.path.splitext(os.path.basename(video_path))[0].casefold()
    return any(os.path.splitext(name)[0].casefold() == stem
               and os.path.splitext(name)[1].casefold() == ".nfo"
               for name in os.listdir(directory))


def ensure_nfo(video_path: str, name_format_hint: str = "create_time uid id",
               filenames: list[str] | None = None) -> tuple[bool, str]:
    """若视频无伴生 nfo 则生成。优先 meta.json，其次文件名解析。
    返回 (是否生成, nfo路径或原因)"""
    base, ext = os.path.splitext(video_path)
    if ext.lower() not in VIDEO_EXTS:
        return False, "非视频文件"
    nfo_path = base + ".nfo"
    # 扫描器已枚举该目录：复用清单，避免对每个视频重复枚举整个 NAS 目录。
    stem = os.path.basename(base).casefold()
    names = filenames if filenames is not None else os.listdir(os.path.dirname(video_path) or ".")
    if any(os.path.splitext(name)[0].casefold() == stem
           and os.path.splitext(name)[1].casefold() == ".nfo" for name in names):
        return False, "已存在 nfo"
    filename = os.path.basename(video_path)
    parent = os.path.basename(os.path.dirname(video_path))

    # 1) meta.json（Douyin_TikTok_Download_API v5）优先——字段准确
    meta = _meta_from_metajson(video_path)
    if meta:
        content = build_nfo(meta, nickname_fallback=parent or "未知博主")
        return (True, nfo_path) if _create_nfo(nfo_path, content) else (False, "已存在 nfo")

    # 2) DouK 文件名解析
    meta = parse_filename(filename, parent_dir=parent)
    content = build_nfo(meta or {}, nickname_fallback=parent or "未知博主")
    return (True, nfo_path) if _create_nfo(nfo_path, content) else (False, "已存在 nfo")


def backfill_nfo_for_video(video_path: str, cloud_video: str,
                           filenames: list[str] | None = None) -> tuple[bool, str]:
    """已迁移视频缺少 NFO 时，依据原始相对文件名生成目标目录中的 NFO。"""
    base, ext = os.path.splitext(cloud_video)
    if ext.lower() not in VIDEO_EXTS:
        return False, "非视频文件"
    nfo_path = base + ".nfo"
    names = filenames if filenames is not None else os.listdir(os.path.dirname(cloud_video) or ".")
    stem = os.path.basename(base).casefold()
    if any(os.path.splitext(name)[0].casefold() == stem
           and os.path.splitext(name)[1].casefold() == ".nfo" for name in names):
        return False, "已存在 nfo"
    parent = os.path.basename(os.path.dirname(video_path))
    meta = parse_filename(os.path.basename(video_path), parent_dir=parent) or {}
    content = build_nfo(meta, nickname_fallback=parent or "未知博主")
    return (True, nfo_path) if _create_nfo(nfo_path, content) else (False, "已存在 nfo")


def _xml_text(value: str) -> str:
    """文件名可能含有 XML 1.0 不允许的控制字符。"""
    clean = "".join(ch for ch in value if ch in "\t\n\r" or
                    32 <= ord(ch) <= 0x10FFFF and not 0xD800 <= ord(ch) <= 0xDFFF
                    and (ord(ch) & 0xFFFF) not in (0xFFFE, 0xFFFF))
    return escape(clean)


def ensure_filename_nfo(video_path: str, filenames: list[str],
                        poster_filenames: list[str] | None = None,
                        target_path: str | None = None) -> tuple[bool, str]:
    """按 标题-作者-YYYYMMDD 生成 NFO；可在已迁移目标侧按原文件名补齐。"""
    stem, ext = os.path.splitext(os.path.basename(video_path))
    if ext.lower() not in VIDEO_EXTS:
        return False, "非视频文件"
    actual_video = target_path or video_path
    try:
        if not stat.S_ISREG(os.lstat(actual_video).st_mode):
            return False, "视频不是普通文件"
    except FileNotFoundError:
        return False, "视频不存在"
    parts = stem.rsplit("-", 2)
    date = None
    if (len(parts) == 3 and parts[0].strip() and parts[1].strip()
            and re.fullmatch(r"[0-9]{8}", parts[2])):
        try:
            date = datetime.strptime(parts[2], "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    # 同目录已有同名 NFO（包含大写扩展名）时绝不抢占下载器的内容。
    target_stem = os.path.splitext(os.path.basename(actual_video))[0]
    if any(os.path.splitext(name)[0].casefold() == target_stem.casefold()
           and os.path.splitext(name)[1].lower() == ".nfo" for name in filenames):
        return False, "已存在 nfo"
    nfo_path = (os.path.splitext(target_path)[0] + ".nfo" if target_path
                else os.path.join(os.path.dirname(video_path), stem + ".nfo"))
    try:
        os.lstat(nfo_path)
    except FileNotFoundError:
        pass
    else:
        return False, "已存在 nfo"
    poster = next((name for name in (poster_filenames if poster_filenames is not None else filenames)
                   if os.path.splitext(name)[0].casefold() == (target_stem + "-poster").casefold()
                   and os.path.splitext(name)[1].lower() in POSTER_EXTS
                   and stat.S_ISREG(os.lstat(os.path.join(os.path.dirname(actual_video), name)).st_mode)), None)
    if date is None:
        video_dir = os.path.dirname(video_path)
        title = os.path.basename(video_dir.rstrip(os.sep)) or stem
        author = os.path.basename(os.path.dirname(video_dir.rstrip(os.sep)).rstrip(os.sep)) or "未知博主"
        video_info = os.stat(actual_video)
        # macOS 等系统可提供出生时间；否则用修改时间，避免将 inode 变更时间误作创建时间。
        created_at = getattr(video_info, "st_birthtime", None) or video_info.st_mtime
        try:
            date = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d")
        except (OverflowError, OSError, ValueError):
            # 异常 mtime 不应生成错误日期或发布不完整的 NFO。
            return False, "视频时间无效"
    else:
        title, author = parts[0].strip(), parts[1].strip()
    title, author = _xml_text(title), _xml_text(author)
    lines = ['<?xml version="1.0" encoding="utf-8" standalone="yes"?>', '<movie>',
             '  <lockdata>true</lockdata>', f'  <title>{title}</title>',
             f'  <releasedate>{date}</releasedate>', f'  <premiered>{date}</premiered>',
             '  <genre>时尚</genre>', '  <genre>随拍</genre>',
             '  <actor>', f'    <name>{author}</name>',
             '    <role>主演</role>', '    <tmdbid></tmdbid>', '  </actor>']
    if poster:
        artwork = _xml_text(poster)
        lines.extend((f'  <thumb aspect="poster">{artwork}</thumb>',
                      '  <fanart>', f'    <thumb>{artwork}</thumb>', '  </fanart>'))
    lines.append('</movie>')
    return ((True, nfo_path) if _create_nfo(nfo_path, "\n".join(lines) + "\n")
            else (False, "已存在 nfo"))
