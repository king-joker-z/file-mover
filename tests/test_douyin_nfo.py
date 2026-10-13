"""douyin_nfo 边界情况测试套件"""
import errno, json, os, sys, stat, tempfile
from datetime import datetime
from types import SimpleNamespace
import unittest
from unittest.mock import patch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.douyin_nfo import (parse_filename, build_nfo, ensure_nfo,
                            ensure_filename_nfo, _create_nfo)
from app.nfo_fix import fix_nfo, prepare_nfo
import xml.etree.ElementTree as ET

class DouyinNfoTests(unittest.TestCase):
    """保留 B1–B22 的独立用例，供 unittest discover 收集。"""


def check(name, fn):
    fn.__doc__ = name
    setattr(DouyinNfoTests, f"test_{fn.__name__}", staticmethod(fn))

def b1():
    m = parse_filename("2026-05-11-7638661243377142373.mp4", parent_dir="up")
    assert m["aweme_id"] == "7638661243377142373", m
    assert m["date"] == "2026-05-11", m
check("B1 split=- 纯ID", b1)

def b2():
    m = parse_filename("2026-05-11 4K超清测试视频 7638661243377142373.mp4", parent_dir="up")
    assert m["aweme_id"] == "7638661243377142373", m
    assert "4K超清测试视频" in (m["desc"] or ""), m
check("B2 desc含4K", b2)

def b3():
    m = parse_filename("2026-05-11 2024年度总结 7638661243377142373.mp4", parent_dir="up")
    assert m["aweme_id"] == "7638661243377142373"
    assert "2024年度总结" in (m["desc"] or ""), m
check("B3 desc含年份数字", b3)

def b4():
    m = parse_filename("random_video.mp4", parent_dir="somewhere")
    assert m["aweme_id"] is None and m["date"] is None
    nfo = build_nfo(m)
    # 简易模式: stem 转 desc 时下划线变空格, title=desc
    assert "random" in nfo and "video" in nfo
    assert "uniqueid" not in nfo and "website" not in nfo
    assert "<year>" not in nfo
check("B4 完全无法解析→简易模式", b4)

def b5():
    m = parse_filename("2026-05-11 测试<A>&B>标签 7638661243377142373.mp4", parent_dir="up&co")
    nfo = build_nfo(m)
    assert "&amp;" in nfo and "&lt;A&gt;" in nfo
    ET.fromstring(nfo)
check("B5 XML特殊字符转义", b5)

def b6():
    m = parse_filename("2026-05-11 ]]>注入测试 7638661243377142373.mp4", parent_dir="up")
    ET.fromstring(build_nfo(m))
check("B6 XML注入尝试", b6)

def b7():
    m = parse_filename("2026-05-11 12-30-45 7638661243377142373.mp4", parent_dir="up")
    assert m["aweme_id"] == "7638661243377142373", m
    print("      带时间解析: date=%r desc=%r" % (m["date"], m["desc"]))
check("B7 文件名带时间", b7)

def b8():
    long_desc = "很长的描述" * 40
    m = parse_filename(f"2026-05-11 {long_desc} 7638661243377142373.mp4", parent_dir="up")
    assert m["aweme_id"] == "7638661243377142373", m
check("B8 超长文件名", b8)

def b9():
    m = parse_filename("7638661243377142373.mp4", parent_dir="up")
    assert m["aweme_id"] == "7638661243377142373"
    assert m["desc"] is None
    nfo = build_nfo(m)
    assert "<year>" not in nfo and "<premiered>" not in nfo
check("B9 无日期无描述", b9)

def b10():
    d = tempfile.mkdtemp()
    f = os.path.join(d, "2026-05-11 7638661243377142373.txt")
    open(f, "w").write("x")
    ok, reason = ensure_nfo(f)
    assert not ok and reason == "非视频文件"
    assert not os.path.exists(os.path.join(d, "2026-05-11 7638661243377142373.nfo"))
check("B10 非视频扩展跳过", b10)

def b11():
    d = tempfile.mkdtemp()
    f = os.path.join(d, "2026-05-11 7638661243377142373-1.jpg")
    open(f, "w").write("x")
    ok, reason = ensure_nfo(f)
    assert not ok
check("B11 图集图片不生成", b11)

def b12():
    m = parse_filename("2026-05-11 7638661243377142373.mp4", parent_dir="月岛川祈_66⚡")
    ET.fromstring(build_nfo(m))
check("B12 昵称含emoji", b12)

def b13():
    d = tempfile.mkdtemp()
    vp = os.path.join(d, "2026-05-11 7638661243377142373.mp4")
    open(vp, "w").write("v")
    np = os.path.join(d, "2026-05-11 7638661243377142373.nfo")
    open(np, "w").write("corrupted-garbage")
    ok, reason = ensure_nfo(vp)
    assert not ok and reason == "已存在 nfo"
    assert open(np).read() == "corrupted-garbage"
check("B13 已有损坏nfo不覆盖", b13)

def b14():
    m = parse_filename("2026-05-11 7638661243377142373.mp4", parent_dir="")
    nfo = build_nfo(m)
    assert "未知博主" in nfo
check("B14 空父目录fallback", b14)

def b15():
    m = parse_filename("2026-05-11-月岛川祈_66-你在乎我的脸吗-7638661243377142373.mp4", parent_dir=None)
    assert m["aweme_id"] == "7638661243377142373", m
    assert m["date"] == "2026-05-11", m
    print("      含nickname样例 desc=%r" % (m["desc"],))
    ET.fromstring(build_nfo(m, nickname_fallback="博主"))
check("B15 name_format含nickname", b15)

# B16: 只读源目录 → ensure_nfo 写入失败应抛 OSError 由 scanner 捕获
def b16():
    d = tempfile.mkdtemp()
    vp = os.path.join(d, "2026-05-11 7638661243377142373.mp4")
    open(vp, "w").write("v")
    os.chmod(d, stat.S_IRUSR | stat.S_IXUSR)
    try:
        try:
            ensure_nfo(vp)
            raised = False
        except OSError:
            raised = True
        assert raised, "只读目录应抛 OSError（scanner 会捕获跳过）"
    finally:
        os.chmod(d, stat.S_IRWXU)
check("B16 只读目录写入失败可捕获", b16)

def b17():
    meta = {"title": "标题\x00\x1f\ufffe<&", "desc": "内容\x0b\ud800",
            "nickname": "作者\x01", "date": "2026-01-01\x02", "aweme_id": "123\x03"}
    movie = ET.fromstring(build_nfo(meta))
    assert movie.findtext("title") == "标题<&"
    assert movie.findtext("plot") == "内容"
    assert movie.findtext("studio") == "作者"
    assert movie.findtext("uniqueid") == "123"
check("B17 build_nfo 清除 XML 禁止字符", b17)

def b18():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "clip.nfo")
        with patch("app.douyin_nfo.os.fsync", side_effect=OSError("磁盘写入失败")):
            try:
                _create_nfo(path, "半截")
                assert False, "应抛出写入异常"
            except OSError:
                pass
        assert not os.path.lexists(path)
        assert os.listdir(d) == [], os.listdir(d)
        assert _create_nfo(path, "完整")
        assert not _create_nfo(path, "不应覆盖")
        assert open(path, encoding="utf-8").read() == "完整"
check("B18 写入失败不留半截目标且可以重试", b18)

def b19():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "clip.nfo")
        protected = os.path.join(d, "protected")
        with open(protected, "w") as f:
            f.write("原始")
        original_link = os.link
        def concurrent_link(src, dst, **kwargs):
            os.symlink(protected, dst)
            return original_link(src, dst, **kwargs)
        with patch("app.douyin_nfo.os.link", side_effect=concurrent_link):
            assert not _create_nfo(path, "新内容")
        assert os.path.islink(path)
        assert open(protected).read() == "原始"
        assert sorted(os.listdir(d)) == ["clip.nfo", "protected"]
check("B19 并发创建软链不覆盖", b19)

def b20():
    with tempfile.TemporaryDirectory() as d:
        video = os.path.join(d, "clip.mp4")
        open(video, "wb").close()
        for existing in ("CLIP.NFO", "clip.NFO"):
            nfo = os.path.join(d, existing)
            with open(nfo, "w") as f:
                f.write("keep")
            assert ensure_nfo(video) == (False, "已存在 nfo")
            assert open(nfo).read() == "keep"
            os.unlink(nfo)
        upper = os.path.join(d, "CLIP.NFO")
        os.symlink("missing", upper)
        assert ensure_nfo(video) == (False, "已存在 nfo")
        os.unlink(upper)
        assert ensure_nfo(video)[0]
        assert ET.parse(os.path.join(d, "clip.nfo")).getroot().tag == "movie"
check("B20 自动生成检测大小写 NFO 与已有路径", b20)

def b21():
    with tempfile.TemporaryDirectory() as d:
        video = os.path.join(d, "clip.mp4")
        open(video, "wb").close()
        for timestamp in ("1767225600", "1767225600000", 1767225600, 1767225600000):
            with open(os.path.join(d, "meta.json"), "w") as f:
                json.dump({"title": "clip", "publish_time": timestamp}, f)
            generated, path = ensure_nfo(video)
            assert generated, path
            expected = datetime.fromtimestamp(1767225600).strftime("%Y-%m-%d")
            assert ET.parse(path).getroot().findtext("premiered") == expected
            os.unlink(path)
        with open(os.path.join(d, "meta.json"), "w") as f:
            json.dump({"publish_time": "9999999999999"}, f)
        generated, path = ensure_nfo(video)
        assert generated and ET.parse(path).getroot().find("premiered") is None
check("B21 meta 时间戳秒毫秒与不合理值", b21)

def b22():
    raw = b"<actor><name>12345</name><role>  </role></actor>"
    assert prepare_nfo(raw, True) == (raw, [])
    assert fix_nfo(raw.decode())[1] == []
    fixed, applied = fix_nfo(raw.decode() + "<actor><name>67890</name><role>Alice</role></actor>")
    assert "<name>12345</name>" in fixed and "<name>Alice</name>" in fixed
    assert applied == ["actor-uid-name→role x1"]
check("B22 空 role 不替换 UID 与计数", b22)

def b23():
    for code in (errno.EPERM, errno.EOPNOTSUPP, errno.ENOSYS, errno.EXDEV):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "clip.nfo")
            with patch("app.douyin_nfo.os.link", side_effect=OSError(code, "unsupported")):
                assert _create_nfo(path, "完整内容")
            with open(path, encoding="utf-8") as f:
                assert f.read() == "完整内容"
            assert os.listdir(d) == ["clip.nfo"]
check("B23 不支持 hardlink 时排他复制发布", b23)

def b24():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "clip.nfo")
        protected = os.path.join(d, "protected")
        with open(protected, "w") as f:
            f.write("不可覆盖")
        def lose_race(src, dst, **kwargs):
            os.symlink(protected, dst)
            raise OSError(errno.EPERM, "hardlink unsupported")
        with patch("app.douyin_nfo.os.link", side_effect=lose_race):
            assert not _create_nfo(path, "new")
        assert os.path.islink(path)
        with open(protected) as f:
            assert f.read() == "不可覆盖"
        os.unlink(path)
        def create_before_open(src, dst, **kwargs):
            with open(dst, "w") as f:
                f.write("竞争者")
            raise OSError(errno.EXDEV, "hardlink unsupported")
        with patch("app.douyin_nfo.os.link", side_effect=create_before_open):
            assert not _create_nfo(path, "new")
        with open(path) as f:
            assert f.read() == "竞争者"
        os.unlink(path)
        real_open = os.open
        def race_at_exclusive_open(name, flags, *args, **kwargs):
            if name == path:
                os.symlink(protected, path)
            return real_open(name, flags, *args, **kwargs)
        with patch("app.douyin_nfo.os.link", side_effect=OSError(errno.EPERM, "unsupported")), \
                patch("app.douyin_nfo.os.open", side_effect=race_at_exclusive_open):
            assert not _create_nfo(path, "new")
        assert os.path.islink(path)
        os.unlink(path)
        with patch("app.douyin_nfo.os.link", side_effect=OSError(errno.EIO, "real failure")):
            try:
                _create_nfo(path, "new")
                assert False, "非不支持错误不能回退"
            except OSError as e:
                assert e.errno == errno.EIO
        assert os.listdir(d) == ["protected"]
check("B24 竞争软链/文件不覆盖且其它错误不回退", b24)

def b25():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "clip.nfo")
        real_fsync = os.fsync
        for mode in ("write_failure", "hash_mismatch", "replaced"):
            calls = 0
            protected = os.path.join(d, "protected")
            if mode == "replaced":
                with open(protected, "w") as f:
                    f.write("保留")
            def inject(fd):
                nonlocal calls
                calls += 1
                if calls == 2:
                    if mode == "write_failure":
                        raise OSError(errno.EIO, "target fsync failed")
                    if mode == "hash_mismatch":
                        with open(path, "r+b") as f:
                            f.write(b"bad")
                    if mode == "replaced":
                        os.unlink(path)
                        os.symlink(protected, path)
                return real_fsync(fd)
            with patch("app.douyin_nfo.os.link", side_effect=OSError(errno.EPERM, "unsupported")), \
                    patch("app.douyin_nfo.os.fsync", side_effect=inject):
                try:
                    _create_nfo(path, "原内容")
                    assert False, mode
                except OSError:
                    pass
            if mode == "replaced":
                assert os.path.islink(path)
                with open(protected) as f:
                    assert f.read() == "保留"
                os.unlink(path)
                os.unlink(protected)
            else:
                assert not os.path.lexists(path), mode
            assert os.listdir(d) == [], (mode, os.listdir(d))
check("B25 复制失败/读回不一致清理自己的目标且不删竞争者", b25)

def b26():
    with tempfile.TemporaryDirectory() as d:
        video = os.path.join(d, "clip.mp4")
        open(video, "wb").close()
        real_stat = os.stat
        def invalid_time(path, *args, **kwargs):
            result = real_stat(path, *args, **kwargs)
            if path == video:
                return SimpleNamespace(st_mode=result.st_mode, st_mtime=1e100)
            return result
        with patch("app.douyin_nfo.os.stat", side_effect=invalid_time):
            assert ensure_filename_nfo(video, ["clip.mp4"]) == (False, "视频时间无效")
        assert os.listdir(d) == ["clip.mp4"]
check("B26 畸形 mtime 溢出保守跳过", b26)

if __name__ == "__main__":
    unittest.main()
