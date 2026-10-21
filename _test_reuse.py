"""验证「复用已下载层」与「保留进度」的修复。

这个测试直接复现用户遇到的事故链：
  1) 下载若干层 -> 中途失败
  2) 重跑 -> 全部层重下（旧行为）
修复后应当：重跑时已存在且内容正确的层**不再联网**。

用本地假 registry 计数真实请求数来判定「有没有重下」。
"""
import contextlib
import hashlib
import io
import json
import os
import shutil
import sys
import tarfile
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dip  # noqa: E402

PASSED = []
OK = []


def blob_of(n_bytes, seed):
    """生成 n_bytes 的确定性伪随机数据（并做成合法 tar.gz 不行，这里只测 blob 层）。"""
    h = hashlib.sha256(str(seed).encode()).digest()
    out = bytearray()
    while len(out) < n_bytes:
        h = hashlib.sha256(h).digest()
        out += h
    return bytes(out[:n_bytes])


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_GET(self):
        s = self.server
        if self.path == "/v2/":
            self.send_response(401)
            self.send_header("WWW-Authenticate",
                             'Bearer realm="http://%s:%d/token",service="fake"'
                             % self.server.server_address)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path.startswith("/token"):
            body = json.dumps({"token": "tok", "expires_in": 3600}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if "/blobs/" not in self.path:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.headers.get("Authorization") != "Bearer tok":
            self.send_response(401)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        dg = self.path.rsplit("/", 1)[-1]
        data = s.blobs.get(dg)
        if data is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        rng = self.headers.get("Range")
        with s.lock:
            s.requests.append((dg, rng))
        start = 0
        if rng:
            start = int(rng.split("=", 1)[1].split("-")[0])
            self.send_response(206)
            self.send_header("Content-Range", "bytes %d-%d/%d" % (start, len(data) - 1, len(data)))
        else:
            self.send_response(200)
        body = data[start:]
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass


def make_server(blobs):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    srv.blobs = blobs
    srv.requests = []
    srv.lock = threading.Lock()
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def run_case(name, fn):
    fn()
    PASSED.append(name)
    print("  [PASS] %s" % name)


# --------------------------------------------------------------------------
def test_reuse_verified_blob_without_network():
    """已存在且内容正确的 blob：复用，且**不得产生任何网络请求**。"""
    data = blob_of(3 * 1024 * 1024, 1)
    dg = "sha256:" + hashlib.sha256(data).hexdigest()
    srv = make_server({dg: data})
    tmpd = tempfile.mkdtemp()
    try:
        reg = dip.Registry("http://127.0.0.1:%d" % srv.server_address[1], "testrepo")
        dest = os.path.join(tmpd, dg.replace(":", "_"))
        with open(dest, "wb") as f:
            f.write(data)                      # 预置一份完好的 blob
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            n = dip.download_blob([reg], dg, dest, "layer", None, "k",
                                  expected_size=len(data))
        assert n == len(data), n
        assert not srv.requests, "已存在且正确的 blob 不该再联网，却发了请求: %s" % srv.requests
    finally:
        srv.shutdown()
        shutil.rmtree(tmpd, ignore_errors=True)


def test_corrupt_same_size_blob_is_redownloaded():
    """长度对但内容坏：必须识别出来并重下，不能直接复用。"""
    data = blob_of(2 * 1024 * 1024, 2)
    dg = "sha256:" + hashlib.sha256(data).hexdigest()
    srv = make_server({dg: data})
    tmpd = tempfile.mkdtemp()
    try:
        reg = dip.Registry("http://127.0.0.1:%d" % srv.server_address[1], "testrepo")
        dest = os.path.join(tmpd, dg.replace(":", "_"))
        bad = bytearray(data)
        bad[0] ^= 0xFF                          # 改一个字节，长度不变
        with open(dest, "wb") as f:
            f.write(bytes(bad))
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            n = dip.download_blob([reg], dg, dest, "layer", None, "k",
                                  expected_size=len(data))
        assert n == len(data), n
        assert srv.requests, "内容损坏的 blob 没有被重下"
        with open(dest, "rb") as f:
            assert hashlib.sha256(f.read()).hexdigest() == dg.split(":")[1], "重下后内容仍不对"
    finally:
        srv.shutdown()
        shutil.rmtree(tmpd, ignore_errors=True)


def test_partial_then_second_run_continues():
    """第一次下到一半失败，第二次运行必须从断点继续而不是从 0。"""
    data = blob_of(4 * 1024 * 1024, 3)
    dg = "sha256:" + hashlib.sha256(data).hexdigest()
    srv = make_server({dg: data})
    tmpd = tempfile.mkdtemp()
    try:
        reg = dip.Registry("http://127.0.0.1:%d" % srv.server_address[1], "testrepo")
        dest = os.path.join(tmpd, dg.replace(":", "_"))
        # 模拟上次跑到 1.5 MiB 就断了：留下 .part
        with open(dest + ".part", "wb") as f:
            f.write(data[:int(1.5 * 1024 * 1024)])
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            n = dip.download_blob([reg], dg, dest, "layer", None, "k",
                                  expected_size=len(data))
        assert n == len(data), n
        ranges = [r for (_d, r) in srv.requests if r]
        assert ranges, "没有发 Range 请求 -> 说明是从头重下"
        starts = [int(r.split("=", 1)[1].split("-")[0]) for r in ranges]
        assert starts[0] == int(1.5 * 1024 * 1024), "续传起点不对: %s" % starts[0]
        assert 0 not in starts, "出现 start=0，进度被丢弃: %s" % starts
    finally:
        srv.shutdown()
        shutil.rmtree(tmpd, ignore_errors=True)


def test_blobs_preserved_by_default():
    """默认必须保留压缩层（--clean-blobs 才删），否则下次运行无从复用。"""
    import inspect
    src = inspect.getsource(dip.cmd_pull)
    assert "--clean-blobs" in open(dip.__file__, encoding="utf-8").read(), \
        "应提供 --clean-blobs 开关"
    assert "if args.clean_blobs:" in src, "删除压缩层的判断应为 opt-in"
    assert "if not args.keep_blobs:" not in src, "仍在默认删除压缩层"
    assert "args.keep_blobs" not in src, "残留 keep_blobs 引用"


def test_workdir_kept_on_failure():
    """工作目录清理必须在成功之后（失败时留下续传本钱）。"""
    import inspect
    src = inspect.getsource(dip.cmd_pull)
    # 清理语句必须出现在「完成」输出之前、且在 try 体内（非 finally）
    idx_clean = src.find("shutil.rmtree(work")
    idx_done = src.find('print("\\n完成')
    assert idx_clean != -1, "找不到工作目录清理点"
    assert idx_done != -1 and idx_clean < idx_done, \
        "清理点应在成功收尾之前，不能在任何失败路径上执行"
    fin = src.find("finally:")
    if fin != -1:
        assert "rmtree(work" not in src[fin:], "工作目录不能在 finally 里被清理"


def main():
    print("dip 复用/保留进度 回归测试")
    cases = [
        ("已存在且正确的 blob -> 复用且不联网", test_reuse_verified_blob_without_network),
        ("长度对但内容坏 -> 识别并重下", test_corrupt_same_size_blob_is_redownloaded),
        ("上次下到一半 -> 第二次从断点续传", test_partial_then_second_run_continues),
        ("默认保留压缩层（opt-in 才删）", test_blobs_preserved_by_default),
        ("失败时保留工作目录", test_workdir_kept_on_failure),
    ]
    for name, fn in cases:
        run_case(name, fn)
    print("\n全部通过: %d/%d" % (len(PASSED), len(cases)))


if __name__ == "__main__":
    main()
