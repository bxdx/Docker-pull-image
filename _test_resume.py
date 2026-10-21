# -*- coding: utf-8 -*-
"""「下载反复从头开始」的回归测试。

真实事故：下 `vllm/vllm-openai:glm53-flash` 的 4.73GB 层，反复出现「下了一部分
又从头开始」，永远下不完。查出两个根因，这个文件把它们钉死：

  A) dockerproxy.net **无视 Range** —— 任何 Range 请求都回 200 整份文件。它测速
     最快、因而最常被选中；拿它续传就等于把已下的几百 MB 全部截断重来。
  B) 镜像站 token 有寿命（实测 1500s / 3600s），而 4.7GB 要下近一小时。旧实现
     把 token 无限缓存，过期后撞 401，而 401 分支直接把 .part 删掉从零开始。

这里的假 registry 可以按需扮演这些坏站：
  mode="ignore"      无视 Range，和 dockerproxy.net 一样回 200 整份
  cut_at=N           只吐到第 N 字节就掐断连接（模拟服务端按令牌寿命切断）
  auth_401=K         前 K 次「带 Range 且 start>0」的请求回 401（模拟令牌过期）

自测必须绕开环境代理（本机 http_proxy=127.0.0.1:1560 连 127.0.0.1 都会被劫持），
所以下面显式构造了 Registry，而不是走 urllib 的全局 opener。
"""
import hashlib
import http.server
import importlib.util
import os
import re
import shutil
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("dipmod", os.path.join(HERE, "dip.py"))
dip = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dip)

SIZE = 12 * 1024 * 1024
DATA = bytes((i * 31 + (i >> 9) * 17) & 0xFF for i in range(SIZE))
DIGEST = "sha256:" + hashlib.sha256(DATA).hexdigest()
REF = "test/blob"
TMP = os.path.join(HERE, "_tmp_resume")

FAIL = []


def check(name, ok, detail=""):
    print("  [%s] %-54s %s" % ("PASS" if ok else "FAIL", name, detail))
    if not ok:
        FAIL.append(name)


def make_handler(cfg):
    lock = threading.Lock()

    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _hit(self):
            with lock:
                cfg["hits"] += 1
                return cfg["hits"]

        def _send_code(self, code):
            self.send_response(code)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            self._hit()
            if self.path.rstrip("/").endswith("/v2"):
                body = b"{}"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if "/blobs/" not in self.path:
                return self._send_code(404)

            rng = self.headers.get("Range")
            if rng:
                m = re.match(r"bytes=(\d+)-(\d*)", rng)
                start = int(m.group(1))
                end = int(m.group(2)) if m.group(2) else SIZE - 1
            else:
                start, end = 0, SIZE - 1
            end = min(end, SIZE - 1)
            full = (start == 0 and not rng)

            # 模拟令牌过期：前 K 次「带 Range 且 start>0」的请求回 401
            if rng and start > 0:
                with lock:
                    cfg["range_hits"] += 1
                    n = cfg["range_hits"]
                if cfg["auth_401"] and n <= cfg["auth_401"]:
                    cfg["saw_401"] = True
                    return self._send_code(401)

            if rng and start > SIZE - 1:
                return self._send_code(416)

            if rng and cfg["mode"] == "ok":
                with lock:
                    cfg["ranges"].append((start, end))
                self.send_response(206)
                self.send_header("Content-Range", "bytes %d-%d/%d" % (start, end, SIZE))
                self.send_header("Content-Length", str(end - start + 1))
                self.end_headers()
            elif rng:
                # 无视 Range：回 200 整份（Content-Length 仍是全量）
                with lock:
                    cfg["full_bodies"] += 1
                    cfg["ranges"].append(("FULL-200", start))
                self.send_response(200)
                self.send_header("Content-Length", str(SIZE))
                self.end_headers()
                start, end = 0, SIZE - 1
            else:
                with lock:
                    cfg["ranges"].append(("NORANGE", 0))
                    cfg["full_bodies"] += 1
                self.send_response(200)
                self.send_header("Content-Length", str(SIZE))
                self.end_headers()

            # 传输：声明的是全量长度，但只吐到 cut_at 就断线 —— 这样客户端会看到
            # 「还有 data 没读完就 EOF」，正是真实事故里服务端掐连接的样子。
            # cut 只生效**一次**：之后正常服务，否则会变成永远吐不出字节。
            cut = cfg["cut_at"]
            if cut is not None and start < cut and not cfg["cut_done"]:
                with lock:
                    cfg["cut_done"] = True
                limit = min(end, cut - 1)
            else:
                limit = end
            CH = 64 * 1024
            pos = start
            try:
                while pos <= limit:
                    n = min(CH, limit - pos + 1)
                    self.wfile.write(DATA[pos:pos + n])
                    with lock:
                        cfg["sent"] += n
                    pos += n
            except (BrokenPipeError, ConnectionResetError, OSError):
                return
            if cut is not None and limit < end:
                self.close_connection = True        # 提前断线

    return H


class Fake:
    def __init__(self, name, mode="ok", cut_at=None, auth_401=0, delay=0.0):
        self.name = name
        self.cfg = {"mode": mode, "cut_at": cut_at, "auth_401": auth_401,
                    "hits": 0, "range_hits": 0, "sent": 0, "cut_done": False,
                    "ranges": [], "full_bodies": 0, "saw_401": False}
        self.delay = delay
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.cfg))
        self.port = self.httpd.server_address[1]
        self.th = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.th.start()

    @property
    def url(self):
        return "http://127.0.0.1:%d" % self.port

    def reg(self):
        return dip.Registry(self.url, REF, timeout=20.0, retries=1)

    def stop(self):
        try:
            self.httpd.shutdown()
            self.httpd.server_close()
        except Exception:  # noqa: BLE001
            pass

    def __getattr__(self, k):
        try:
            return self.cfg[k]
        except KeyError:
            raise AttributeError(k)


def cleanup(*paths):
    """连 .part 和 .segNN 一起清掉。

    只删主文件是不够的：上一轮留下的分片会让下一轮的「无分片残留」断言
    恒假，也会让分片被误当成已完成而不再下（测试就失去意义了）。
    """
    import glob as _glob
    for p in paths:
        for q in [p, p + ".part"] + _glob.glob(p + ".seg*"):
            try:
                os.remove(q)
            except OSError:
                pass


def run_tests():
    shutil.rmtree(TMP, ignore_errors=True)   # 必须从干净状态开始
    os.makedirs(TMP, exist_ok=True)

    # ---------------------------------------------------------------- 0)
    print("[0] 健康站：单流下载应正常完成（基线）")
    s = Fake("good")
    dest = os.path.join(TMP, "t0.bin")
    cleanup(dest)
    n = dip.download_blob([s.reg()], DIGEST, dest, "L", None, "L1",
                          expected_size=SIZE, max_retries=6, idle_timeout=10.0,
                          names=["good"])
    check("内容正确", os.path.exists(dest) and
          hashlib.sha256(open(dest, "rb").read()).hexdigest() == DIGEST.split(":")[1],
          "%s" % dip.human_bytes(n))
    check("没有残留 .part", not os.path.exists(dest + ".part"))
    s.stop()

    # ---------------------------------------------------------------- 1)
    print("[1] 不认 Range 的站（仿 dockerproxy）不得让已下进度清零")
    bad = Fake("bad", mode="ignore", cut_at=3 * 1024 * 1024)
    good = Fake("good", mode="ok")
    dest = os.path.join(TMP, "t1.bin")
    cleanup(dest)
    n = dip.download_blob([bad.reg(), good.reg()], DIGEST, dest, "L", None, "L1",
                          expected_size=SIZE, max_retries=6, idle_timeout=10.0,
                          names=["bad", "good"])
    check("最终内容正确", os.path.exists(dest) and
          hashlib.sha256(open(dest, "rb").read()).hexdigest() == DIGEST.split(":")[1],
          "%s" % dip.human_bytes(n))
    g_req = [r[0] for r in good.cfg["ranges"] if isinstance(r[0], int)]
    check("好站是从**断点**续上的（不是从 0 重下）",
          bool(g_req) and min(g_req) >= 3 * 1024 * 1024,
          "好站请求的起始偏移 = %s" % (g_req or "无"))
    check("全程没有出现过 start=0 的续传请求（即从未从头重下）",
          not [x for x in g_req if x == 0],
          "start=0 的 Range 请求数=%d" % len([x for x in g_req if x == 0]))
    check("坏站被标记为不支持 Range",
          good is not None and bad.cfg["full_bodies"] >= 1,
          "坏站返回整份次数=%d" % bad.cfg["full_bodies"])
    check("好站拿到的字节 < 整份（证明前 3MiB 没白下）",
          good.cfg["sent"] < SIZE, "good 传了 %s / 共 %s"
          % (dip.human_bytes(good.cfg["sent"]), dip.human_bytes(SIZE)))
    check("没有残留 .part", not os.path.exists(dest + ".part"))
    bad.stop()
    good.stop()

    # ---------------------------------------------------------------- 2)
    print("[2] 令牌过期(401) 不得让已下进度清零")
    s = Fake("tok", mode="ok", cut_at=4 * 1024 * 1024, auth_401=3)
    dest = os.path.join(TMP, "t2.bin")
    cleanup(dest)
    n = dip.download_blob([s.reg()], DIGEST, dest, "L", None, "L1",
                          expected_size=SIZE, max_retries=6, idle_timeout=10.0,
                          names=["tok"])
    check("最终内容正确", os.path.exists(dest) and
          hashlib.sha256(open(dest, "rb").read()).hexdigest() == DIGEST.split(":")[1],
          "%s" % dip.human_bytes(n))
    check("确实遇到了 401（测试有效）", bool(s.cfg["saw_401"]))
    starts = [r[0] for r in s.cfg["ranges"] if isinstance(r[0], int)]
    check("401 之后从**原断点**续传（未归零）",
          bool(starts) and min(starts) == 4 * 1024 * 1024,
          "续传起始偏移 = %s（掐断点 = %d）"
          % (starts or "无", 4 * 1024 * 1024))
    check("401 之后没有出现 start=0 的续传请求",
          not [x for x in starts if x == 0],
          "start=0 的 Range 请求数=%d" % len([x for x in starts if x == 0]))
    s.stop()

    # ---------------------------------------------------------------- 3)
    print("[3] .part 其实已完整时应直接收尾，不打网络")
    s = Fake("good")
    dest = os.path.join(TMP, "t3.bin")
    cleanup(dest)
    with open(dest + ".part", "wb") as f:
        f.write(DATA)
    n = dip.download_blob([s.reg()], DIGEST, dest, "L", None, "L1",
                          expected_size=SIZE, max_retries=3, idle_timeout=10.0,
                          names=["good"])
    check("扶正为成品且内容正确", os.path.exists(dest) and
          hashlib.sha256(open(dest, "rb").read()).hexdigest() == DIGEST.split(":")[1],
          "%s" % dip.human_bytes(n))
    check("一次网络请求都没发", s.cfg["hits"] == 0, "hits=%d" % s.cfg["hits"])
    check(".part 已消失", not os.path.exists(dest + ".part"))
    s.stop()

    # ---------------------------------------------------------------- 4)
    print("[4] 多源：不能续传的站应被预检剔除，其余站继续把分片做完")
    bad = Fake("bad", mode="ignore")
    good = Fake("good", mode="ok")
    dest = os.path.join(TMP, "t4.bin")
    cleanup(dest)
    existing = set(os.listdir(TMP))
    n = dip.download_blob_multisource([bad.reg(), good.reg()], DIGEST, dest, "L",
                                      None, "L1", expected_size=SIZE,
                                      names=["bad", "good"], sources=2, segments=2,
                                      use_governor=True)
    check("最终内容正确", os.path.exists(dest) and
          hashlib.sha256(open(dest, "rb").read()).hexdigest() == DIGEST.split(":")[1],
          "%s" % dip.human_bytes(n))
    check("坏站只被探测了一次（随即被剔除）",
          bad.cfg["full_bodies"] <= 2, "坏站返回整份次数=%d" % bad.cfg["full_bodies"])
    check("好站承担了全部传输", good.cfg["sent"] >= SIZE,
          "good 传了 %s" % dip.human_bytes(good.cfg["sent"]))
    left = [f for f in os.listdir(TMP) if ".seg" in f]
    check("没有遗留 .seg 分片", not left, str(left[:4]))
    bad.stop()
    good.stop()

    # ---------------------------------------------------------------- 5)
    print("[5] 模式切换：单流残留的 .part 应被分片模式接管，不重下")
    a = Fake("a", mode="ok")
    b = Fake("b", mode="ok")
    dest = os.path.join(TMP, "t5.bin")
    cleanup(dest)
    with open(dest + ".part", "wb") as f:
        f.write(DATA[:3 * 1024 * 1024])
    n = dip.download_blob_multisource([a.reg(), b.reg()], DIGEST, dest, "L", None, "L1",
                                      expected_size=SIZE, names=["a", "b"],
                                      sources=2, segments=4, use_governor=True)
    check("最终内容正确", os.path.exists(dest) and
          hashlib.sha256(open(dest, "rb").read()).hexdigest() == DIGEST.split(":")[1],
          "%s" % dip.human_bytes(n))
    sent = a.cfg["sent"] + b.cfg["sent"]
    check("网络只补了余下部分（前 3MiB 被复用，没重下）",
          sent <= SIZE - 2 * 1024 * 1024,
          "网络共传 %s，复用前缀 3.0 MiB" % dip.human_bytes(sent))
    a.stop()
    b.stop()

    # ---------------------------------------------------------------- 5)
    print("[5] 模式切换：单流残留的 .part 应被分片模式接管，不重下")
    a = Fake("a", mode="ok")
    b = Fake("b", mode="ok")
    dest = os.path.join(TMP, "t5.bin")
    cleanup(dest)
    with open(dest + ".part", "wb") as f:
        f.write(DATA[:3 * 1024 * 1024])
    n = dip.download_blob_multisource([a.reg(), b.reg()], DIGEST, dest, "L", None, "L1",
                                      expected_size=SIZE, names=["a", "b"],
                                      sources=2, segments=4, use_governor=True)
    check("最终内容正确", os.path.exists(dest) and
          hashlib.sha256(open(dest, "rb").read()).hexdigest() == DIGEST.split(":")[1],
          "%s" % dip.human_bytes(n))
    sent = a.cfg["sent"] + b.cfg["sent"]
    check("网络只补了余下部分（前 3MiB 被复用，没重下）",
          sent <= SIZE - 2 * 1024 * 1024,
          "网络共传 %s，复用前缀 3.0 MiB" % dip.human_bytes(sent))
    a.stop()
    b.stop()

    print()
    if FAIL:
        print("!! %d 项失败：" % len(FAIL))
        for f in FAIL:
            print("   - %s" % f)
        return 1
    print("全部通过。")
    return 0


if __name__ == "__main__":
    try:
        rc = run_tests()
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    raise SystemExit(rc)
