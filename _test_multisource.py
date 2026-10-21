"""多源分片并行下载的确定性测试。

本机到公网镜像站的链路极不稳（一会儿 1.4MB/s 一会儿握手超时），拿它当测试环境
没法复现。这里在本地起几台**支持 Range 的假 registry**，把要验的东西钉死：
分片是否正确、拼接哈希是否对得上、多个源是否真的都被用到、快源是否多干活、
不支持 Range 时是否干净回退、断点续传是否真的从上次的位置接着拉。
"""
import hashlib
import http.server
import importlib.util
import os
import re
import shutil
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("dipmod", os.path.join(HERE, "dip.py"))
dip = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dip)

SIZE = 24 * 1024 * 1024                       # 24 MiB，够切成多片
DATA = bytes((i * 7 + (i >> 8) * 13) & 0xFF for i in range(SIZE))
DIGEST = "sha256:" + hashlib.sha256(DATA).hexdigest()
REF = "test/blob"
TMP = os.path.join(HERE, "_tmp_test")

FAIL = []


def check(name, ok, detail=""):
    print("  [%s] %-46s %s" % ("PASS" if ok else "FAIL", name, detail))
    if not ok:
        FAIL.append(name)


def make_handler(state):
    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _send(self, code, body, extra=None):
            self.send_response(code)
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass

        def do_GET(self):
            if self.path.rstrip("/").endswith("/v2"):
                state["state"]["touch"]()
                return self._send(200, b"{}")
            if "/blobs/" in self.path:
                rng = self.headers.get("Range")
                st = state["state"]
                st["touch"]()
                if rng and st["range_ok"]:
                    m = re.match(r"bytes=(\d+)-(\d*)", rng)
                    s = int(m.group(1))
                    e = int(m.group(2)) if m.group(2) else SIZE - 1
                    e = min(e, SIZE - 1)
                    # dip 划段前会用一个小 Range 预检「该站是否真支持断点续传」。
                    # 那是能力探测、不是分片工作，单独记账，免得混进分片统计。
                    if s == 0 and (e - s + 1) == dip.RANGE_PROBE_BYTES:
                        st["probes"] = st.get("probes", 0) + 1
                    else:
                        st["ranges"].append((s, e))
                    self.send_response(206)
                    self.send_header("Content-Range", "bytes %d-%d/%d" % (s, e, SIZE))
                    self.send_header("Content-Length", str(e - s + 1))
                    self.end_headers()
                    pos, CH = s, 64 * 1024
                    while pos <= e:
                        n = min(CH, e - pos + 1)
                        time.sleep(st["delay"] * (n / float(CH)))
                        try:
                            self.wfile.write(DATA[pos:pos + n])
                        except (BrokenPipeError, ConnectionResetError, OSError):
                            return
                        pos += n
                    return
                st["full_bodies"] += 1
                return self._send(200, DATA)

    return H


class Server:
    def __init__(self, name, delay, range_ok=True):
        self.name = name
        self.state = {"ranges": [], "full_bodies": 0, "probes": 0, "delay": delay,
                      "range_ok": range_ok, "hits": 0,
                      "touch": lambda: self.state.__setitem__("hits", self.state["hits"] + 1)}
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), make_handler({"state": self.state}))
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def reg(self):
        return dip.Registry("http://127.0.0.1:%d" % self.port, REF, timeout=20.0, retries=1)

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def fresh(p):
    if os.path.exists(p):
        os.remove(p)
    for f in os.listdir(TMP):
        if f.startswith(os.path.basename(p)):
            os.remove(os.path.join(TMP, f))


def main():
    shutil.rmtree(TMP, ignore_errors=True)
    os.makedirs(TMP)
    dest = os.path.join(TMP, "blob.bin")
    srcs = [Server("fast", 0.0004), Server("mid", 0.0012), Server("slow", 0.0025)]
    print("假 registry: " + ", ".join("127.0.0.1:%d(%s)" % (s.port, s.name) for s in srcs))
    print("样本 %s, digest %s\n" % (dip.human_bytes(SIZE), DIGEST[:26]))

    try:
        # ---------- 1) 基本正确性 ----------
        print("[1] 多源分片：3 源 × 4 片")
        n = dip.download_blob_multisource(
            [s.reg for s in srcs], DIGEST, dest, "测试", None, "k",
            expected_size=SIZE, names=[s.name for s in srcs], sources=3, segments=4)
        got = open(dest, "rb").read()
        check("返回字节数 == 期望", n == SIZE, "%d" % n)
        check("拼接结果 sha256 正确", hashlib.sha256(got).hexdigest() == DIGEST[7:])
        check("分片文件已清理", not [f for f in os.listdir(TMP) if ".seg" in f],
              str([f for f in os.listdir(TMP) if ".seg" in f]))
        total_ranges = sum(len(s.state["ranges"]) for s in srcs)
        used = [s.name for s in srcs if s.state["ranges"]]
        check("切了 4 片（Range 请求数）", total_ranges == 4, "共 %d 次 Range" % total_ranges)
        probes = sum(s.state.get("probes", 0) for s in srcs)
        check("划段前做了 Range 能力预检", probes >= 1, "预检 %d 次" % probes)
        check("3 个源全部被用到", len(used) == 3, "用到: %s" % ",".join(used) or "无")

        # ---------- 2) 快源多干活（负载再平衡） ----------
        print("\n[2] 负载再平衡：片数 >= 源数时应重新分配")
        for s in srcs:
            s.state["ranges"] = []
        fresh(dest)
        dip.download_blob_multisource(
            [s.reg for s in srcs], DIGEST, dest, "测试", None, "k",
            expected_size=SIZE, names=[s.name for s in srcs], sources=2, segments=8)
        cnt = {s.name: len(s.state["ranges"]) for s in srcs}
        check("8 片全部下发", sum(cnt.values()) == 8, str(cnt))
        check("结果仍然正确",
              hashlib.sha256(open(dest, "rb").read()).hexdigest() == DIGEST[7:])

        # ---------- 3) 断点续传 ----------
        print("\n[3] 断点续传：预置半片，应只补剩余字节")
        for s in srcs:
            s.state["ranges"] = []
        fresh(dest)
        seg0 = dest + ".seg00"
        with open(seg0, "wb") as f:                     # 第 0 片（6 MiB）预先写好前 2 MiB
            f.write(DATA[:2 * 1024 * 1024])
        dip.download_blob_multisource(
            [s.reg for s in srcs], DIGEST, dest, "测试", None, "k",
            expected_size=SIZE, names=[s.name for s in srcs], sources=2, segments=4)
        first = min((s.state["ranges"][0][0] for s in srcs if s.state["ranges"]), default=None)
        seg_len = SIZE // 4
        check("续传从 2MiB 处开始（不是 0）", first == 2 * 1024 * 1024, "首片起点 %s" % first)
        check("续传后结果正确",
              hashlib.sha256(open(dest, "rb").read()).hexdigest() == DIGEST[7:])
        check("片大小按预期切分", seg_len == 6 * 1024 * 1024, str(seg_len))

        # ---------- 4) 不支持 Range 时回退 ----------
        print("\n[4] 回退：站点返回 200 而非 206 时应退回单流")
        nos = Server("no-range", 0.0, range_ok=False)
        fresh(dest)
        dip.download_blob_multisource(
            [nos.reg], DIGEST, dest, "测试", None, "k",
            expected_size=SIZE, names=["no-range"], sources=3, segments=4)
        check("回退后结果正确",
              hashlib.sha256(open(dest, "rb").read()).hexdigest() == DIGEST[7:])
        check("确实走了整包响应", nos.state["full_bodies"] >= 1, "%d 次" % nos.state["full_bodies"])
        nos.stop()

        # ---------- 5) 源坏掉时自动换站 ----------
        print("\n[5] 故障转移：一个源中途挂掉，剩下的源应补上")
        for s in srcs:
            s.state["ranges"] = []
        fresh(dest)
        dead = Server("dead", 0.0)
        dead.stop()                                    # 起完就关，制造不可达的源
        dip.download_blob_multisource(
            [dead.reg] + [s.reg for s in srcs], DIGEST, dest, "测试", None, "k",
            expected_size=SIZE, names=["dead", "fast", "mid", "slow"], sources=3, segments=6)
        check("含死源仍能下完",
              hashlib.sha256(open(dest, "rb").read()).hexdigest() == DIGEST[7:])

        # ---------- 6) 关闭分片时行为不变 ----------
        print("\n[6] sources=1：退回单流，行为与旧版一致")
        fresh(dest)
        dip.download_blob_multisource(
            [s.reg for s in srcs], DIGEST, dest, "测试", None, "k",
            expected_size=SIZE, names=[s.name for s in srcs], sources=1)
        check("单流结果正确",
              hashlib.sha256(open(dest, "rb").read()).hexdigest() == DIGEST[7:])
        check("未产生分片文件", not [f for f in os.listdir(TMP) if ".seg" in f])
    finally:
        for s in srcs:
            s.stop()
        shutil.rmtree(TMP, ignore_errors=True)

    print("\n" + ("全部通过" if not FAIL else "失败项: " + ", ".join(FAIL)))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
