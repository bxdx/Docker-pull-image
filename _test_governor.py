"""动态源池（SourceGovernor）的确定性测试。

本机到公网镜像站的链路极不稳，没法拿它复现「源变慢 / 源复活 / 源掉线」这些场景。
这里在本地起几台**速率可控、支持 Range** 的假 registry，把源池的行为钉死：

  1. 慢源被主动关闭       —— 明显拖后腿的源应当被暂停，且不再接新活
  2. 分片不丢             —— 被关闭时已下的字节保留，由健康源从断点续传
  3. 池大于并发上限       —— 慢源下线后，池里没上场的快源应被主动启用
  4. 最后一个源不停       —— 只剩一个可用源时，再慢也不能停，否则会僵住
  5. 关掉源池行为不变     —— use_governor=False 退回静态分配，结果必须一致
  6. 单流「超时先暂停」   —— 某站超时后进冷却，换站续传而不是反复戳它

时间尺度被压到秒级（采样 0.25s / 判慢 2 拍 / 冷却 1s），行为不变，只是跑得快。
"""
import contextlib
import hashlib
import http.server
import importlib.util
import io
import os
import re
import shutil
import socket
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("dipmod", os.path.join(HERE, "dip.py"))
dip = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dip)

# ---- 把源池的时间尺度压到秒级（这些都是运行时读取的模块全局） ----
dip.GOV_SAMPLE_SEC = 0.25
dip.GOV_SLOW_STRIKES = 2
dip.GOV_STALL_STRIKES = 3
dip.GOV_PROBE_BYTES = 64 * 1024
dip.GOV_EMA_ALPHA = 0.6
dip.GOV_ERROR_LIMIT = 2

SIZE = 32 * 1024 * 1024
DATA = bytes((i * 7 + (i >> 8) * 13) & 0xFF for i in range(SIZE))
DIGEST = "sha256:" + hashlib.sha256(DATA).hexdigest()

SMALL = 8 * 1024 * 1024
DATA_S = DATA[:SMALL]
DIGEST_S = "sha256:" + hashlib.sha256(DATA_S).hexdigest()

REF = "test/blob"
TMP = os.path.join(HERE, "_tmp_gov")

FAST = 0.02      # 每个连接约 3.3 MB/s
SLOW = 0.25      # 每个连接约 260 KB/s
COOLDOWN = 1.0

FAIL = []


def check(name, ok, detail=""):
    print("  [%s] %-44s %s" % ("PASS" if ok else "FAIL", name, detail))
    if not ok:
        FAIL.append(name)


def make_handler(st, data, size):
    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _send(self, code, body):
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        def do_GET(self):
            if self.path.rstrip("/").endswith("/v2"):
                return self._send(200, b"{}")
            if "/blobs/" not in self.path:
                return self._send(404, b"{}")
            rng = self.headers.get("Range")
            if not rng or not st["range_ok"]:
                with st["lock"]:
                    st["full"] += 1
                    st["served"] += size
                return self._send(200, data)
            m = re.match(r"bytes=(\d+)-(\d*)", rng)
            s = int(m.group(1))
            e = min(int(m.group(2)) if m.group(2) else size - 1, size - 1)
            with st["lock"]:
                st["ranges"].append((s, e))
                st["served"] += e - s + 1
            self.send_response(206)
            self.send_header("Content-Range", "bytes %d-%d/%d" % (s, e, size))
            self.send_header("Content-Length", str(e - s + 1))
            self.end_headers()
            pos = s
            while pos <= e:
                if st["stall"]:
                    time.sleep(0.5)
                    continue
                n = min(64 * 1024, e - pos + 1)
                time.sleep(st["delay"] * (n / 65536.0))
                try:
                    self.wfile.write(data[pos:pos + n])
                except (BrokenPipeError, ConnectionResetError, OSError):
                    return
                pos += n

    return H


class Server:
    def __init__(self, name, delay=FAST, size=SIZE, data=None):
        self.name = name
        self.size = size
        self.state = {"ranges": [], "served": 0, "full": 0, "delay": delay,
                      "range_ok": True, "stall": False, "lock": threading.Lock()}
        self.httpd = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0), make_handler(self.state, data if data is not None else DATA, size))
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    @property
    def reg(self):
        return dip.Registry("http://127.0.0.1:%d" % self.port, REF, timeout=8.0, retries=1)

    @property
    def served(self):
        with self.state["lock"]:
            return self.state["served"]

    @property
    def ranges(self):
        with self.state["lock"]:
            return list(self.state["ranges"])

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class BlackHole:
    """收下连接就永不回包，用来制造「超时」这条路径。"""

    def __init__(self):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(16)
        self.port = self.sock.getsockname()[1]
        self.accepted = 0
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while True:
            try:
                c, _ = self.sock.accept()
            except OSError:
                return
            self.accepted += 1
            # 故意不关闭，也不回任何数据

    @property
    def reg(self):
        return dip.Registry("http://127.0.0.1:%d" % self.port, REF, timeout=1.0, retries=1)

    def stop(self):
        try:
            self.sock.close()
        except OSError:
            pass


def run_gov(srcs, sources, segments, size=SIZE, digest=DIGEST, **kw):
    logs = []
    lock = threading.Lock()

    def _log(m):
        with lock:
            logs.append(m)

    stats = {}
    dest = os.path.join(TMP, "blob_%d.bin" % int(time.time() * 1000 % 100000))
    kw.setdefault("min_rate", 64 * 1024)
    kw.setdefault("slow_ratio", 0.25)
    kw.setdefault("cooldown", COOLDOWN)
    n = dip.download_blob_multisource(
        [s.reg for s in srcs], digest, dest, "测试", None, "k",
        expected_size=size, names=[s.name for s in srcs],
        sources=sources, segments=segments, gov_log=_log, stats=stats, **kw)
    return n, dest, logs, stats


def sha_ok(path, digest):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest() == digest[7:]


def gov_of(stats):
    return (stats.get("governor") or {})


def src_row(stats, name):
    for r in gov_of(stats).get("sources") or []:
        if r["name"] == name:
            return r
    return {}


def cleanup(dest):
    for f in os.listdir(TMP):
        p = os.path.join(TMP, f)
        try:
            os.remove(p)
        except OSError:
            pass


def main():
    shutil.rmtree(TMP, ignore_errors=True)
    os.makedirs(TMP)
    print("样本 %s  digest %s" % (dip.human_bytes(SIZE), DIGEST[:26]))
    print("假 registry 速率: 快 ~%.1f MB/s, 慢 ~%.0f KB/s  采样 %.2fs / 冷却 %.1fs\n"
          % (65536 / FAST / dip.MB, 65536 / SLOW / 1024, dip.GOV_SAMPLE_SEC, COOLDOWN))

    try:
        # ---------- 1) 慢源被主动关闭 + 分片从断点续传 ----------
        # 放两个不同档位的慢源，分别踩中源池的两条判据：
        #   vslow ~256 KB/s  一个采样窗口都读不满 -> 走「无数据」停摆判据
        #   mslow ~655 KB/s  高于停摆线但低于「最快源 × 25%」-> 走比值判据
        print("[1] 慢源应被主动关闭（停摆判据 + 比值判据各验一次）")
        f1 = Server("fast1")
        vslow = Server("vslow", SLOW)
        mslow = Server("mslow", 0.1)
        f2 = Server("fast2")
        n, dest, logs, stats = run_gov([f1, vslow, mslow, f2], sources=3, segments=8)
        check("下完且 sha256 正确", n == SIZE and sha_ok(dest, DIGEST),
              "%s" % dip.human_bytes(n))
        paused = [m for m in logs if "已暂停" in m]
        check("极慢源被主动暂停", any("vslow" in m for m in paused),
              next((m for m in paused if "vslow" in m), "无"))
        ratio_pause = [m for m in paused if "mslow" in m and "低于阈值" in m]
        check("中等慢源按「低于阈值」被暂停（比值判据有效）", bool(ratio_pause),
              (ratio_pause[0] if ratio_pause else
               "无；实际暂停日志: %s" % (paused or "无")))
        check("源池报告里两个慢源都有暂停记录",
              src_row(stats, "vslow").get("pauses", 0) >= 1
              and src_row(stats, "mslow").get("pauses", 0) >= 1,
              "vslow=%s mslow=%s" % (src_row(stats, "vslow").get("pauses"),
                                     src_row(stats, "mslow").get("pauses")))
        seg_len = SIZE // 8
        mid = [(s, e) for sv in (f1, f2, vslow, mslow) for (s, e) in sv.ranges
               if s > 0 and s % seg_len != 0]
        check("存在片内续传（起点非片边界）", bool(mid),
              "如 %s" % (["%d" % s for s, _ in mid[:3]] or "无"))
        kills = (src_row(stats, "vslow").get("kills_inflight", 0)
                 + src_row(stats, "mslow").get("kills_inflight", 0))
        check("被主动关闭的在途传输有计数", kills >= 1,
              "vslow=%s mslow=%s" % (src_row(stats, "vslow").get("kills_inflight"),
                                     src_row(stats, "mslow").get("kills_inflight")))
        slow_sum = vslow.served + mslow.served
        fast_max = max(f1.served, f2.served)
        check("慢源搬的字节明显少于快源", slow_sum < fast_max,
              "慢源合计 %s vs 单快源 %s" % (dip.human_bytes(slow_sum),
                                            dip.human_bytes(fast_max)))
        for s in (f1, vslow, mslow, f2):
            s.stop()
        cleanup(dest)

        # ---------- 2) 池大于并发上限：慢源下线后启用池里的快源 ----------
        print("\n[2] 池中 4 站但只许 2 站在岗，慢源下线后应主动启用池里的快源")
        s1, s2 = Server("s1"), Server("s2_slow", SLOW)
        s3, s4 = Server("s3"), Server("s4")
        n, dest, logs, stats = run_gov([s1, s2, s3, s4], sources=2, segments=8)
        check("下完且 sha256 正确", n == SIZE and sha_ok(dest, DIGEST), "%s" % dip.human_bytes(n))
        g = gov_of(stats)
        check("池大小 4 / 同时在岗 2", g.get("pool_size") == 4 and g.get("max_active") == 2,
              "pool=%s active=%s" % (g.get("pool_size"), g.get("max_active")))
        prom = [m for m in logs if "已启用" in m]
        check("有源被主动启用（探测通过）", bool(prom), (prom[0] if prom else "无启用日志"))
        late = [sv.name for sv in (s3, s4) if sv.served > 0]
        check("池里原本没上场的快源确实干活了", bool(late), "参与: %s" % (",".join(late) or "无"))
        check("slow 被暂停", src_row(stats, "s2_slow").get("pauses", 0) >= 1,
              str(src_row(stats, "s2_slow").get("pauses")))
        check("无源被反复抖动（每站暂停 <= 2 次）",
              all((r.get("pauses") or 0) <= 2 for r in g.get("sources") or []),
              str({r["name"]: r.get("pauses") for r in g.get("sources") or []}))
        for s in (s1, s2, s3, s4):
            s.stop()
        cleanup(dest)

        # ---------- 3) 只剩一个可用源时不能被停 ----------
        print("\n[3] 最后一道保险：只剩一个可用源时，再慢也不许停")
        alive = Server("alive", 0.05, size=SMALL, data=DATA_S)
        dead = Server("dead")
        dead.stop()
        n, dest, logs, stats = run_gov([dead, alive], sources=2, segments=4,
                                       size=SMALL, digest=DIGEST_S)
        check("死源 + 慢源仍能下完", n == SMALL and sha_ok(dest, DIGEST_S),
              "%s" % dip.human_bytes(n))
        check("唯一的可用源没有被停", src_row(stats, "alive").get("pauses", 0) == 0,
              "pauses=%s" % src_row(stats, "alive").get("pauses"))
        check("死源被拿掉", src_row(stats, "dead").get("state") == "benched",
              str(src_row(stats, "dead").get("state")))
        alive.stop()
        cleanup(dest)

        # ---------- 4) 关掉源池，退回静态分配 ----------
        print("\n[4] use_governor=False：退回静态分配，结果必须一致")
        a, b, c = Server("a"), Server("b"), Server("c")
        n, dest, logs, stats = run_gov([a, b, c], sources=3, segments=6,
                                       use_governor=False)
        check("关闭源池后仍下完且正确", n == SIZE and sha_ok(dest, DIGEST),
              "%s" % dip.human_bytes(n))
        check("未建立源池", not gov_of(stats), str(list(stats.keys())))
        for s in (a, b, c):
            s.stop()
        cleanup(dest)

        # ---------- 5) 单流：超时就暂停该站 ----------
        print("\n[5] 单流：某站超时应被暂停并换站续传，而不是反复戳它")
        good = Server("good")
        bh = BlackHole()
        dest = os.path.join(TMP, "single.bin")
        buf = io.StringIO()
        t0 = time.time()
        with contextlib.redirect_stdout(buf):
            got = dip.download_blob([bh.reg, good.reg], DIGEST, dest, "测试", None, "k",
                                    expected_size=SIZE, names=["blackhole", "good"],
                                    max_retries=6, idle_timeout=1.0, resume_cooldown=1.0)
        out = buf.getvalue()
        check("绕开超时站后仍下完", got == SIZE and sha_ok(dest, DIGEST),
              "%s / %.1fs" % (dip.human_bytes(got), time.time() - t0))
        check("日志中出现「超时，暂停该站」", "超时，暂停该站" in out,
              (out.strip().splitlines() or [""])[0][:60])
        check("确实先从坏站开始试过", bh.accepted >= 1, "被连入 %d 次" % bh.accepted)
        check("没有对坏站无限重试（连入 <= 4 次）", bh.accepted <= 4,
              "%d 次" % bh.accepted)
        bh.stop()
        good.stop()
        cleanup(dest)
    finally:
        shutil.rmtree(TMP, ignore_errors=True)

    print("\n" + ("全部通过" if not FAIL else "失败项: " + ", ".join(FAIL)))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
