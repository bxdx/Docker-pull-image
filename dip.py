#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dip - Docker Image Puller
=========================

在**没有 docker / podman / skopeo** 的机器上，直接从 Docker Registry v2 HTTP API
下载镜像，自动挑选最快的镜像站，打包成 `docker load` 可直接导入的 `.tar.gz`，
并与官方/其它镜像站做密码学级别的完整性比对。

纯 Python 标准库实现，零第三方依赖，Python 3.8+ 可用。

子命令
------
  dip.py pull   IMAGE      下载镜像 -> tar.gz
  dip.py verify ARCHIVE    校验已有归档的完整性（可联网复核 digest）
  dip.py bench  [IMAGE]    只做镜像站测速排名，不下载
  dip.py mirrors           列出当前镜像站候选与配置来源

设计要点
--------
1. 选站：分三级 —— 握手延迟 -> manifest 可用性 -> 真实吞吐测速（Range 拉取样本）。
   延迟快不等于带宽大，所以最终以**实测吞吐**为准。
2. 完整性：三层校验，层层独立
     L1 传输校验：每个 blob 的 sha256 必须等于 manifest 里写的 digest（内容寻址）
     L2 解包校验：每层解压出的 tar 的 sha256 必须等于 config 的 rootfs.diff_ids[i]
     L3 官方比对：manifest digest 与官方源 / 其它镜像站交叉比对（见下）
3. 信任锚（trust anchor）优先级，报告里会明确写出用了哪一种：
     a) --expect-digest            用户给定的权威 digest（最强）
     b) 官方 registry 可达          与官方 manifest digest 逐一比对
     c) 多镜像站共识                >=2 个独立镜像站返回同一个 manifest digest
     d) 单站自洽                    仅内容寻址校验通过，风险等级下调并告警
4. 可复现：gzip mtime=0 + tar 成员 mtime/uid/gid/uname 归零，
   同样内容每次产出的 .tar.gz 字节完全一致，sha256 可对外公布。
5. 动态源池（SourceGovernor）：多源分片下载时，源不是静态分配给 worker 的，
   而是由一个后台监控线程持续测速、动态调度。它主动做两件事：
     关闭 —— 吞吐低于「最快源 × slow_ratio」或连续多个周期零字节，立即暂停该源
             并打断它在途的那段传输（分片保留，交给健康源续传）；
     启用 —— 冷却期满后用小样本 Range 探测，通了就重新上岗；出现空闲位子时
             也会去探测还没上场的站，所以「刚变可用的快源」会自己冒出来。
   单流下载同样适用「超时先暂停」：某站超时后进冷却期不再被选中。
   最后一道保险：永远不会把最后一个在岗的源也停掉。可用 --no-governor 关掉。
6. 保住进度（三条铁律，都是被真实事故逼出来的）：
     a) 令牌过期(401)不清盘。各站 token 有寿命（实测 1500s / 3600s），而 4.7GB
        的层在慢链路上要下近一小时 —— 必然跨过好几个 TTL。所以令牌按有效期
        提前刷新，401 也只重取凭证、从原地续传，绝不丢已下的字节。
     b) 不认 Range 的站不拿来续传。实测 dockerproxy.net 对任何 Range 请求都返回
        200 整份文件；它测速最快、最容易被选中，一旦用它续传就等于把已下的几百
        MB 全部截断重来。这类站在多源阶段会被预先剔除，在单流阶段会被跳过。
     c) 只有「重试而毫无进展」才消耗重试预算。令牌轮换、服务端按寿命掐断连接
        这类有进展的中断不该耗尽次数，否则大层永远下不完。
7. 代理按站决定：官方源和国外镜像站往往必须走代理，国内站走代理反而更慢。
   --proxy 只作用于官方源，--proxy-all 才作用于镜像站；mirrors.json 里每条站
   可写 proxy: true/false/"URL" 单独覆盖，实现「国内直连 + 国外走代理」。
   默认不读系统代理：本机 http_proxy 常指向死端口，隐式采用会把请求全带进沟里。
8. 失败分层重试，且预算按「连续无进展」而非「次数」算：单请求 -> 单 blob
   （默认 20 次）-> 整层补下（默认 2 轮）-> manifest 多轮跨站。几 GB 的层
   在时通时断的链路上，连续失败十几次仍应能下完；但只要每次能往前挪一点，
   预算就不消耗。只有 404 这类确定性错误不做重试。
"""

from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import hashlib
import http.client
import http.server
import io
import json
import os
import platform as _platform
import re
import shutil
import socket
import sys
import tarfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

__version__ = "1.5.0"
UA = "dip/%s (+docker-registry-v2-client)" % __version__

# --------------------------------------------------------------------------
# 常量
# --------------------------------------------------------------------------

MEDIA_INDEX = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
)
MEDIA_MANIFEST = (
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
)
MANIFEST_ACCEPT = ",".join(
    list(MEDIA_INDEX) + list(MEDIA_MANIFEST) + [
        "application/vnd.docker.distribution.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v1+prettyjws",
        "application/json",
    ]
)

RETRY_HTTP_CODES = (429, 500, 502, 503, 504)

# 失败预算：按「连续无进展」计，而不是按次数计。链路本来就会抽风，几次失败
# 不代表没救；只要还在进字节就该继续试。这几个值决定「什么时候才认输」。
DEFAULT_NO_PROGRESS = 20        # 连续 20 次毫无进展才放弃（旧值 8/10）
SEG_STALL_SECONDS = 240.0       # 或者连续 4 分钟一个新字节都没进
SEG_ATTEMPT_HARD_CAP = 400      # 纯防死循环的硬上限
LAYER_RETRY_ROUNDS = 2          # 整层失败后再补下几轮

DEFAULT_UPSTREAM = "https://registry-1.docker.io"

#: 内置镜像站候选。都被实测验证过能正常提供 Docker Hub 镜像。
DEFAULT_MIRRORS: List[Dict[str, Any]] = [
    {"name": "daocloud",    "url": "https://docker.m.daocloud.io"},
    {"name": "1ms",         "url": "https://docker.1ms.run"},
    {"name": "rat.dev",     "url": "https://hub.rat.dev"},
    {"name": "dockerproxy", "url": "https://dockerproxy.net"},
    {"name": "1panel",      "url": "https://docker.1panel.live"},
]

DOCKER_HUB_HOSTS = {"docker.io", "index.docker.io", "registry-1.docker.io", "registry.docker.io"}

MB = 1048576.0

# ---- 鉴权令牌 ----------------------------------------------------------
# 实测各站给的 TTL 并不一致：daocloud 3600s、1ms/rat.dev 1500s。旧实现把 token
# 无限缓存，于是过期之后每一个新请求都撞 401。而 4.7GB 的层在慢链路上要下近
# 一小时，必然跨过好几个 TTL —— 「下到一半令牌过期」于是成了必然事件，而不是
# 偶发。提前这么久就换新令牌，让它退化成一次普通的续传。
TOKEN_REFRESH_MARGIN = 120.0      # 距到期还有这么久就提前换新令牌
TOKEN_DEFAULT_TTL = 300.0         # 服务端没给 expires_in 时的保守假设


# --------------------------------------------------------------------------
# 小工具
# --------------------------------------------------------------------------

def eprint(*a: Any) -> None:
    print(*a, file=sys.stderr, flush=True)


def human_bytes(n: Optional[float]) -> str:
    if n is None:
        return "?"
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024.0 or unit == "TiB":
            return ("%.0f %s" % (n, unit)) if unit == "B" else ("%.1f %s" % (n, unit))
        n /= 1024.0
    return "%.1f TiB" % n


def sha256_file(path: str, chunk: int = 1024 * 512) -> Tuple[str, int]:
    h = hashlib.sha256()
    total = 0
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
            total += len(b)
    return h.hexdigest(), total


def split_digest(digest: str) -> Tuple[str, str]:
    """'sha256:abc...' -> ('sha256', 'abc...')"""
    if ":" in digest:
        algo, _, hexpart = digest.partition(":")
        return algo, hexpart
    return "sha256", digest


def digest_of_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def is_digest(alg: str, data: bytes, expect_hex: str) -> bool:
    try:
        h = hashlib.new(alg)
    except ValueError:
        return False
    h.update(data)
    return h.hexdigest() == expect_hex


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


class DipError(RuntimeError):
    pass


class Cancelled(DipError):
    """用户主动取消。"""


# --------------------------------------------------------------------------
# 事件总线 + 取消令牌
#
# 命令行模式下事件处理器是 None，emit() 就是空操作，零开销；
# Web 界面注册处理器后，核心逻辑会把结构化进度推给它。
# --------------------------------------------------------------------------

_EVENT_HANDLER: Optional[Callable[[str, Dict[str, Any]], None]] = None
_CANCEL = threading.Event()


def set_event_handler(fn: Optional[Callable[[str, Dict[str, Any]], None]]) -> None:
    global _EVENT_HANDLER
    _EVENT_HANDLER = fn


def emit(event: str, **data: Any) -> None:
    h = _EVENT_HANDLER
    if h is None:
        return
    try:
        h(event, data)
    except Exception:  # noqa: BLE001  事件回调绝不能影响主流程
        pass


def request_cancel() -> None:
    _CANCEL.set()


def clear_cancel() -> None:
    _CANCEL.clear()


def check_cancel() -> None:
    if _CANCEL.is_set():
        raise Cancelled("已取消")


# --------------------------------------------------------------------------
# 引用解析
# --------------------------------------------------------------------------

class ImageRef:
    """解析镜像引用，拆成 registry / repo / tag / digest。"""

    def __init__(self, raw: str):
        self.raw = raw.strip()
        if not self.raw:
            raise DipError("镜像引用为空")
        s = self.raw
        self.digest: Optional[str] = None
        if "@" in s:
            s, _, dg = s.rpartition("@")
            dg = dg.strip()
            if not dg.startswith("sha256:") and re.fullmatch(r"[0-9a-f]{64}", dg):
                dg = "sha256:" + dg
            self.digest = dg
        # tag：最后一个冒号且其后不含 '/'
        self.tag = "latest"
        m = re.search(r":([^/:]+)$", s)
        if m and "/" not in m.group(1):
            self.tag = m.group(1)
            s = s[: m.start()]
        # registry
        first = s.split("/", 1)[0]
        if ("." in first or ":" in first or first == "localhost") and "/" in s:
            self.registry_host = first
            self.path = s.split("/", 1)[1]
        else:
            self.registry_host = "docker.io"
            self.path = s
        self.path = self.path.strip("/")
        if self.registry_host in DOCKER_HUB_HOSTS and "/" not in self.path:
            self.path = "library/" + self.path
        self.is_hub = self.registry_host in DOCKER_HUB_HOSTS

    @property
    def reference(self) -> str:
        """给 registry API 用的 reference（digest 优先于 tag）。"""
        return self.digest or self.tag

    @property
    def upstream_base(self) -> str:
        return "https://" + self.registry_host

    def repo_tag(self, default_tag: Optional[str] = None) -> str:
        """写进 manifest.json 的 RepoTags 的名字。"""
        tag = default_tag or self.tag
        if self.is_hub:
            name = self.path[8:] if self.path.startswith("library/") else self.path
        else:
            name = "%s/%s" % (self.registry_host, self.path)
        return "%s:%s" % (name, tag)

    def file_stem(self) -> str:
        base = self.path.rsplit("/", 1)[-1]
        return "%s-%s" % (base, re.sub(r"[^A-Za-z0-9_.-]", "_", self.tag))

    def __str__(self) -> str:
        return self.raw


# --------------------------------------------------------------------------
# Registry 客户端
# --------------------------------------------------------------------------

class Registry:
    """极简 Docker Registry v2 客户端：token 鉴权 + 重试 + 可选代理。"""

    def __init__(
        self,
        base: str,
        repo: Optional[str] = None,
        timeout: float = 60.0,
        retries: int = 3,
        proxy: Optional[str] = None,
    ):
        self.base = base.rstrip("/")
        self.repo = repo
        self.timeout = timeout
        self.retries = retries
        self._tokens: Dict[str, Tuple[Optional[str], float]] = {}
        self._lock = threading.Lock()
        # 该站是否支持 Range 断点续传。None=未知，False=实测无视 Range（返回整份文件）。
        # 实测 dockerproxy.net 属于后者：任何 Range 请求都回 200 而非 206。
        # 对它请求 Range 不但没用，还会让上层误以为能续传而把已下进度截断重来。
        self.range_ok: Optional[bool] = None
        handlers: List[Any] = []
        if proxy:
            handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        else:
            handlers.append(urllib.request.ProxyHandler({}))  # 显式不走系统代理
        self._opener = urllib.request.build_opener(*handlers)

    # -- 底层 ------------------------------------------------------------
    def _open(self, method: str, url: str, headers: Optional[Dict[str, str]] = None):
        req = urllib.request.Request(url, method=method)
        req.add_header("User-Agent", UA)
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        last: Optional[BaseException] = None
        for attempt in range(max(1, self.retries)):
            try:
                return self._opener.open(req, timeout=self.timeout)
            except urllib.error.HTTPError as e:
                last = e
                if e.code in RETRY_HTTP_CODES and attempt + 1 < self.retries:
                    time.sleep(min(2 ** attempt, 8))
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as e:
                last = e
                if attempt + 1 < self.retries:
                    time.sleep(min(2 ** attempt, 8))
                    continue
                raise
        raise last  # pragma: no cover

    # -- 鉴权 ------------------------------------------------------------
    def token(self, scope: str) -> Optional[str]:
        """取该 scope 的 token，**按有效期提前刷新**。

        token 是有寿命的（实测 1500s / 3600s 不等），而一个 4.7GB 的层要下近一小时。
        所以必须在到期前就换掉，否则每个新请求都会撞 401。
        """
        now = time.monotonic()
        with self._lock:
            ent = self._tokens.get(scope)
            if ent is not None and now < ent[1]:
                return ent[0]
        token, ttl = self._probe_token(scope)
        span = (float(ttl) - TOKEN_REFRESH_MARGIN) if ttl else TOKEN_DEFAULT_TTL
        expire_at = now + max(5.0, span)   # 至少留 5 秒，避免 TTL 极小时疯狂刷新
        with self._lock:
            self._tokens[scope] = (token, expire_at)
        return token

    def _probe_token(self, scope: str) -> Tuple[Optional[str], Optional[float]]:
        """探测鉴权方式并取 token。返回 (token, expires_in)。"""
        try:
            r = self._open("GET", self.base + "/v2/")
            r.read()
            return None, None              # 匿名可访问，不用 token
        except urllib.error.HTTPError as e:
            if e.code != 401:
                raise
            challenge = e.headers.get("WWW-Authenticate", "") or ""
            if not challenge.lower().startswith("bearer"):
                return None, None
            return self._fetch_token(challenge, scope)
        except Exception:  # noqa: BLE001
            return None, None

    def _fetch_token(self, challenge: str, scope: str) -> Tuple[Optional[str], Optional[float]]:
        realm = re.search(r'realm="([^"]+)"', challenge)
        service = re.search(r'service="([^"]+)"', challenge)
        if not realm:
            return None, None
        url = realm.group(1)
        params = []
        if service:
            params.append("service=" + urllib.parse.quote(service.group(1)))
        params.append("scope=" + urllib.parse.quote(scope))
        sep = "&" if "?" in url else "?"
        try:
            resp = self._open("GET", url + sep + "&".join(params))
            data = json.loads(resp.read().decode("utf-8", "replace"))
        except Exception:  # noqa: BLE001
            return None, None              # 拿不到 token 就当匿名试一次
        ttl = data.get("expires_in")
        try:
            ttl = float(ttl) if ttl is not None else None
        except (TypeError, ValueError):
            ttl = None
        return (data.get("token") or data.get("access_token")), ttl

    def refresh_token(self, scope: Optional[str] = None) -> None:
        """丢掉缓存的 token，下次请求会重新取一个。"""
        with self._lock:
            if scope is None:
                self._tokens.clear()
            else:
                self._tokens.pop(scope, None)

    def clear_tokens(self) -> None:
        """兼容旧调用点：清空全部 token。"""
        self.refresh_token()

    def check_range_support(self, digest: str) -> bool:
        """实测该站是否真的按 Range 返回 206（结果缓存，只测一次）。

        这件事必须用**真实字节**去试，不能靠猜：dockerproxy.net 对任何 Range
        请求都回 200 整份文件，如果不试出来，上层会以为能从中间续传，于是把
        已经下好的几百 MB 截断重来。
        """
        if self.range_ok is not None:
            return self.range_ok
        try:
            resp = self.blob_response(digest, 0, RANGE_PROBE_BYTES)
            ok = (resp.status == 206)
            try:
                resp.read(RANGE_PROBE_BYTES)
            except Exception:  # noqa: BLE001
                pass
            try:
                resp.close()
            except Exception:  # noqa: BLE001
                pass
        except RangeUnsupported:
            ok = False
        except Exception:  # noqa: BLE001
            return self.range_ok is not False      # 网络问题不代表不支持，下次再判
        self.range_ok = ok
        return ok

    # -- 高层 ------------------------------------------------------------
    def _get(self, path: str, extra_headers: Optional[Dict[str, str]] = None,
             scope: Optional[str] = None):
        if not self.repo:
            raise DipError("Registry 未绑定 repo")
        scope = scope or ("repository:%s:pull" % self.repo)
        headers = dict(extra_headers or {})
        url = "%s/v2/%s/%s" % (self.base, self.repo, path)
        token = self.token(scope)
        if token:
            headers["Authorization"] = "Bearer " + token
        try:
            return self._open("GET", url, headers)
        except urllib.error.HTTPError as e:
            if e.code != 401:
                raise
            # 令牌过期/失效：刷一个新的，然后**原样重试同一个请求**。
            # 关键在于这里就把 401 消化掉 —— 绝不让它冒到下载层去，
            # 否则下载层会误判为「这份数据不能用了」而清盘重下。
            self.refresh_token(scope)
            headers.pop("Authorization", None)
            token = self.token(scope)
            if token:
                headers["Authorization"] = "Bearer " + token
            return self._open("GET", url, headers)

    def fetch_manifest(self, ref: str, retries: int = 2) -> Dict[str, Any]:
        """返回 {'bytes','json','media_type','digest_header','digest_computed'}"""
        last: Optional[BaseException] = None
        for attempt in range(max(1, retries)):
            try:
                resp = self._get("manifests/" + ref, {"Accept": MANIFEST_ACCEPT})
                body = resp.read()
                ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
                hdr = resp.headers.get("Docker-Content-Digest") or ""
                try:
                    doc = json.loads(body.decode("utf-8", "replace"))
                except ValueError as e:
                    raise DipError("manifest 不是合法 JSON: %s" % e)
                return {
                    "bytes": body,
                    "json": doc,
                    "media_type": ctype,
                    "digest_header": hdr.strip(),
                    "digest_computed": digest_of_bytes(body),
                }
            except urllib.error.HTTPError as e:
                last = e
                if e.code == 401:
                    self.clear_tokens()
                if e.code in RETRY_HTTP_CODES and attempt + 1 < retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as e:
                last = e
                if attempt + 1 < retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise
        raise last  # pragma: no cover

    def blob_response(self, digest: str, start: int = 0, length: Optional[int] = None):
        headers: Dict[str, str] = {}
        if length:
            headers["Range"] = "bytes=%d-%d" % (start, start + length - 1)
        elif start:
            headers["Range"] = "bytes=%d-" % start
        resp = self._get("blobs/" + digest, headers)
        if "Range" in headers and resp.status == 200:
            # 站方无视 Range，把整份文件塞了回来。这里必须把话说清楚：
            # 上层若把它当成「从 start 开始的响应」，就会把已经下好的几百 MB
            # 截断重来 —— 实测 dockerproxy.net 正是这种站，且它测速最快、
            # 最容易被选中，于是「下一点就清零」反复发生。
            self.range_ok = False
            try:
                resp.close()
            except Exception:  # noqa: BLE001
                pass
            raise RangeUnsupported("该站无视 Range，返回 200 整份文件（无法断点续传）")
        if "Range" in headers and resp.status == 206:
            self.range_ok = True
        return resp


def is_index(doc: Dict[str, Any], media_type: str) -> bool:
    if media_type in MEDIA_INDEX:
        return True
    if media_type in MEDIA_MANIFEST:
        return False
    return "manifests" in doc and "layers" not in doc


def pick_platform(doc: Dict[str, Any], os_: str, arch: str, variant: Optional[str]) -> Dict[str, Any]:
    """从 manifest list / OCI index 里挑出目标平台的条目。"""
    entries = doc.get("manifests") or []
    exact, loose = [], []
    for m in entries:
        p = m.get("platform") or {}
        if p.get("os") != os_ or p.get("architecture") != arch:
            continue
        pv = p.get("variant") or ""
        if variant:
            (exact if pv == variant else loose).append(m)
        else:
            (exact if not pv else loose).append(m)
    # 过滤掉 attestation / 非镜像条目
    def usable(m: Dict[str, Any]) -> bool:
        p = m.get("platform") or {}
        return p.get("os") != "unknown" and (m.get("annotations") or {}).get(
            "vnd.docker.reference.type") != "attestation-manifest"
    exact = [m for m in exact if usable(m)] or [m for m in exact]
    loose = [m for m in loose if usable(m)] or [m for m in loose]
    if exact or loose:
        return (exact or loose)[0]
    avail = sorted({
        "%s/%s%s" % ((m.get("platform") or {}).get("os"), (m.get("platform") or {}).get("architecture"),
                     "/" + (m.get("platform") or {}).get("variant", "") if (m.get("platform") or {}).get("variant") else "")
        for m in entries if (m.get("platform") or {}).get("os")
    })
    raise DipError("该镜像没有 %s/%s%s 平台，可选：[%s]" % (
        os_, arch, "/" + variant if variant else "", ", ".join(avail)))


# --------------------------------------------------------------------------
# 进度显示
# --------------------------------------------------------------------------

class Progress:
    """线程安全的进度输出：TTY 下多行实时刷新，管道下只打关键行。"""

    def __init__(self, enabled: bool = True, interval: float = 5.0):
        self.enabled = enabled
        self.tty = bool(getattr(sys.stdout, "isatty", lambda: False)())
        self.interval = interval
        self._lock = threading.Lock()
        self._rows: Dict[str, List[Any]] = {}
        self._order: List[str] = []
        self._last = 0.0
        self._drawn = 0
        self._last_line = 0.0
        self._dirty = False

    def add(self, key: str, label: str, total: Optional[int] = None) -> None:
        if not self.enabled:
            return
        with self._lock:
            if key in self._rows:          # 续传/重试：不要重置已累计的字节数
                self._rows[key][0] = label
                if total:
                    self._rows[key][2] = total
                return
            self._order.append(key)
            self._rows[key] = [label, 0, total or 0, time.time()]
            if not self.tty:
                print("  · %s" % label, flush=True)
            self._dirty = True
            self._emit_locked()

    def update(self, key: str, delta: int) -> None:
        if not self.enabled:
            return
        with self._lock:
            row = self._rows.get(key)
            if not row:
                return
            row[1] += delta
            row[3] = time.time()
            self._maybe_draw_locked()

    def set_done(self, key: str, done: int) -> None:
        """重试/续传后把计数对齐到磁盘上的真实字节数，避免重复累加。"""
        if not self.enabled:
            return
        with self._lock:
            row = self._rows.get(key)
            if row:
                row[1] = done

    def relabel(self, key: str, label: str) -> None:
        if not self.enabled:
            return
        with self._lock:
            row = self._rows.get(key)
            if row:
                row[0] = label

    def total_snapshot(self) -> Tuple[int, int]:
        with self._lock:
            done = sum(r[1] for r in self._rows.values())
            total = sum(r[2] for r in self._rows.values())
        return done, total

    def _snapshot_locked(self) -> List[Dict[str, Any]]:
        return [{"key": k, "label": self._rows[k][0], "done": self._rows[k][1],
                 "total": self._rows[k][2]} for k in self._order]

    def _emit_locked(self) -> None:
        emit("progress", rows=self._snapshot_locked())

    def _maybe_draw_locked(self) -> None:
        now = time.time()
        if now - self._last < 0.4:
            return
        self._last = now
        self._emit_locked()
        if self.tty:
            self._draw_locked()
        else:
            if now - self._last_line >= self.interval:
                self._last_line = now
                done = sum(r[1] for r in self._rows.values())
                total = sum(r[2] for r in self._rows.values())
                pct = ("%.1f%%" % (done * 100.0 / total)) if total else "?"
                print("    ... 已下载 %s%s" % (human_bytes(done),
                      (" / %s (%s)" % (human_bytes(total), pct)) if total else ""), flush=True)

    def _draw_locked(self) -> None:
        self._clear_locked()
        out = []
        for k in self._order:
            label, done, total, _ = self._rows[k]
            bar_w = 24
            if total:
                frac = min(1.0, done / float(total))
                filled = int(frac * bar_w)
                bar = "#" * filled + "-" * (bar_w - filled)
                out.append("  %-24s [%s] %6.1f%%  %s/%s" % (
                    label[:24], bar, frac * 100, human_bytes(done), human_bytes(total)))
            else:
                out.append("  %-24s %s" % (label[:24], human_bytes(done)))
        sys.stdout.write("\n".join(out) + "\n")
        sys.stdout.flush()
        self._drawn = len(out)

    def _clear_locked(self) -> None:
        """擦掉 TTY 下已绘制的多行进度块。"""
        if not self.tty or not self._drawn:
            return
        sys.stdout.write("\x1b[%dA" % self._drawn)
        for _ in range(self._drawn):
            sys.stdout.write("\r\x1b[K\x1b[1B")
        sys.stdout.write("\x1b[%dA" % self._drawn)
        sys.stdout.flush()
        self._drawn = 0

    def finish(self, key: str, note: str = "") -> None:
        if not self.enabled:
            return
        with self._lock:
            row = self._rows.pop(key, None)
            try:
                self._order.remove(key)
            except ValueError:
                pass
            self._clear_locked()
        if row:
            print("  ✓ %-28s %10s %s" % (row[0], human_bytes(row[1]), note), flush=True)
        with self._lock:
            self._emit_locked()

    def close(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            if self.tty:
                self._clear_locked()
            # 任务收尾时把残留进度行清空，避免前端留一条永远不动的进度条
            self._rows.clear()
            self._order.clear()
            self._emit_locked()


# --------------------------------------------------------------------------
# 镜像站发现 / 测速
# --------------------------------------------------------------------------

class Candidate:
    def __init__(self, name: str, url: str, official: bool = False,
                 proxy: Optional[str] = None, proxy_flag: Optional[bool] = None):
        self.name = name
        self.url = url
        self.official = official
        # 配置里的 proxy 字段：具体 URL 表示这条链接走它；true 表示走命令行/环境
        # 里的代理；false 表示永不走代理（内网站常见）；缺省则跟随全局设置。
        self.proxy = proxy
        self.proxy_flag = proxy_flag
        self.effective_proxy: Optional[str] = None
        self.rtt: Optional[float] = None
        self.error: Optional[str] = None
        self.capable = False
        self.speed: Optional[float] = None       # bytes/s
        self.manifest: Optional[Dict[str, Any]] = None
        self.manifest_digest: Optional[str] = None
        self.platform_digest: str = ""

    def __repr__(self) -> str:
        return "<Candidate %s %s>" % (self.name, self.url)


def load_mirrors(path: Optional[str], extra: Sequence[str], no_builtin: bool) -> List[Candidate]:
    """优先级：命令行 --mirror > 配置文件 > 内置默认。URL 去重。"""
    cands: List[Candidate] = []
    seen = set()

    def push(name: str, url: str, official: bool = False,
             proxy: Optional[str] = None, proxy_flag: Optional[bool] = None) -> None:
        url = url.rstrip("/")
        if url in seen:
            return
        seen.add(url)
        cands.append(Candidate(name, url, official, proxy, proxy_flag))

    # 1) 命令行（优先级最高）
    for u in extra:
        push(u, u)
    # 2) 配置文件
    cfg_path = path
    if cfg_path is None:
        for probe in ("./mirrors.json", os.path.join(os.path.dirname(os.path.abspath(__file__)), "mirrors.json")):
            if os.path.exists(probe):
                cfg_path = probe
                break
    if cfg_path:
        if not os.path.exists(cfg_path):
            raise DipError("镜像站配置文件不存在: %s" % cfg_path)
        with open(cfg_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        items = data.get("mirrors") if isinstance(data, dict) else data
        if not isinstance(items, list):
            raise DipError("镜像站配置格式错误，应为 {\"mirrors\":[{\"name\",\"url\"}]} 或直接的数组")
        for it in items:
            if isinstance(it, str):
                push(it, it)
            elif isinstance(it, dict) and it.get("url"):
                if it.get("enabled", True) is False:
                    continue
                pv = it.get("proxy")
                purl = pv if isinstance(pv, str) and pv else None
                pflag = pv if isinstance(pv, bool) else None
                push(it.get("name") or it["url"], it["url"],
                     bool(it.get("official")), purl, pflag)
        if isinstance(data, dict) and data.get("upstream"):
            push("upstream", data["upstream"], True)
    # 3) 内置默认
    if not no_builtin:
        for m in DEFAULT_MIRRORS:
            push(m["name"], m["url"])
    if not cands:
        raise DipError("镜像站候选为空")
    return cands


def env_proxy() -> Optional[str]:
    """从环境变量取代理。http_proxy 常被设成一个不通的地址，所以只在显式
    要求时才读（--proxy-from-env 或 --proxy env），绝不隐式使用。"""
    for k in ("https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY",
              "all_proxy", "ALL_PROXY"):
        v = os.environ.get(k)
        if v and v.strip():
            return v.strip()
    return None


def apply_candidate_proxy(cands: Sequence[Candidate], base_proxy: Optional[str],
                          available_proxy: Optional[str] = None) -> None:
    """按站点解析出各自要用的代理。

    base_proxy       默认给镜像站用的代理（未加 --proxy-all 时为 None）
    available_proxy  用户显式给出的代理，供配置里写 "proxy": true 的站单独启用
    """
    for c in cands:
        if c.official:                    # 官方源始终用显式代理（正是为了翻墙）
            c.effective_proxy = base_proxy or available_proxy or c.proxy
        elif c.proxy:                     # 配置里写死了 URL
            c.effective_proxy = c.proxy
        elif c.proxy_flag is False:       # 明确不走代理
            c.effective_proxy = None
        elif c.proxy_flag is True:        # 明确要代理：用 --proxy，再退化到环境变量
            c.effective_proxy = base_proxy or available_proxy or env_proxy()
        else:
            c.effective_proxy = base_proxy


def handshake(c: Candidate, timeout: float) -> None:
    """探测 /v2/ 可达性与握手延迟（取两次里较快的一次）。"""
    best: Optional[float] = None
    last_err: Optional[str] = None
    for _ in range(2):
        t0 = time.perf_counter()
        try:
            reg = Registry(c.url, timeout=timeout, retries=1, proxy=c.effective_proxy)
            try:
                r = reg._open("GET", c.url + "/v2/")
                r.read()
            except urllib.error.HTTPError as e:
                if e.code not in (401, 403, 200):
                    raise
                e.read()
            dt = time.perf_counter() - t0
            best = dt if best is None else min(best, dt)
        except Exception as e:  # noqa: BLE001
            last_err = "%s%s" % (type(e).__name__, ("(%s)" % getattr(e, "code", "")) if hasattr(e, "code") else "")
            if best is not None:
                break
    c.rtt = best
    c.error = None if best is not None else (last_err or "unreachable")


def qualify(c: Candidate, image: ImageRef, remote_ref: str,
            os_: str, arch: str, variant: Optional[str], timeout: float,
            proxy: Optional[str]) -> None:
    """拉一次 manifest，确认这个镜像站真的提供服务，并记录 digest。"""
    try:
        reg = Registry(c.url, image.path, timeout=timeout, retries=1, proxy=c.effective_proxy)
        first = reg.fetch_manifest(remote_ref, retries=1)
        doc, mt = first["json"], first["media_type"]
        plat_digest = ""
        if is_index(doc, mt):
            entry = pick_platform(doc, os_, arch, variant)
            plat_digest = entry.get("digest", "")
        c.manifest = first
        c.manifest_digest = first["digest_header"] or first["digest_computed"]
        c.platform_digest = plat_digest
        c.capable = True
        c.error = None
    except Exception as e:  # noqa: BLE001
        c.capable = False
        code = getattr(e, "code", "")
        c.error = "%s%s" % (type(e).__name__, ("(%s)" % code) if code else "")
        detail = getattr(e, "reason", "") or ""
        if code == 404:
            c.error += " 该站无此镜像"
        elif detail:
            c.error += " " + str(detail)[:40]


def bench(c: Candidate, image: ImageRef, probe_bytes: int, proxy: Optional[str],
          timeout: float, budget: float = 20.0) -> None:
    """Range 拉一小段真实层数据，实测吞吐（bytes/s）。

    必须**串行**执行：并发测速会互相抢带宽，测出来的数字是假的。
    每次尝试都新建连接（含 TLS 握手），因为很多镜像站会掐掉空闲/复用连接。
    """
    mf = c.manifest["json"]
    mt = c.manifest["media_type"]
    try:
        if is_index(mf, mt):
            if not c.platform_digest:
                c.speed = 0.0
                return
            reg = Registry(c.url, image.path, timeout=timeout, retries=1, proxy=c.effective_proxy)
            layers = reg.fetch_manifest(c.platform_digest, retries=1)["json"].get("layers") or []
        else:
            layers = mf.get("layers") or []
    except Exception as e:  # noqa: BLE001
        c.error = "测速失败: %s" % type(e).__name__
        c.speed = 0.0
        return
    if not layers:
        c.speed = 0.0
        return

    target = max(layers, key=lambda l: l.get("size") or 0)
    digest = target.get("digest")
    size = int(target.get("size") or probe_bytes)
    # 采样量下限：小样本会被内核缓冲摊平成假速度，见 BENCH_MIN_BYTES 注释
    want = min(max(probe_bytes, BENCH_MIN_BYTES), size)
    deadline = time.monotonic() + budget
    best = 0.0
    samples: List[float] = []
    for attempt in range(4):  # 链接不稳，多试几次取最好值
        if time.monotonic() >= deadline:
            break
        try:
            reg = Registry(c.url, image.path, timeout=timeout, retries=1, proxy=c.effective_proxy)
            resp = reg.blob_response(digest, 0, want)
            got = 0
            t_first: Optional[float] = None
            t_last: Optional[float] = None
            while got < want:
                if time.monotonic() >= deadline:
                    break
                chunk = resp.read(min(65536, want - got))
                if not chunk:
                    break
                if t_first is None:
                    t_first = time.perf_counter()  # 从第一个字节开始计时
                t_last = time.perf_counter()
                got += len(chunk)
            resp.close()
            # 只统计真正在传数据的窗口：首字节 -> 最后一个字节。
            # 连接建立耗时和尾部停滞都不算进带宽，否则测出来的是"卡了多久"而不是"有多快"。
            if t_first is not None and t_last is not None and got >= want * 0.25:
                dt = max(1e-6, t_last - t_first)
                rate = got / dt
                if dt < BENCH_MIN_SECONDS or rate > BENCH_MAX_PLAUSIBLE:
                    # 数据早就在内核缓冲里等着，计时窗口被摊平 —— 这个数字是
                    # 假的，绝不能拿去选站或喂给源池。放大样本重测一次。
                    if want < min(BENCH_RETRY_MAX_BYTES, size):
                        want = min(want * 4, BENCH_RETRY_MAX_BYTES, size)
                        c.error = "样本被缓冲摊平(%s)，放大到 %s 重测" % (
                            _rate_str(rate), human_bytes(want))
                        continue
                    c.error = "样本被缓冲摊平(%s)，无法测得可信吞吐" % _rate_str(rate)
                    continue
                samples.append(rate)
                best = max(best, rate)
        except Exception as e:  # noqa: BLE001
            c.error = "测速失败: %s" % type(e).__name__
    c.speed = best
    c.speed_samples = samples


def cand_view(c: Candidate, selected: bool = False) -> Dict[str, Any]:
    return {"name": c.name, "url": c.url, "rtt": c.rtt, "speed": c.speed,
            "capable": c.capable, "error": c.error, "selected": selected,
            "digest": c.manifest_digest}


def select_fastest(
    mirrors: List[Candidate],
    image: ImageRef,
    remote_ref: str,
    os_: str,
    arch: str,
    variant: Optional[str],
    top_k: int,
    probe_bytes: int,
    probe_timeout: float,
    proxy: Optional[str],
    jobs: int,
    bench_budget: float = 20.0,
    verbose: bool = True,
) -> Tuple[Candidate, List[Candidate]]:
    """三级选站：握手 -> 可用性 -> 吞吐。返回 (winner, capable列表)"""
    if verbose:
        print("[1/5] 探测镜像站连通性  (%d 个候选)" % len(mirrors))
    emit("stage", name="探测镜像站连通性", step=1, total_steps=5)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(jobs, len(mirrors)))) as ex:
        list(ex.map(lambda c: handshake(c, probe_timeout), mirrors))
    alive = [c for c in mirrors if c.rtt is not None]
    emit("mirrors", rows=[cand_view(c) for c in mirrors])
    if verbose:
        for c in mirrors:
            if c.rtt is None:
                print("      %-12s %-34s %s" % (c.name, c.url, "不可达: " + (c.error or "")))
            else:
                print("      %-12s %-34s %6.0f ms" % (c.name, c.url, c.rtt * 1000))
    if not alive:
        raise DipError("所有镜像站都不可达。可用 --mirror 指定，或检查网络/代理。")

    if verbose:
        print("[2/5] 校验各站能否提供 %s" % image)
    emit("stage", name="校验镜像可用性", step=2, total_steps=5)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(jobs, len(alive)))) as ex:
        list(ex.map(lambda c: qualify(c, image, remote_ref, os_, arch, variant, probe_timeout * 2.5, proxy), alive))
    capable = [c for c in alive if c.capable]
    emit("mirrors", rows=[cand_view(c) for c in mirrors])
    if verbose:
        for c in alive:
            if c.capable:
                print("      %-12s OK   manifest %s" % (c.name, (c.manifest_digest or "")[:26]))
            else:
                print("      %-12s 不可用: %s" % (c.name, c.error))
    if not capable:
        detail = "; ".join("%s(%s)" % (c.name, c.error) for c in alive)
        raise DipError("没有镜像站能提供该镜像: %s" % detail)

    ranked = sorted(capable, key=lambda c: c.rtt or 9e9)
    finalists = ranked[: max(1, min(top_k, len(ranked)))]
    if verbose:
        print("[3/5] 实测下载速度  (各站拉取 %s 真实层数据, 串行)" % human_bytes(probe_bytes))
    emit("stage", name="实测下载速度", step=3, total_steps=5)
    if len(finalists) == 1:
        finalists[0].speed = 0.0
    else:
        # 串行：并发测速会互相抢带宽，结果不可信
        for c in finalists:
            check_cancel()
            bench(c, image, probe_bytes, proxy, max(5.0, probe_timeout), budget=bench_budget)
            emit("mirrors", rows=[cand_view(x, x is c) for x in mirrors])
    measured = [c for c in finalists if (c.speed or 0) > 0]
    fastest = max(measured, key=lambda c: c.speed or 0.0) if measured else ranked[0]
    emit("mirrors", rows=[cand_view(c, c is fastest) for c in mirrors])
    emit("speedtest", ok=bool(measured),
         note="" if measured else "所有镜像站测速均失败，已降级为按延迟排序挑选")
    if verbose:
        for c in finalists:
            sp = c.speed or 0.0
            extra = ("  [%s]" % c.error) if sp <= 0 and c.error else ""
            print("      %-12s %s%s" % (c.name,
                  ("%.2f MB/s" % (sp / MB)) if sp > 0 else "测速失败, 按延迟排序", extra))
        print("      => 采用最快镜像站: %s (%s)%s"
              % (fastest.name, fastest.url, "" if measured else "  [降级: 无有效测速结果]"))
    return fastest, capable


# --------------------------------------------------------------------------
# 信任锚 / 官方比对
# --------------------------------------------------------------------------

def trust_check(
    image: ImageRef,
    remote_ref: str,
    capable: List[Candidate],
    upstream_url: str,
    proxy: Optional[str],
    expect_digest: Optional[str],
    os_: str,
    arch: str,
    variant: Optional[str],
    timeout: float,
    verbose: bool = True,
) -> Dict[str, Any]:
    """确定「官方真值」并与各镜像站比对，返回 trust 信息字典。

    信任锚优先级（报告里会写明用了哪一种）：
      1. 用户给的 --expect-digest      —— 最强，直接作为唯一真值
      2. 官方 registry 可达             —— 与官方 manifest digest 逐一比对
      3. 多个独立镜像站 digest 一致      —— 交叉验证，256bit 摘要同时吻合极难伪造
      4. 只有单站可用                   —— 仅能自洽，置信度降为 low 并告警
    """
    if verbose:
        print("[4/5] 完整性信任锚比对")
    emit("stage", name="完整性信任锚比对", step=4, total_steps=5)

    agreement: Dict[str, List[str]] = {}
    for c in capable:
        agreement.setdefault(c.manifest_digest or "?", []).append(c.name)

    info: Dict[str, Any] = {
        "official_registry": {"url": upstream_url, "reachable": False, "error": None},
        "agreement": agreement,
        "mirrors_compared": [c.name for c in capable],
        "notes": [],
    }
    _trust_decide(image, remote_ref, capable, upstream_url, proxy, expect_digest,
                  timeout, info, agreement, verbose)
    emit("trust", info=info)
    return info


def _trust_decide(
    image: ImageRef,
    remote_ref: str,
    capable: List[Candidate],
    upstream_url: str,
    proxy: Optional[str],
    expect_digest: Optional[str],
    timeout: float,
    info: Dict[str, Any],
    agreement: Dict[str, List[str]],
    verbose: bool,
) -> None:
    """把结论写进 info（basis / confidence / expected_digest / notes）。"""

    # ---- (a) 用户显式给定的权威 digest：最高优先级 ----
    if expect_digest:
        want = expect_digest.strip()
        if not want.startswith("sha256:") and re.fullmatch(r"[0-9a-f]{64}", want):
            want = "sha256:" + want
        matched = [c.name for c in capable if (c.manifest_digest or "") == want]
        info["basis"] = "user-provided-digest"
        info["expected_digest"] = want
        info["confidence"] = "high" if matched else "low"
        info["matched_mirrors"] = matched
        if not matched:
            info["notes"].append(
                "没有任何镜像站的 manifest digest 等于 --expect-digest，"
                "可能镜像已更新，或该站内容被篡改")
        if verbose:
            print("      信任锚: 用户指定 digest %s" % want[:26])
            print("      匹配站: %s" % (", ".join(matched) if matched else "无 !!"))
        return

    # ---- (b) 官方 registry 可达：直接与官方比对 ----
    up_proxy = proxy
    official_digest: Optional[str] = None
    official_err: Optional[str] = None
    try:
        ureg = Registry(upstream_url, image.path, timeout=timeout, retries=1, proxy=up_proxy)
        ufirst = ureg.fetch_manifest(remote_ref, retries=1)
        official_digest = ufirst["digest_header"] or ufirst["digest_computed"]
    except Exception as e:  # noqa: BLE001
        code = getattr(e, "code", "")
        official_err = "%s%s" % (type(e).__name__, ("(%s)" % code) if code else "")

    if official_digest:
        info["official_registry"]["reachable"] = True
        info["official_registry"]["manifest_digest"] = official_digest
        matched = [c.name for c in capable if (c.manifest_digest or "") == official_digest]
        info["basis"] = "official-registry"
        info["confidence"] = "high" if matched else "low"
        info["matched_mirrors"] = matched
        info["expected_digest"] = official_digest
        if not matched:
            info["notes"].append("镜像站的 manifest digest 与官方不一致！高度可疑")
        if verbose:
            print("      官方源可达: %s" % official_digest[:26])
            print("      与官方一致: %s" % (", ".join(matched) if matched else "无  <<< 不一致，可疑！"))
        return

    info["official_registry"]["reachable"] = False
    info["official_registry"]["error"] = official_err
    info["notes"].append("官方 registry 不可达：%s" % official_err)

    # ---- (c) 跨镜像站共识 ----
    top = sorted(agreement.items(), key=lambda kv: -len(kv[1]))
    best_digest, best_names = top[0]
    info["consensus_digest"] = best_digest
    info["consensus_count"] = len(best_names)
    info["expected_digest"] = best_digest

    if len(best_names) >= 2:
        info["basis"] = "cross-mirror-consensus"
        info["confidence"] = "high" if len(best_names) >= 3 else "medium"
        if len(agreement) > 1:
            info["notes"].append("存在 digest 不一致的镜像站: %s"
                                 % {d: n for d, n in agreement.items() if d != best_digest})
        if verbose:
            print("      官方源不可达，改用跨站共识")
            print("      %d 个独立镜像站返回同一 manifest digest: %s"
                  % (len(best_names), best_digest[:26]))
            print("      一致站: %s" % ", ".join(best_names))
        return

    # ---- (d) 兜底：只有单站可用 ----
    info["basis"] = "single-mirror-only"
    info["confidence"] = "low"
    info["notes"].append("只有 1 个镜像站可用，无法交叉验证来源；"
                         "建议用 --expect-digest 指定权威 digest")
    if verbose:
        print("      !! 只有一个镜像站可用，无法交叉验证")


# --------------------------------------------------------------------------
# 下载
# --------------------------------------------------------------------------

class RangeUnsupported(Exception):
    """镜像站没有按 Range 返回 206，分片策略在该站上不可用。"""


# ---- 多源并行下载的调参 ------------------------------------------------
SEG_MIN_BYTES = 4 * 1024 * 1024   # 小于这个尺寸不值得分片（分片收益 < 握手开销）
SEG_MIN_CHUNK = 1 * 1024 * 1024   # 每片至少这么大，避免切出一堆碎块
DEFAULT_SOURCES = 3               # 自动模式下每个 blob 同时用几个源
MAX_SOURCES = 8                   # 硬上限
MAX_TOTAL_CONNS = 12              # 层并发 × 源数 的总连接上限，护着本就不稳的链路
MAX_SEGMENTS = 8                  # 单片数量硬上限
RANGE_PROBE_BYTES = 64 * 1024     # 探测「该站是否真支持 Range」时拉多少字节

# ---- 吞吐测速的可信度护栏 --------------------------------------------
# 采样量太小时，数据其实早躺在内核接收缓冲里，计时窗口会被压成几微秒，
# 于是算出 62.5 GB/s 这种物理上不可能的「吞吐」。危害不止选错站：这个假值
# 会作为初值传给源池当 peak，而源池的慢源阈值是「最快源 × 25%」—— 阈值被
# 抬到十几 GB/s 后，所有真实源都会被判成慢源踢掉。
BENCH_MIN_BYTES = 4 * 1024 * 1024        # 计时采样量下限
BENCH_RETRY_MAX_BYTES = 32 * 1024 * 1024  # 样本不可信时放大重测的上限
BENCH_MIN_SECONDS = 0.05                 # 计时窗口短于此 -> 样本被摊平，弃用
BENCH_MAX_PLAUSIBLE = 200 * 1024 * 1024  # 201 MB/s 以上的「吞吐」只能是假象

# ---- 动态源池（SourceGovernor）的调参 ---------------------------------
GOV_SAMPLE_SEC = 1.0              # 速率采样周期（秒）
GOV_SLOW_RATIO = 0.25             # 吞吐低于「最快源 × 该比例」即判为慢
GOV_MIN_RATE = 64 * 1024          # 绝对速率地板：再快也不过这条线的不算「快」
GOV_SLOW_STRIKES = 3              # 连续这么多个采样周期都慢 -> 暂停该源
GOV_STALL_STRIKES = 4             # 有在途传输却连续这么多个周期零字节 -> 暂停
GOV_RESUME_COOLDOWN = 15.0        # 暂停后冷却这么久，才重新探测能否复活
GOV_ERROR_LIMIT = 2               # 连续失败这么多次 -> 暂停
GOV_PROBE_BYTES = 192 * 1024      # 复活探测的采样字节数（越小越不抢带宽）
GOV_EMA_ALPHA = 0.4               # 吞吐的指数滑动平均系数
GOV_MAX_PER_SOURCE = 3            # 单个源最多同时开几条传输（源变少时用来补并发）


def _watchdog(resp: Any, idle_timeout: float) -> threading.Timer:
    """空闲看门狗：超时直接 shutdown 底层 socket，打断阻塞中的 recv。

    注意要关 socket 而不是 resp.fp.raw —— 后者会把 BufferedReader 标记为 closed，
    随后 GC 时 HTTPResponse.close() 里的 flush() 会抛 'I/O operation on closed file' 噪声。
    """
    def _fire() -> None:
        try:
            raw = getattr(getattr(resp, "fp", None), "raw", None)
            sock = getattr(raw, "_sock", None)
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except Exception:  # noqa: BLE001
                    pass
                sock.close()
            elif raw is not None:
                raw.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            resp.close()
        except Exception:  # noqa: BLE001
            pass

    t = threading.Timer(idle_timeout, _fire)
    t.daemon = True
    t.start()
    return t


def _blob_total_size(reg: "Registry", digest: str) -> int:
    """用一个 1 字节的 Range 请求问出 blob 的总长度。"""
    resp = reg.blob_response(digest, 0, 1)
    try:
        cr = (resp.headers.get("Content-Range") or "").strip()
        if "/" in cr:
            tail = cr.rsplit("/", 1)[1]
            if tail.isdigit():
                return int(tail)
        return int(resp.headers.get("Content-Length") or 0)
    finally:
        try:
            resp.close()
        except Exception:  # noqa: BLE001
            pass


def _seg_ranges(total: int, n: int) -> List[Tuple[int, int, int]]:
    """把 total 均分成 n 段，返回 [(start, end_inclusive, length)]，余数摊给前几段。"""
    base, rem = divmod(total, n)
    out: List[Tuple[int, int, int]] = []
    off = 0
    for i in range(n):
        ln = base + (1 if i < rem else 0)
        if ln <= 0:
            continue
        out.append((off, off + ln - 1, ln))
        off += ln
    return out


def _rate_str(bps: float) -> str:
    if bps <= 0:
        return "0 B/s"
    if bps >= MB:
        return "%.2f MB/s" % (bps / MB)
    return "%.0f KB/s" % (bps / 1024.0)


def _err_kind(e: BaseException) -> str:
    if isinstance(e, (TimeoutError, socket.timeout)):
        return "超时"
    code = getattr(e, "code", None)
    if code:
        return "HTTP %s" % code
    return type(e).__name__


class SourcePaused(Exception):
    """当前源被源管理器主动暂停，在途传输应立即收手、换源续传。"""


class SourceGovernor:
    """动态源池：持续测速 -> 主动暂停慢源 -> 冷却后探测复活。

    与静态分配的区别：源不是一次性「分配」给某个 worker 的，而是随时向池子
    「申请」的。池子持续测量每个源的真实吞吐，把明显拖后腿的踢下线（同时打断
    它在途的那一小段传输，分片退回队列交给健康源），也会在冷却期满后主动探测，
    把恢复可用的、或本来就没上场的快源拉上来顶替。

    它主动做的三件事：
      1) 持续测速  每秒采样各源本窗口内实际搬运的字节数，算指数滑动平均。
      2) 主动关闭  吞吐低于「最快源 × slow_ratio」且低于绝对地板，或有在途传输
                   却连续若干周期零字节，即暂停该源并置 abort 打断在途传输。
      3) 主动启用  暂停的源冷却期满后用小样本 Range 探测，通过就重新上岗；
                   出现空闲位子时也会去探测还没上场的源，好源会自己冒出来。

    最后一道保险：永远不会把最后一个在岗的源也停掉，否则会僵住。
    """

    def __init__(
        self,
        registries: List["Registry"],
        names: List[str],
        digest: str,
        max_active: int,
        initial_speeds: Optional[List[float]] = None,
        slow_ratio: float = GOV_SLOW_RATIO,
        min_rate: float = GOV_MIN_RATE,
        cooldown: float = GOV_RESUME_COOLDOWN,
        log: Optional[Callable[[str], None]] = None,
    ):
        self.registries = list(registries)
        self.names = list(names)
        self.digest = digest
        self.n = len(self.registries)
        self.max_active = max(1, min(int(max_active), max(1, self.n)))
        self.slow_ratio = float(slow_ratio)
        self.min_rate = float(min_rate)
        self.cooldown = float(cooldown)
        self._log = log or (lambda _m: None)

        self._cv = threading.Condition()
        self._stop = threading.Event()
        self._monitor: Optional[threading.Thread] = None
        self._pool: Optional[concurrent.futures.ThreadPoolExecutor] = None

        self.state = ["benched"] * self.n         # active | benched
        self.rate = [0.0] * self.n                # EMA 吞吐
        self.peak = [0.0] * self.n                # 历史峰值吞吐
        self.fails = [0] * self.n
        self.inflight = [0] * self.n
        self.slow_hits = [0] * self.n
        self.stall_hits = [0] * self.n
        self.served = [0] * self.n                # 干完的分片数
        self.pauses = [0] * self.n
        self.kills = [0] * self.n                 # 被主动关闭的在途传输次数
        self.reason = ["未启用"] * self.n
        # 能力上就不可用（如该站根本不支持 Range）——不再探测、不再复活
        self.dead = [False] * self.n
        self.abort = [threading.Event() for _ in range(self.n)]
        self.retry_at = [0.0] * self.n
        self._win = [0] * self.n                  # 采样窗口内累计字节
        self._probing = [False] * self.n
        self._last_sample = time.monotonic()

        # 长度必须与池子站点数严格对齐：预检把不支持 Range 的站剔出去后，
        # 池子会比当初算测速初值时小，多出来的初值既不安全（越界）也没意义。
        speeds = list(initial_speeds or [])[: self.n]
        speeds += [0.0] * max(0, self.n - len(speeds))
        for i, sp in enumerate(speeds):
            # 再钳一道：万一上游给了物理上不可能的初值，慢源阈值会被它抬到
            # 十几 GB/s，于是所有真实源都被判成慢源踢光 —— 宁可当成未知。
            self.peak[i] = max(0.0, min(float(sp or 0.0), BENCH_MAX_PLAUSIBLE))
        for i in sorted(range(self.n), key=lambda k: -self.peak[k])[: self.max_active]:
            self.state[i] = "active"
            self.rate[i] = self.peak[i]
            self.reason[i] = ""

    # -- 生命周期 --------------------------------------------------------
    def start(self) -> None:
        self._pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="dip-probe")
        self._monitor = threading.Thread(target=self._monitor_loop,
                                         name="dip-gov", daemon=True)
        self._monitor.start()

    def stop(self) -> None:
        self._stop.set()
        with self._cv:
            self._cv.notify_all()
        if self._monitor is not None:
            self._monitor.join(timeout=3.0)
        if self._pool is not None:
            self._pool.shutdown(wait=False)

    # -- 对内 ------------------------------------------------------------
    def _actives_locked(self) -> List[int]:
        return [i for i in range(self.n)
                if self.state[i] == "active" and not self.dead[i]]

    def _bar_locked(self) -> float:
        """当前认可的速率下限：绝对地板 与「最快在岗源 × slow_ratio」取大。

        复活探测必须用同一把尺子。否则一个因为「相对太慢」而被暂停的源，
        会因为「高于绝对地板」又被探测通过，于是暂停/复活来回抖动。
        """
        act = self._actives_locked()
        best = max((self.rate[i] for i in act), default=0.0)
        return max(self.min_rate, best * self.slow_ratio)

    def _cap_locked(self) -> int:
        """每个源允许的并发传输数：源变少时放宽，让健康源多扛几条。"""
        a = len(self._actives_locked())
        if a <= 0:
            return 0
        return max(1, min(GOV_MAX_PER_SOURCE, -(-self.max_active // a)))

    def _pause_locked(self, i: int, reason: str) -> bool:
        if self.state[i] != "active":
            return False
        if len(self._actives_locked()) <= 1:
            return False                      # 最后一个在岗的源不能停
        self.state[i] = "benched"
        self.abort[i].set()                   # 打断它在途的传输
        self.pauses[i] += 1
        self.slow_hits[i] = 0
        self.stall_hits[i] = 0
        self.reason[i] = reason
        self.retry_at[i] = time.monotonic() + self.cooldown
        self._log("源 %s 已暂停：%s（%.0fs 后探测复活）"
                  % (self.names[i], reason, self.cooldown))
        self._cv.notify_all()
        return True

    def _promote_locked(self, i: int, rate: float, why: str) -> None:
        if self.dead[i]:
            return                           # 已移出池子，不复活
        self.state[i] = "active"
        self.abort[i].clear()                 # 重新上岗，清掉中止标志
        self.fails[i] = 0
        self.slow_hits[i] = 0
        self.stall_hits[i] = 0
        self.reason[i] = ""
        if rate > 0:
            self.rate[i] = rate
            self.peak[i] = max(self.peak[i], rate)
        self._log("源 %s 已启用：%s%s"
                  % (self.names[i], why,
                     ("，实测 %s" % _rate_str(rate)) if rate > 0 else ""))
        self._cv.notify_all()

    def disable(self, i: int, reason: str) -> None:
        """把一个源永久移出池子（例如它根本不支持 Range）。

        与 _pause 的区别：暂停是「暂时不行，等会儿再试」，disable 是「能力上
        就不满足」，重试多少次都一样 —— 所以不再探测、不再让它复活。
        """
        with self._cv:
            if self.dead[i]:
                return
            self.dead[i] = True
            self.state[i] = "benched"
            self.abort[i].set()              # 打断它在途的传输
            self.reason[i] = reason
            self.retry_at[i] = float("inf")  # 永不进入探测队列
            self._log("源 %s 已移出池子：%s" % (self.names[i], reason))
            self._cv.notify_all()

    # -- 对外 ------------------------------------------------------------
    def acquire(self, timeout: float = 1.0) -> Optional[int]:
        """申请一个在岗的源，返回其下标；拿不到返回 None。"""
        deadline = time.monotonic() + max(0.0, timeout)
        with self._cv:
            while True:
                if self._stop.is_set():
                    return None
                act = self._actives_locked()
                if act:
                    cap = self._cap_locked()
                    free = [i for i in act if self.inflight[i] < cap]
                    if free:
                        i = min(free, key=lambda k: (self.inflight[k], -self.rate[k]))
                        self.inflight[i] += 1
                        return i
                left = deadline - time.monotonic()
                if left <= 0:
                    return None
                self._cv.wait(min(left, 0.5))

    def release(self, i: int) -> None:
        with self._cv:
            if self.inflight[i] > 0:
                self.inflight[i] -= 1
            self._cv.notify_all()

    def report(self, i: int, nbytes: int) -> None:
        if nbytes > 0:
            with self._cv:
                self._win[i] += nbytes

    def guard(self, i: int) -> None:
        """传输热路径上的检查：源被暂停就立刻抛错收手。"""
        if self.abort[i].is_set():
            raise SourcePaused(self.names[i])

    def ok(self, i: int) -> None:
        with self._cv:
            self.fails[i] = 0
            self.slow_hits[i] = 0
            self.stall_hits[i] = 0
            if self.state[i] == "active":
                self.served[i] += 1

    def fail(self, i: int, kind: str) -> None:
        with self._cv:
            self.fails[i] += 1
            if self.state[i] == "active" and self.fails[i] >= GOV_ERROR_LIMIT:
                self._pause_locked(i, "连续 %d 次失败(%s)" % (self.fails[i], kind))
            self._cv.notify_all()

    def note_kill(self, i: int) -> None:
        with self._cv:
            self.kills[i] += 1

    def active_names(self) -> List[str]:
        with self._cv:
            return [self.names[i] for i in self._actives_locked()]

    # -- 监控线程 --------------------------------------------------------
    def _monitor_loop(self) -> None:
        while not self._stop.wait(GOV_SAMPLE_SEC):
            try:
                self._sample()
            except Exception as e:  # noqa: BLE001  监控不能因单次异常而停摆
                self._log("源管理器采样异常：%r" % (e,))

    def _sample(self) -> None:
        now = time.monotonic()
        todo: List[int] = []
        with self._cv:
            dt = max(1e-3, now - self._last_sample)
            self._last_sample = now
            for i in range(self.n):
                moved = self._win[i]
                self._win[i] = 0
                if self.state[i] != "active":
                    self.stall_hits[i] = 0
                    continue
                inst = moved / dt
                if inst > 0:
                    self.rate[i] = inst if self.rate[i] <= 0 else (
                        GOV_EMA_ALPHA * inst + (1.0 - GOV_EMA_ALPHA) * self.rate[i])
                    self.peak[i] = max(self.peak[i], inst)
                    self.stall_hits[i] = 0
                elif self.inflight[i] > 0:
                    self.stall_hits[i] += 1

            act = self._actives_locked()
            if len(act) >= 2:                  # 只剩一个源时没有可比对象，不做慢判定
                floor = self._bar_locked()
                for i in list(act):
                    if self.state[i] != "active":
                        continue               # 本轮已被停掉
                    if self.inflight[i] <= 0 and self.rate[i] <= 0:
                        continue               # 还没轮到它干活，不评价
                    if 0 < self.rate[i] < floor:
                        self.slow_hits[i] += 1
                        if self.slow_hits[i] >= GOV_SLOW_STRIKES:
                            self._pause_locked(
                                i, "吞吐 %s 低于阈值 %s"
                                % (_rate_str(self.rate[i]), _rate_str(floor)))
                    elif self.rate[i] >= floor:
                        self.slow_hits[i] = 0
                    if (self.state[i] == "active"
                            and self.stall_hits[i] >= GOV_STALL_STRIKES):
                        self._pause_locked(
                            i, "连续 %d 个周期无数据" % self.stall_hits[i])

            todo = self._plan_probe_locked(now)
            for i in todo:
                self._probing[i] = True
        for i in todo:
            if self._pool is not None:
                self._pool.submit(self._probe, i)

    def _plan_probe_locked(self, now: float) -> List[int]:
        act = self._actives_locked()
        if not act:
            return []
        cand = [i for i in range(self.n)
                if self.state[i] != "active" and not self._probing[i]
                and not self.dead[i]
                and now >= self.retry_at[i]]
        if not cand:
            return []
        cand.sort(key=lambda k: (-self.peak[k], self.retry_at[k]))
        free = self.max_active - len(act)
        if free > 0:
            return cand[: min(len(cand), free + 1, 2)]
        # 没空位：只探测「历史成绩明显优于当前最慢在岗源」的，用来顶替它
        slowest = min(act, key=lambda k: self.rate[k])
        bar = max(self.rate[slowest], self.min_rate) * 1.5
        return [i for i in cand if self.peak[i] >= bar][:1]

    def _probe(self, i: int) -> None:
        rate = 0.0
        err = ""
        try:
            t0 = time.perf_counter()
            resp = self.registries[i].blob_response(self.digest, 0, GOV_PROBE_BYTES)
            got = 0
            try:
                if resp.status != 206:
                    raise RangeUnsupported("探测请求返回 %s" % resp.status)
                while got < GOV_PROBE_BYTES and not self._stop.is_set():
                    ch = resp.read(64 * 1024)
                    if not ch:
                        break
                    got += len(ch)
            finally:
                try:
                    resp.close()
                except Exception:  # noqa: BLE001
                    pass
            dt = max(1e-6, time.perf_counter() - t0)
            if got >= GOV_PROBE_BYTES * 0.6:
                rate = got / dt
        except Exception as e:  # noqa: BLE001
            err = type(e).__name__
        with self._cv:
            self._probing[i] = False
            if self._stop.is_set():
                pass
            else:
                self.retry_at[i] = time.monotonic() + self.cooldown
                if err:
                    self.reason[i] = "探测不通(%s)" % err
                elif rate < self._bar_locked():
                    self.reason[i] = "探测吞吐 %s 低于当前阈值 %s" % (
                        _rate_str(rate), _rate_str(self._bar_locked()))
                else:
                    act = self._actives_locked()
                    if len(act) < self.max_active:
                        self._promote_locked(i, rate, "探测通过")
                    else:
                        # 没空位：只有明显更快才值得顶替掉当前最慢的那个
                        slowest = min(act, key=lambda k: self.rate[k])
                        if rate > self.rate[slowest] * 1.5:
                            self._pause_locked(
                                slowest, "被更快的 %s 顶替" % self.names[i])
                            self._promote_locked(i, rate, "探测通过(顶替上岗)")
                        else:
                            self.reason[i] = "探测可用但无空位，暂不上岗"
            self._cv.notify_all()

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._cv:
            return [{
                "name": self.names[i],
                "state": self.state[i],
                "rate_bytes_per_sec": round(self.rate[i], 1),
                "peak_bytes_per_sec": round(self.peak[i], 1),
                "segments_done": self.served[i],
                "pauses": self.pauses[i],
                "kills_inflight": self.kills[i],
                "note": self.reason[i],
                "disabled": self.dead[i],
            } for i in range(self.n)]


def _try_finish_partial(tmp: str, dest: str, digest: str,
                        expected_size: Optional[int], label: str) -> bool:
    """若 .part 已经完整且 sha256 与 digest 相符，直接扶正为成品。

    这是「其实早就下完了、只差改个名」的救命通道。字节已经躺在磁盘上，却因为
    最后一次请求失败（令牌过期 / 416 / 连接被掐）而白扔几百 MB，代价太大。
    长度对、摘要对才认；摘要不对说明这份字节确实坏了，删掉重来。
    """
    if not os.path.exists(tmp):
        return False
    n = os.path.getsize(tmp)
    if expected_size and n != expected_size:
        return False
    want = split_digest(digest)[1] if ":" in digest else digest
    got, _sz = sha256_file(tmp)
    if got != want:
        print("    %s: 已有 %s 但 sha256 不符，丢弃重下" % (label, human_bytes(n)),
              flush=True)
        try:
            os.remove(tmp)
        except OSError:
            pass
        return False
    os.replace(tmp, dest)
    print("    %s: %s 其实早已下完，直接收尾" % (label, human_bytes(n)), flush=True)
    return True


def _reusable(dest: str, digest: str, expected_size) -> bool:
    """判断已存在的 dest 能否直接复用（内容寻址校验，不只看长度）。

    以前只看文件长度是否等于 expected_size 就复用。这有两个毛病：
    长度对但内容坏（写入被截断、被别的进程动过）会被当成好文件收下，
    一路带到打包才炸；反过来长度不等时又直接从零重下，
    而磁盘上那些字节明明是好的、只差没下完。
    这里改成 sha256 校验 —— blob 是内容寻址的，digest 就是它的身份，
    对得上才复用，对不上才重下。
    """
    if not os.path.exists(dest) or not (os.path.getsize(dest) > 0):
        return False
    alg, expect_hex = split_digest(digest)
    if alg != "sha256" or not expect_hex:
        n = os.path.getsize(dest)          # 非 sha256 摘要无法校验，退化为长度判断
        return bool(not expected_size or n == expected_size)
    actual, _sz = sha256_file(dest)
    return actual == expect_hex


def download_blob(
    registries: List["Registry"],
    digest: str,
    dest: str,
    label: str,
    progress: Optional[Progress],
    key: str,
    expected_size: Optional[int] = None,
    max_retries: int = 12,
    idle_timeout: float = 45.0,
    names: Optional[List[str]] = None,
    resume_cooldown: float = GOV_RESUME_COOLDOWN,
    no_progress_limit: int = DEFAULT_NO_PROGRESS,
) -> int:
    """断点续传式下载一个 blob，边下边报进度。返回字节数。

    支持**跨镜像站故障转移**：同一 blob 在任何站上都是内容寻址的同一串字节，
    所以中途换站也能用 HTTP Range 从已下载处接着下。

    三条保住进度的铁律（都是被真实事故逼出来的，别再改回去）：
      1) **令牌过期(401)不丢进度**。它只说明「这次没带对凭证」，与盘上的字节
         无关；刷新令牌后从原地续传。
      2) **不认 Range 的站（返回 200 而非 206）不拿来续传**。它只会从 0 重来，
         用它续传等于把已下的几百 MB 全扔掉。实测 dockerproxy.net 就是这种站，
         而它测速最快、最容易被选中 —— 这正是「下一点就清零」反复发生的根因。
      3) **只有「重试而毫无进展」才消耗预算**。令牌轮换、服务端按寿命掐断连接
         这类**有进展**的中断不该耗尽重试次数；否则 4.7GB 的层（要跨好几次
         令牌周期）永远下不完。
    """
    if not registries:
        raise DipError("没有可用的 registry")
    names = names or ["%d" % i for i in range(len(registries))]
    # 已有完整且内容正确的 dest -> 直接复用，不联网。
    # 注意这里必须**校验内容**而不是只看长度，否则长度对但内容坏的会混进来。
    if _reusable(dest, digest, expected_size):
        n = os.path.getsize(dest)
        if progress:
            progress.set_done(key, n)
        return n
    # dest 内容不对（或长度不对），但 .part 可能其实早就完整了（只差改名）
    if _try_finish_partial(dest + ".part", dest, digest, expected_size, label):
        n = os.path.getsize(dest)
        if progress:
            progress.set_done(key, n)
        return n

    tmp = dest + ".part"
    start = os.path.getsize(tmp) if os.path.exists(tmp) else 0
    done = start
    total = 0
    order = list(range(len(registries)))
    pos = 0
    fails: Dict[int, int] = {i: 0 for i in order}
    # 「超时先暂停」：超时过的站在冷却期内不被选中
    cooldown_until: List[float] = [0.0] * len(registries)
    idle_attempts = 0      # 连续「毫无进展」的次数 —— 这才是真正的失败预算
    range_rejects = 0      # 连续被 Range 拒绝的次数
    hard_cap = max(64, max_retries * 8)    # 防死循环；真正管住循环的是 idle_attempts

    for attempt in range(hard_cap):
        check_cancel()
        if idle_attempts > no_progress_limit:
            raise DipError("blob %s 连续 %d 次重试毫无进展，放弃（已下 %s）"
                           % (digest[:19], idle_attempts, human_bytes(done)))
        ri = order[pos % len(order)]
        if len(order) > 1:
            now = time.monotonic()

            def _usable(k: int) -> bool:
                """第 k 个位置上的站能否用来**续传**。"""
                if start > 0 and registries[order[k]].range_ok is False:
                    return False          # 它不认 Range，只会从头重来
                return cooldown_until[order[k]] <= now

            pick: Optional[int] = None
            for step in range(len(order)):
                cand = (pos + step) % len(order)
                if _usable(cand):
                    pick = cand
                    break
            if pick is None and start > 0:
                # 找不到「既不在冷却期、又能续传」的站。退一步只要求不在冷却期：
                # 那也比用一个会从头重来的站强。
                for step in range(len(order)):
                    cand = (pos + step) % len(order)
                    if cooldown_until[order[cand]] <= now:
                        pick = cand
                        break
            if pick is None:               # 全在冷却：挑最早解冻的那个
                pick = min(range(len(order)), key=lambda s: cooldown_until[order[s]])
            pos = pick
            ri = order[pos % len(order)]
        reg = registries[ri]
        if progress:
            progress.relabel(key, "%s @%s" % (label, names[ri]))
        resp = None
        timer: Optional[threading.Timer] = None
        last_done = done
        try:
            resp = reg.blob_response(digest, start=start)
            cr = resp.headers.get("Content-Range")
            if cr and "/" in cr:
                try:
                    total = int(cr.rsplit("/", 1)[1])
                except ValueError:
                    total = 0
            if not total:
                total = int(resp.headers.get("Content-Length") or expected_size or 0)
            if total and start:
                # 206 的 Content-Range 给的是完整长度；Content-Length 只是剩余片段
                total = max(total, done + int(resp.headers.get("Content-Length") or 0))
            if progress and total:
                progress.add(key, label, total)
                progress.set_done(key, done)

            timer = _watchdog(resp, idle_timeout)
            mode = "ab" if start else "wb"
            with open(tmp, mode) as f:
                while True:
                    if timer is not None:
                        timer.cancel()
                    timer = _watchdog(resp, idle_timeout)
                    chunk = resp.read(1024 * 256)
                    if not chunk:
                        break
                    f.write(chunk)
                    done += len(chunk)
                    if progress:
                        progress.update(key, len(chunk))
            if timer is not None:
                timer.cancel()
                timer = None
            if total and done < total:
                # 被掐断了（多半是服务端按令牌寿命切断连接），但**进度在盘上**
                if done > last_done:
                    idle_attempts = 0
                    range_rejects = 0
                else:
                    idle_attempts += 1
                    time.sleep(min(2.0 * idle_attempts, 10.0))
                start = done
                if expected_size and done >= expected_size:
                    if _try_finish_partial(tmp, dest, digest, expected_size, label):
                        if progress:
                            progress.set_done(key, os.path.getsize(dest))
                        return os.path.getsize(dest)
                continue
            os.replace(tmp, dest)
            return done
        except RangeUnsupported as e:
            # 该站不认 Range。start==0 时它照样能用（整份拉），start>0 时不能用。
            if timer is not None:
                timer.cancel()
                timer = None
            reg.range_ok = False
            resumable = [order[k] for k in range(len(order))
                         if order[k] != ri and registries[order[k]].range_ok is not False]
            if start > 0 and resumable:
                nxt = resumable[0]
                pos = order.index(nxt)
                print("    %s: %s %s，改用 %s 从 %s 续传（进度保留）"
                      % (label, names[ri], e, names[nxt], human_bytes(start)),
                      flush=True)
                time.sleep(0.5)
                continue
            if start > 0:
                # 所有可用站都不认 Range，没有别的办法只能从头来。这种站会反复
                # 触发，所以必须明确喊出来，绝不能让它静悄悄地转圈。
                print("    %s: !! 已下 %s，但所有可用镜像站都不支持断点续传，"
                      "只能从头重下（建议换支持 Range 的站）"
                      % (label, human_bytes(start)), flush=True)
                start = done = 0
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
                idle_attempts += 1
            else:
                pos = (pos + 1) % len(order)
                idle_attempts += 1
            time.sleep(min(2.0 * max(1, idle_attempts), 10.0))
            continue
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError,
                socket.timeout, OSError, ValueError, http.client.HTTPException) as e:
            if timer is not None:
                timer.cancel()
                timer = None
            code = getattr(e, "code", None)
            if done > last_done:
                idle_attempts = 0
                range_rejects = 0
            else:
                idle_attempts += 1

            if code == 401:
                # 令牌过期。**保留进度**：blob 是内容寻址的，401 说的只是
                # 「这次没带对凭证」，与盘上的字节毫无关系。
                reg.refresh_token()
                start = done = os.path.getsize(tmp) if os.path.exists(tmp) else 0
                print("    %s: %s 令牌过期，已刷新，从 %s 续传"
                      % (label, names[ri], human_bytes(start)), flush=True)
                time.sleep(min(2.0 * max(1, idle_attempts), 8.0))
                continue

            if code == 416:
                # 请求范围越过文件末尾：本地这份多半已经下满了
                if _try_finish_partial(tmp, dest, digest, expected_size, label):
                    if progress:
                        progress.set_done(key, os.path.getsize(dest))
                    return os.path.getsize(dest)
                cur = os.path.getsize(tmp) if os.path.exists(tmp) else 0
                if expected_size and cur > expected_size:
                    try:                       # 比目标还长，字节串了，只能丢
                        os.remove(tmp)
                    except OSError:
                        pass
                    cur = 0
                start = done = cur
                time.sleep(min(2.0 * max(1, idle_attempts), 8.0))
                continue

            if code in (400, 403):
                # Range 被拒。先保留进度换站试，连续两次才认输归零 ——
                # 一次 400 就扔掉几百 MB 太亏。
                range_rejects += 1
                if range_rejects >= 2:
                    start = done = 0
                    range_rejects = 0
                    if os.path.exists(tmp):
                        try:
                            os.remove(tmp)
                        except OSError:
                            pass
                else:
                    start = done = os.path.getsize(tmp) if os.path.exists(tmp) else 0
            else:
                start = os.path.getsize(tmp) if os.path.exists(tmp) else 0
                done = start

            # ---- 超时：先把这个站暂停，立刻换一个 ----
            if _err_kind(e) == "超时" and len(order) > 1:
                cooldown_until[ri] = time.monotonic() + resume_cooldown
                pos = (pos + 1) % len(order)
                nxt = order[pos]
                print("    %s: %s 超时，暂停该站 %.0fs 并换 %s 续传（已下 %s）"
                      % (label, names[ri], resume_cooldown, names[nxt],
                         human_bytes(start)), flush=True)
                if progress:
                    progress.relabel(key, "%s @%s" % (label, names[nxt]))
                time.sleep(min(2 ** attempt, 8))
                continue
            # ---- 跨镜像站故障转移（非超时错误） ----
            fails[ri] = fails.get(ri, 0) + 1
            if len(order) > 1 and fails[ri] >= 2:
                fails[ri] = 0
                pos = (pos + 1) % len(order)
                nxt = order[pos]
                fails[nxt] = 0
                print("    %s: %s 在 %s 上连续失败，换 %s 续传（已下 %s）"
                      % (label, type(e).__name__, names[ri], names[nxt], human_bytes(start)),
                      flush=True)
                if progress:
                    progress.relabel(key, "%s @%s" % (label, names[nxt]))
            backoff = 45 if code == 429 else min(2 ** attempt, 15)
            time.sleep(backoff)
            continue
        finally:
            if timer is not None:
                timer.cancel()
    raise DipError("blob %s 下载失败（无进展重试 %d 次，镜像站: %s）"
                   % (digest[:19], idle_attempts, ", ".join(names)))


def _fetch_segment(
    reg: "Registry",
    digest: str,
    start: int,
    end: int,
    path: str,
    idle_timeout: float,
    progress: Optional[Progress],
    key: str,
    gov: Optional[SourceGovernor] = None,
    ri: int = -1,
) -> int:
    """把 blob 的 [start, end] 段拉进 path，支持从已下字节续传。返回本次新增字节数。

    拿不到 206 / Content-Range 与请求不符时抛 RangeUnsupported —— 这是分片策略
    在该站上的硬性不兼容，不是网络抖动，不该重试。

    每读到一个 chunk 都会向源管理器报账（report）并检查是否被勒令收手（guard）。
    被暂停时抛 SourcePaused，此时已写入 path 的字节**保留**，下次接着续。
    """
    want = end - start + 1
    have = os.path.getsize(path) if os.path.exists(path) else 0
    if have >= want:
        return 0
    req_start = start + have
    resp = reg.blob_response(digest, req_start, want - have)
    timer: Optional[threading.Timer] = None
    added = 0
    try:
        if resp.status != 206:
            raise RangeUnsupported("对 Range 请求返回 %s 而非 206" % resp.status)
        cr = (resp.headers.get("Content-Range") or "").strip()
        m = re.match(r"bytes\s+(\d+)-(\d+)/(\d+|\*)", cr)
        if cr and (not m or int(m.group(1)) != req_start):
            raise RangeUnsupported("Content-Range 与请求不符: %s" % cr)
        timer = _watchdog(resp, idle_timeout)
        with open(path, "ab" if have else "wb") as f:
            while have < want:
                if timer is not None:
                    timer.cancel()
                timer = _watchdog(resp, idle_timeout)
                chunk = resp.read(1024 * 256)
                if not chunk:
                    break
                n = min(len(chunk), want - have)
                f.write(chunk[:n])
                have += n
                added += n
                if progress:
                    progress.update(key, n)
                if gov is not None:
                    gov.report(ri, n)
                    gov.guard(ri)
    finally:
        if timer is not None:
            timer.cancel()
        try:
            resp.close()
        except Exception:  # noqa: BLE001
            pass
    return added


def _seg_prefix_bytes(dest: str, ranges: List[Tuple[int, int, int]]) -> int:
    """返回从第 0 片起**连续完整**的分片覆盖了多少字节。"""
    total = 0
    for i, (_s, _e, ln) in enumerate(ranges):
        p = dest + ".seg%02d" % i
        if not os.path.exists(p) or os.path.getsize(p) < ln:
            break
        total += ln
    return total


def _salvage_segments_to_part(dest: str, ranges: List[Tuple[int, int, int]]) -> int:
    """把开头连续完整的分片拼进 `<dest>.part`，供单流续传接着用。

    分片是**已经下好的真实字节**，退回单流时丢掉它们就是白白重下。
    只在拼接结果比现有 `.part` 更长时才重写；分片本身**不删**。
    返回拼接后的 `.part` 长度（0 表示没救到任何东西）。
    """
    prefix = _seg_prefix_bytes(dest, ranges)
    if prefix <= 0:
        return 0
    part = dest + ".part"
    cur = os.path.getsize(part) if os.path.exists(part) else 0
    if prefix <= cur:
        return cur
    tmp = part + ".salvage"
    try:
        with open(tmp, "wb") as out:
            left = prefix
            for i, (_s, _e, ln) in enumerate(ranges):
                if left <= 0:
                    break
                p = dest + ".seg%02d" % i
                with open(p, "rb") as f:
                    shutil.copyfileobj(f, out, 1024 * 512)
                left -= ln
        os.replace(tmp, part)
    except OSError:
        try:
            os.remove(tmp)
        except OSError:
            pass
        return cur
    return prefix


def download_blob_multisource(
    registries: List["Registry"],
    digest: str,
    dest: str,
    label: str,
    progress: Optional[Progress],
    key: str,
    expected_size: Optional[int] = None,
    names: Optional[List[str]] = None,
    sources: int = DEFAULT_SOURCES,
    segments: int = 0,
    max_retries: int = 8,
    idle_timeout: float = 45.0,
    initial_speeds: Optional[List[float]] = None,
    use_governor: bool = True,
    slow_ratio: float = GOV_SLOW_RATIO,
    min_rate: float = GOV_MIN_RATE,
    cooldown: float = GOV_RESUME_COOLDOWN,
    gov_log: Optional[Callable[[str], None]] = None,
    stats: Optional[Dict[str, Any]] = None,
    no_progress_limit: int = DEFAULT_NO_PROGRESS,
    stall_seconds: float = SEG_STALL_SECONDS,
) -> int:
    """多源分片并行下载：把一个 blob 切成若干段，同时从多个镜像站各拉一段。

    源是**动态**的：由 SourceGovernor 维护一个池子，持续测速、主动关闭慢源、
    冷却后探测复活。worker 每拉一片都要向池子申请源，被关闭时立刻收手并把
    分片交给健康源续传。

    依据是 blob 的**内容寻址**特性 —— 同一个 digest 在任何镜像站上都是同一串字节，
    所以不同段可以来自不同站，拼起来仍会被 digest 校验通过。安全阀也在于此：拼接
    完成后立刻算 sha256，对不上就判失败并清掉全部分片，绝不把坏数据留在盘上。

    落盘用「一段一个文件」（`<dest>.segNN`）+ 末尾拼接，而不是预分配大文件 + seek 写入。
    代价是多一次拼接 I/O，换来的是**续传语义天然正确**：分片文件的字节数就是进度，
    不需要额外的状态文件，也不怕状态与磁盘不一致。

    任何站不支持 Range 时（返回 200 而非 206），整块回退到单流下载。
    """
    names = names or ["%d" % i for i in range(len(registries))]
    if not registries:
        raise DipError("没有可用的 registry")
    # 与 download_blob 同样的道理：复用前必须校验内容，不能只看长度
    if _reusable(dest, digest, expected_size):
        n = os.path.getsize(dest)
        if progress:
            progress.set_done(key, n)
        return n
    # 只有一个源 / 显式关闭分片 -> 走单流
    if sources <= 1 or len(registries) <= 1:
        return download_blob(registries, digest, dest, label, progress, key,
                             expected_size, max_retries, idle_timeout, names,
                             resume_cooldown=cooldown,
                             no_progress_limit=no_progress_limit)

    # ---- 1) 问出总长度（Range 0-0） ----
    total = int(expected_size or 0)
    if not total:
        for reg in registries:
            check_cancel()
            try:
                total = _blob_total_size(reg, digest)
            except Exception:  # noqa: BLE001
                total = 0
            if total:
                break
    if total < SEG_MIN_BYTES:
        return download_blob(registries, digest, dest, label, progress, key,
                             expected_size, max_retries, idle_timeout, names,
                             resume_cooldown=cooldown,
                             no_progress_limit=no_progress_limit)

    # ---- 2) 划段 ----
    n_src = max(1, min(sources, len(registries), MAX_SOURCES))
    n_seg = segments if segments > 0 else min(n_src * 2, MAX_SEGMENTS)
    n_seg = max(1, min(n_seg, max(1, total // SEG_MIN_CHUNK), MAX_SEGMENTS))
    ranges = _seg_ranges(total, n_seg)
    if len(ranges) <= 1:
        return download_blob(registries, digest, dest, label, progress, key,
                             expected_size, max_retries, idle_timeout, names,
                             resume_cooldown=cooldown,
                             no_progress_limit=no_progress_limit)
    seg_paths = [dest + ".seg%02d" % i for i in range(len(ranges))]
    # 上一次运行留下的分片状态。有这个，就不能再退回单流：单流只认 `.part`，
    # 会把已经下好的几个 GB 当成不存在 —— 实测就是这样把 83% 的进度丢掉的。
    has_seg_state = any(os.path.exists(p) for p in seg_paths)

    # 源池最多管这么多站；多余的连探活都不做，避免平白占带宽
    regs = registries[:MAX_SOURCES] if len(registries) > MAX_SOURCES else registries
    nm = names[:len(regs)]

    # ---- 预检 Range 支持 ----
    # 不支持断点续传的站（实测 dockerproxy.net：任何 Range 都回 200 整份文件）
    # 必须**现在就**剔出去。等到分片跑到一半才发现，代价是白折腾一场 ——
    # 旧实现甚至会因此删光已下的分片、退回单流从头下，这正是「下一点就
    # 清零」反复出现的直接原因。check_range_support 按站点缓存结果，
    # 整轮只花一次 64KiB 探测的代价。
    if len(regs) > 1:
        kept = [(k, r, t) for k, (r, t) in enumerate(zip(regs, nm))
                if r.check_range_support(digest)]
        if len(kept) < len(regs):
            print("      %s: %d 个站不支持 Range 断点续传，已排除"
                  % (label, len(regs) - len(kept)), flush=True)
        if not kept:
            # 一个支持 Range 的站都没有。分片没法继续往下长，但已经下好的
            # 分片**不是垃圾**：把开头连续完整的部分拼成单流续传起点，
            # 能救多少救多少，绝不原地清零。
            if has_seg_state:
                got = _salvage_segments_to_part(dest, ranges)
                if got:
                    print("      %s: 无站支持 Range，已把完整分片拼为单流续传起点（%s）"
                          % (label, human_bytes(got)), flush=True)
            return download_blob(regs, digest, dest, label, progress, key,
                                 expected_size, max_retries, idle_timeout, nm,
                                 resume_cooldown=cooldown,
                                 no_progress_limit=no_progress_limit)
        keep_idx = [k for k, _, _ in kept]
        regs = [r for _, r, _ in kept]
        nm = [t for _, _, t in kept]
        if initial_speeds:
            # 初值当初是按「未被剔除的完整站点顺序」算的，这里必须跟着站点
            # 一起重排，否则速率会张冠李戴（快源被当成慢源、开局就歇着）。
            _sp = list(initial_speeds)
            initial_speeds = [_sp[k] if k < len(_sp) else 0.0 for k in keep_idx]
        # 只剩一个支持 Range 的站时**不要退回单流**：
        # 单流只看 .part，会把分片里的进度全部弃用。一个站照样能把分片
        # 一片一片续完 —— 分片数和边界都没变，正是「断点续传」该有的样子。
        if len(regs) <= 1 and not has_seg_state:
            return download_blob(regs, digest, dest, label, progress, key,
                                 expected_size, max_retries, idle_timeout, nm,
                                 resume_cooldown=cooldown,
                                 no_progress_limit=no_progress_limit)
        n_src = max(1, min(n_src, len(regs)))

    # ---- 接管单流时代留下的 .part，当作前几片的已有前缀 ----
    # 不加这一步，「先单流、后分片」的模式切换会把已经下好的几百 MB 整个丢掉
    # 重下 —— 真实事故里那 510 MiB 就是这么白费的。
    # 注意 .part 本身**不删**：万一后面回退单流，它仍是单流的续传起点。
    legacy = dest + ".part"
    if os.path.exists(legacy) and os.path.getsize(legacy) > 0:
        have = os.path.getsize(legacy)
        adopted = 0
        copied = 0
        try:
            with open(legacy, "rb") as src:
                for pth, (_s, _e, ln) in zip(seg_paths, ranges):
                    if adopted >= have:
                        break
                    take = min(ln, have - adopted)
                    cur = os.path.getsize(pth) if os.path.exists(pth) else 0
                    if cur < take:
                        src.seek(adopted)
                        chunk = src.read(take - cur)
                        with open(pth, "ab" if cur else "wb") as dst:
                            dst.write(chunk)
                        copied += len(chunk)
                    adopted += take
        except OSError:
            adopted = copied = 0
        # 只在**真的搬了字节**时才说；分片本来就比 .part 更长时无需多嘴，
        # 否则会报出一个并未发生的「接管」，让人误判进度从哪来。
        if copied > 0:
            print("      %s: 接管上次残留的 %s 作为前几片前缀，这部分不必重下"
                  % (label, human_bytes(copied)), flush=True)

    done0 = 0
    for p, (_s, _e, ln) in zip(seg_paths, ranges):
        if os.path.exists(p):
            done0 += min(os.path.getsize(p), ln)
    if progress:
        progress.add(key, "%s [%d源×%d片]" % (label, n_src, len(ranges)), total)
        progress.set_done(key, done0)

    pending = list(range(len(ranges)))
    plock = threading.Lock()
    abort = threading.Event()
    unsupported = threading.Event()
    errors: List[str] = []

    # ---- 2.5) 建源池并启动监控线程 ----
    gov: Optional[SourceGovernor] = None
    if use_governor:
        gov = SourceGovernor(regs, nm, digest, n_src,
                             initial_speeds=initial_speeds,
                             slow_ratio=slow_ratio, min_rate=min_rate,
                             cooldown=cooldown, log=gov_log)
        gov.start()
    n_worker = max(1, min(n_src + 1, len(ranges)))

    def worker(wid: int) -> None:
        """向源池申请一个源 -> 拉一片 -> 还回去；做完再抢下一片。

        与旧的静态分配不同，worker 不再绑死在某个源上：源被池子判定过慢而关闭时
        它会立刻收手（SourcePaused），已写入 .seg 的字节保留，换个健康源接着续。
        因此「被关闭」不消耗失败重试预算 —— 那是池子的正常调度，不是错误。
        """
        waits = 0
        while not abort.is_set():
            with plock:
                if not pending:
                    return
                si = pending.pop(0)
            start, end, ln = ranges[si]
            path = seg_paths[si]
            last: Optional[BaseException] = None
            ok = False
            attempts = 0
            idle = 0                     # 连续「毫无进展」次数
            kill_hits = 0
            last_size = os.path.getsize(path) if os.path.exists(path) else 0
            stall_since = time.monotonic()

            def _after_error() -> None:
                """失败后重新记账：只要分片长过，就认为仍在推进，预算清零。"""
                nonlocal last_size, idle, stall_since
                cur = os.path.getsize(path) if os.path.exists(path) else 0
                if cur > last_size:
                    idle = 0
                    stall_since = time.monotonic()
                else:
                    idle += 1
                last_size = cur

            while not abort.is_set():
                check_cancel()
                if idle >= no_progress_limit:
                    last = last or DipError("连续 %d 次毫无进展" % idle)
                    break
                if time.monotonic() - stall_since >= stall_seconds:
                    last = last or DipError("连续 %.0fs 没有新字节" % stall_seconds)
                    break
                if attempts >= SEG_ATTEMPT_HARD_CAP:
                    break
                if gov is not None:
                    ri = gov.acquire(timeout=1.0)
                    if ri is None:
                        if abort.is_set():
                            return
                        waits += 1
                        if waits > 240:       # 约 4 分钟等不到任何可用源
                            errors.append("片 %d/%d: 长时间等不到可用源"
                                          % (si + 1, len(ranges)))
                            abort.set()
                            return
                        continue
                else:
                    ri = attempts % len(regs)
                reg = regs[ri]
                try:
                    _fetch_segment(reg, digest, start, end, path, idle_timeout,
                                   progress, key, gov=gov, ri=ri)
                    if gov is not None:
                        gov.ok(ri)
                    ok = True
                    break
                except SourcePaused as e:
                    last = e
                    if gov is not None:
                        gov.note_kill(ri)
                    kill_hits += 1
                    _after_error()
                    idle = max(0, idle - 1)      # 池子调度不是错误，不消耗预算
                    have_now = os.path.getsize(path) if os.path.exists(path) else 0
                    print("      %s: 源 %s 已判定过慢被关闭，片 %d/%d 转由其它源续传"
                          "（已收 %s）" % (label, e, si + 1, len(ranges),
                                          human_bytes(have_now)), flush=True)
                    if kill_hits >= max_retries * 4:
                        break
                    continue
                except RangeUnsupported as e:
                    # 该站不认 Range。**别为一棵树砍掉整片林子**：旧实现直接置
                    # unsupported + abort，把已下好的全部分片删光、退回单流从头
                    # 下 —— 一个不支持 Range 的站就能反复触发这件事。正确做法是
                    # 把它移出源池，其余站继续把分片续完。
                    reg.range_ok = False
                    if gov is not None:
                        gov.disable(ri, "不支持 Range")
                    usable = [k for k in range(len(regs)) if regs[k].range_ok is not False]
                    if not usable:
                        print("      %s: 所有镜像站都不支持 Range，回退单流下载"
                              % label, flush=True)
                        unsupported.set()
                        abort.set()
                        return
                    print("      %s: %s %s，已移出源池（还剩 %d 个源，分片保留）"
                          % (label, nm[ri], e, len(usable)), flush=True)
                    last = e
                    _after_error()
                    if gov is None:
                        attempts += 1
                    continue
                except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError,
                        socket.timeout, OSError, ValueError, http.client.HTTPException) as e:
                    last = e
                    attempts += 1
                    if getattr(e, "code", None) == 401:
                        reg.clear_tokens()
                    if gov is not None:
                        gov.fail(ri, _err_kind(e))
                    _after_error()
                    print("      片 %d/%d 在 %s 上失败(%s)，换源重试"
                          "（连续无进展 %d/%d 次）"
                          % (si + 1, len(ranges), nm[ri], type(e).__name__,
                             idle, no_progress_limit), flush=True)
                    time.sleep(min(2 ** min(attempts, 4), 15))
                    continue
                finally:
                    if gov is not None:
                        gov.release(ri)
            if ok:
                continue
            if abort.is_set():
                return
            errors.append("片 %d/%d: %s（重试 %d 次，连续无进展 %d 次）"
                          % (si + 1, len(ranges), last or "未知原因", attempts, idle))
            abort.set()
            return

    cancelled = False
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_worker) as ex:
            futs = [ex.submit(worker, w) for w in range(n_worker)]
            for f in concurrent.futures.as_completed(futs):
                try:
                    f.result()
                except Cancelled:
                    cancelled = True
                    abort.set()
                except Exception as e:  # noqa: BLE001
                    errors.append("%s" % e)
    finally:
        if gov is not None:
            if stats is not None:
                stats["governor"] = {
                    "enabled": True,
                    "pool_size": len(regs),
                    "max_active": n_src,
                    "sources": gov.snapshot(),
                }
            gov.stop()
        # 下载途中所有站都退化成了「不认 Range」时，不能再像以前那样把分片
        # 一删了之 —— 那等于把几个小时的下行流量直接倒掉。把开头连续完整的
        # 分片拼成 `.part`，下面的单流回退就从那里接着下。
        if unsupported.is_set():
            got = _salvage_segments_to_part(dest, ranges)
            if got:
                print("      %s: 回退单流，已保留 %s 进度"
                      % (label, human_bytes(got)), flush=True)

    if cancelled:
        raise Cancelled("已取消")
    if unsupported.is_set():
        return download_blob(registries, digest, dest, label, progress, key,
                             expected_size, max_retries, idle_timeout, names,
                             resume_cooldown=cooldown,
                             no_progress_limit=no_progress_limit)
    if errors:
        raise DipError("blob %s 多源分片下载失败:\n  %s"
                       % (digest[:19], "\n  ".join(errors[:4])))

    # ---- 3) 每片长度必须刚好到位 ----
    for si, (_s, _e, ln) in enumerate(ranges):
        p = seg_paths[si]
        got = os.path.getsize(p) if os.path.exists(p) else 0
        if got != ln:
            raise DipError("blob %s 第 %d 片长度不符（期望 %d，实际 %d）"
                           % (digest[:19], si + 1, ln, got))

    # ---- 4) 拼接 -> 内容寻址校验 -> 落位 ----
    alg, expect_hex = split_digest(digest)
    part = dest + ".part"
    with open(part, "wb") as out_f:
        for si, p in enumerate(seg_paths, 1):
            if progress:
                progress.relabel(key, "%s 拼接 %d/%d" % (label, si, len(seg_paths)))
            with open(p, "rb") as f:
                shutil.copyfileobj(f, out_f, 1024 * 512)
    if alg == "sha256" and expect_hex:
        actual, _sz = sha256_file(part)
        if actual != expect_hex:
            for p in seg_paths + [part]:
                try:
                    os.remove(p)
                except OSError:
                    pass
            raise DipError("多源拼接后 sha256 不匹配，已丢弃全部分片\n"
                           "  expect sha256:%s\n  actual sha256:%s" % (expect_hex, actual))
    os.replace(part, dest)
    for p in seg_paths:
        try:
            os.remove(p)
        except OSError:
            pass
    if progress:
        progress.relabel(key, label)
        progress.set_done(key, total)
    return total


def decompress_layer(src: str, dst: str, kind: str) -> None:
    if kind == "gzip":
        with gzip.open(src, "rb") as fin, open(dst, "wb") as fout:
            shutil.copyfileobj(fin, fout, 1024 * 512)
    elif kind == "zstd":
        dec = _zstd_opener()
        with dec(src) as fin, open(dst, "wb") as fout:
            shutil.copyfileobj(fin, fout, 1024 * 512)
    else:
        shutil.copyfile(src, dst)


def _zstd_opener():
    try:
        import compression.zstd as _z  # Python 3.14+
        return lambda p: _z.ZstdFile(p, "rb")
    except Exception:  # noqa: BLE001
        pass
    try:
        import zstandard as _zs  # 可选第三方
        return lambda p: _zs.ZstdDecompressor().stream_reader(open(p, "rb"))
    except Exception:  # noqa: BLE001
        pass
    raise DipError(
        "该镜像含 zstd 压缩层，但当前 Python 无 zstd 支持。\n"
        "  解决：升级到 Python 3.14+，或 pip install zstandard")


def layer_kind(media_type: str, path: str) -> str:
    mt = (media_type or "").lower()
    if "zstd" in mt:
        return "zstd"
    if "gzip" in mt:
        return "gzip"
    if "tar" in mt:
        return "none"
    with open(path, "rb") as f:
        magic = f.read(4)
    if magic[:2] == b"\x1f\x8b":
        return "gzip"
    if magic == b"\x28\xb5\x2f\xfd":
        return "zstd"
    return "none"


# --------------------------------------------------------------------------
# 打包（docker save 格式）
# --------------------------------------------------------------------------

def _tarinfo_bytes(name: str, size: int) -> tarfile.TarInfo:
    ti = tarfile.TarInfo(name)
    ti.size = size
    ti.mtime = 0
    ti.mode = 0o644
    ti.uid = 0
    ti.gid = 0
    ti.uname = ""
    ti.gname = ""
    return ti


def _tarinfo_file(name: str, size: int) -> tarfile.TarInfo:
    ti = _tarinfo_bytes(name, size)
    ti.type = tarfile.REGTYPE
    return ti


def build_archive(
    out_path: str,
    compression: str,
    level: int,
    manifest_json: List[Dict[str, Any]],
    repositories: Dict[str, Any],
    config_name: str,
    config_bytes: bytes,
    layer_paths: List[str],
    progress: Optional[Progress] = None,
) -> None:
    """写出 docker-archive 格式的 tar(.gz)。成员顺序：manifest -> repositories -> config -> layers。"""
    tmp = out_path + ".tmp"
    raw = open(tmp, "wb")
    gz = None
    try:
        if compression == "gzip":
            gz = gzip.GzipFile(filename="", mode="wb", compresslevel=level,
                               fileobj=raw, mtime=0)
            sink = gz
        else:
            sink = raw
        with tarfile.open(fileobj=sink, mode="w", format=tarfile.GNU_FORMAT) as tf:
            man = json.dumps(manifest_json, separators=(",", ":")).encode()
            tf.addfile(_tarinfo_bytes("manifest.json", len(man)), io.BytesIO(man))
            rep = json.dumps(repositories, separators=(",", ":")).encode()
            tf.addfile(_tarinfo_bytes("repositories", len(rep)), io.BytesIO(rep))
            tf.addfile(_tarinfo_bytes(config_name, len(config_bytes)), io.BytesIO(config_bytes))
            if progress:
                progress.add("pack", "打包写入归档", sum(os.path.getsize(p) for p in layer_paths))
            for i, p in enumerate(layer_paths, 1):
                arc = "layer%d/layer.tar" % i
                sz = os.path.getsize(p)
                with open(p, "rb") as f:
                    tf.addfile(_tarinfo_file(arc, sz), f)
                if progress:
                    progress.update("pack", sz)
            if progress:
                progress.finish("pack")
        if gz is not None:
            gz.close()
            gz = None
    finally:
        raw.close()
        if gz is not None:
            try:
                gz.close()
            except Exception:  # noqa: BLE001
                pass
    os.replace(tmp, out_path)


# --------------------------------------------------------------------------
# 归档校验
# --------------------------------------------------------------------------

def verify_archive(path: str, online_report: Optional[Dict[str, Any]] = None,
                   quiet: bool = False) -> Dict[str, Any]:
    """顺序读一遍归档，逐层重算 sha256 并比对 config.diff_ids。"""
    if not os.path.exists(path):
        raise DipError("文件不存在: %s" % path)
    res: Dict[str, Any] = {"file": os.path.abspath(path), "checks": [], "result": "PASS"}

    def check(name: str, ok: bool, detail: str = "") -> None:
        res["checks"].append({"name": name, "status": "PASS" if ok else "FAIL", "detail": detail})
        if not ok:
            res["result"] = "FAIL"
        emit("check", name=name, status="PASS" if ok else "FAIL", detail=detail)
        if not quiet:
            print("  [%s] %-30s %s" % ("PASS" if ok else "FAIL", name, detail))

    size = os.path.getsize(path)
    res["size"] = size
    if not quiet:
        print("  文件: %s  (%s)" % (path, human_bytes(size)))

    manifest: Optional[Dict[str, Any]] = None
    config_bytes: Optional[bytes] = None
    config_name: Optional[str] = None
    layer_digests: List[str] = []
    layer_sizes: List[int] = []
    seen = set()

    with tarfile.open(path, "r:*") as tf:
        n = 0
        for member in tf:
            if not member.isfile():
                continue
            n += 1
            name = member.name
            seen.add(name)
            if name == "manifest.json":
                manifest = json.loads(tf.extractfile(member).read().decode("utf-8"))
            elif name == "repositories":
                res["repositories"] = json.loads(tf.extractfile(member).read().decode("utf-8"))
            elif re.fullmatch(r"[0-9a-f]{64}\.json", name):
                config_name = name
                config_bytes = tf.extractfile(member).read()
            elif re.fullmatch(r"layer\d+/layer\.tar", name):
                h = hashlib.sha256()
                f = tf.extractfile(member)
                while True:
                    b = f.read(1024 * 1024)
                    if not b:
                        break
                    h.update(b)
                layer_digests.append(h.hexdigest())
                layer_sizes.append(member.size)
        res["entries"] = n

    check("归档可解析", True, "%d 个成员" % res.get("entries", 0))
    if manifest is None:
        check("manifest.json 存在", False, "")
        return res
    check("manifest.json 存在", True, "RepoTags=%s" % (manifest[0].get("RepoTags") if manifest else None))

    if config_bytes is None or config_name is None:
        check("config 存在", False, "")
        return res
    cfg_digest = hashlib.sha256(config_bytes).hexdigest()
    check("config 文件名与内容摘要一致", config_name == cfg_digest + ".json",
          "%s.json" % cfg_digest[:16])
    cfg = json.loads(config_bytes.decode("utf-8"))
    res["platform"] = {"os": cfg.get("os"), "architecture": cfg.get("architecture"),
                       "variant": cfg.get("variant")}
    res["config_digest"] = "sha256:" + cfg_digest

    m_layers = [l for l in (manifest[0].get("Layers") or [])]
    diff_ids = [d for d in ((cfg.get("rootfs") or {}).get("diff_ids") or [])]
    check("层数一致 (manifest vs config)",
          len(m_layers) == len(diff_ids) == len(layer_digests),
          "manifest=%d config=%d 归档=%d" % (len(m_layers), len(diff_ids), len(layer_digests)))
    check("manifest 引用的层齐全", all(l in seen for l in m_layers),
          "%d 层" % len(m_layers))

    bad = []
    for i, d in enumerate(diff_ids, 1):
        if split_digest(d)[1] != layer_digests[i - 1]:
            bad.append("layer%d" % i)
    check("每层 sha256 == config.diff_ids", not bad,
          "全部 %d 层匹配" % len(diff_ids) if not bad else "不匹配: %s" % ", ".join(bad))
    res["layer_diff_ids"] = ["sha256:" + d for d in diff_ids]
    res["layer_sizes"] = layer_sizes
    res["uncompressed_size"] = sum(layer_sizes)

    if online_report:
        res["online"] = online_report

    if not quiet:
        print("  平台: %s/%s  层数: %d  解压后大小: %s"
              % (res["platform"]["os"], res["platform"]["architecture"],
                 len(diff_ids), human_bytes(res["uncompressed_size"])))
    return res


# --------------------------------------------------------------------------
# 子命令: pull
# --------------------------------------------------------------------------

def cmd_pull(args: argparse.Namespace) -> int:
    image = ImageRef(args.image)
    os_, arch, variant = args.os, args.arch, args.variant
    if args.platform:
        parts = args.platform.split("/")
        if len(parts) < 2:
            raise DipError("--platform 格式应为 os/arch[/variant]，例如 linux/amd64")
        os_, arch = parts[0], parts[1]
        variant = parts[2] if len(parts) > 2 else None

    out = args.out
    if not out:
        out = "%s-%s-%s.tar.gz" % (image.file_stem(), os_, arch)
    if args.compress == "none" and out.endswith(".tar.gz"):
        out = out[:-3]
    out = os.path.abspath(out)
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)

    print("dip %s  —  %s" % (__version__, image))
    print("  目标: %s  平台: %s/%s%s  输出: %s"
          % (image.path, os_, arch, "/" + variant if variant else "", out))

    mirrors = load_mirrors(args.mirrors_file, args.mirror, args.no_builtin_mirrors)
    upstream = args.upstream or (image.upstream_base if not image.is_hub else DEFAULT_UPSTREAM)

    mirror_proxy = args.proxy if args.proxy_all else None
    apply_candidate_proxy(mirrors, mirror_proxy, args.proxy)
    _px_used = sorted({c.effective_proxy for c in mirrors if c.effective_proxy})
    if _px_used:
        print("      代理: %s  (按 mirrors.json / --proxy-all 决定哪些站走)" % ", ".join(_px_used))
    winner, capable = select_fastest(
        mirrors, image, image.reference, os_, arch, variant,
        top_k=args.top_k, probe_bytes=args.probe_bytes, probe_timeout=args.probe_timeout,
        proxy=mirror_proxy, jobs=args.jobs, bench_budget=args.bench_budget,
    )

    report: Dict[str, Any] = {
        "tool": "dip %s" % __version__,
        "generated_at": now_iso(),
        "image": str(image),
        "platform": {"os": os_, "architecture": arch, "variant": variant},
        "output": {"path": out, "format": "docker-archive", "compression": args.compress},
    }

    trust = trust_check(image, image.reference, capable, upstream, args.proxy,
                        args.expect_digest, os_, arch, variant, args.probe_timeout * 4)
    report["trust"] = trust

    # ---- 只有 manifest digest 与信任锚一致的镜像站才允许参与下载 ----
    expect = trust.get("expected_digest")
    others = sorted([c for c in capable if c is not winner],
                    key=lambda c: (-(c.speed or 0), c.rtt or 9e9))
    chain = [winner] + others
    verified_chain: List[Candidate] = []
    for c in chain:
        if expect and (c.manifest_digest or "") != expect:
            print("      忽略 %s：manifest digest 与信任锚不一致（疑似被污染或镜像已更新）" % c.name)
            continue
        verified_chain.append(c)
    if not verified_chain:
        raise DipError("没有任何镜像站的 manifest digest 与信任锚 %s 一致，拒绝下载" % str(expect)[:26])

    # ---- 拉取平台 manifest，同样支持跨站故障转移 ----
    first: Optional[Dict[str, Any]] = None
    mf: Optional[Dict[str, Any]] = None
    platform_manifest_digest = ""
    platform_manifest_computed = ""
    fetch_errors: List[str] = []
    # 取 manifest 也按「多轮 + 多站」重试。这一步在下载之前，一旦失败整轮拉取
    # 就直接退出，用户只能手工重敲命令 —— 在时通时断的链路上单轮失败率并不低。
    for _mf_round in range(3):
        if _mf_round:
            print("      manifest 第 %d/3 轮重试（上一轮全部失败）" % (_mf_round + 1),
                  flush=True)
            time.sleep(4.0 * _mf_round)
            fetch_errors = []
        for c in verified_chain:
            try:
                reg = Registry(c.url, image.path, timeout=args.timeout, retries=2,
                               proxy=c.effective_proxy)
                first = reg.fetch_manifest(image.reference, retries=2)
                doc, mt = first["json"], first["media_type"]
                if is_index(doc, mt):
                    entry = pick_platform(doc, os_, arch, variant)
                    chosen = reg.fetch_manifest(entry["digest"], retries=2)
                    mf = chosen["json"]
                    platform_manifest_digest = (chosen["digest_header"]
                                                or chosen["digest_computed"])
                    platform_manifest_computed = chosen["digest_computed"]
                else:
                    mf = doc
                    platform_manifest_digest = (first["digest_header"]
                                                or first["digest_computed"])
                    platform_manifest_computed = first["digest_computed"]
                if c is not winner:
                    print("      manifest 改从 %s 获取（%s 不可用）" % (c.name, winner.name))
                    winner = c
                break
            except Exception as e:  # noqa: BLE001
                fetch_errors.append("%s: %s" % (c.name, type(e).__name__))
                first = mf = None
                continue
        if mf is not None:
            break
    if first is None or mf is None:
        raise DipError("所有镜像站都拿不到 manifest（已重试 3 轮）:\n  "
                       + "\n  ".join(fetch_errors))

    config_descriptor = mf.get("config") or {}
    config_digest = config_descriptor.get("digest")
    if not config_digest:
        raise DipError("manifest 缺少 config digest")
    layers = mf.get("layers") or []
    if not layers:
        raise DipError("manifest 里没有任何层")

    report["digests"] = {
        "manifest_list": first["digest_header"] or first["digest_computed"],
        "manifest_list_computed": first["digest_computed"],
        "manifest": platform_manifest_digest,
        "manifest_computed": platform_manifest_computed,
        "config": config_digest,
        "layers": [l["digest"] for l in layers],
        "layer_sizes": [l.get("size") for l in layers],
    }
    emit("manifest", image=str(image), platform="%s/%s%s" % (os_, arch, "/" + variant if variant else ""),
         repo_tags=[] if not args.repo_tag else [args.repo_tag], digests=report["digests"],
         compressed_total=sum(int(l.get("size") or 0) for l in layers))

    report["trust"]["trust_anchor_digest"] = expect

    if args.plan_only:
        print("\n--plan-only: 选站完成，未下载。")
        return 0

    # ---------------- 工作目录 ----------------
    work = os.path.abspath(args.work_dir or (out + ".work"))
    blob_dir = os.path.join(work, "blobs")
    layer_dir = os.path.join(work, "layers")
    os.makedirs(blob_dir, exist_ok=True)
    os.makedirs(layer_dir, exist_ok=True)

    total_compressed = sum(int(l.get("size") or 0) for l in layers)
    print("[5/5] 下载并打包  (压缩层共 %s, 工作目录 %s)" % (human_bytes(total_compressed), work))
    emit("stage", name="下载并打包", step=5, total_steps=5,
         detail="压缩层共 %s" % human_bytes(total_compressed))
    free = shutil.disk_usage(work).free
    need = total_compressed * 3
    if free < need:
        eprint("警告: 可用空间 %s，预计需要约 %s（压缩层+解压层+成品），可能不够"
               % (human_bytes(free), human_bytes(need)))

    progress = Progress(enabled=not args.quiet)
    # 下载用的镜像站链：拿到 manifest 的那个站排第一，其余作为故障转移后备
    blob_order = [winner] + [c for c in verified_chain if c is not winner]
    blob_regs = [Registry(c.url, image.path, timeout=args.timeout, retries=2,
                          proxy=c.effective_proxy) for c in blob_order]
    blob_names = [c.name for c in blob_order]
    report["trust"]["download_chain"] = blob_names
    report["trust"]["manifest_source"] = winner.name
    if len(blob_order) > 1:
        print("      故障转移链: %s" % " -> ".join(blob_names))

    # ---- 多源并行分片的连接预算 ----
    # 总连接数 = 层并发 × 每 blob 源数。链路本就不稳，硬顶在 MAX_TOTAL_CONNS，
    # 需要更多源时优先压缩层并发，而不是把连接数堆上去。
    layer_jobs = max(1, int(args.jobs))
    want_sources = int(getattr(args, "sources", 0) or 0)
    if want_sources <= 0:
        want_sources = min(len(blob_order), DEFAULT_SOURCES)
    src_eff = max(1, min(want_sources, max(1, MAX_TOTAL_CONNS // layer_jobs),
                         len(blob_order), MAX_SOURCES))
    seg_eff = max(0, int(getattr(args, "segments", 0) or 0))
    if src_eff > 1:
        print("      多源并行: 每 blob 最多 %d 个源同时拉，层并发 %d（连接上限 %d）"
              % (src_eff, layer_jobs, src_eff * layer_jobs))
        if want_sources > src_eff:
            print("      注意: 请求 %d 个源，受连接上限压到 %d（可用 --jobs 调低层并发换取更多源）"
                  % (want_sources, src_eff))
    elif want_sources > 1:
        print("      多源并行未启用（层并发 %d × %d 源会超出 %d 连接上限）"
              % (layer_jobs, want_sources, MAX_TOTAL_CONNS))
    # ---- 动态源池参数 ----
    # 源池只在多源并行时才真正起作用；单流下载另有一套「超时先暂停」的冷却逻辑。
    use_gov = not bool(getattr(args, "no_governor", False))
    slow_ratio = float(getattr(args, "slow_ratio", GOV_SLOW_RATIO) or GOV_SLOW_RATIO)
    min_rate = max(0.0, float(getattr(args, "min_rate", GOV_MIN_RATE // 1024) or 0) * 1024.0)
    cooldown = float(getattr(args, "resume_cooldown", GOV_RESUME_COOLDOWN)
                     or GOV_RESUME_COOLDOWN)
    # 各站初始速率沿用前面实测的吞吐，源池据此决定先让谁上场
    initial_speeds = [float(c.speed or 0.0) for c in blob_order]
    if use_gov and src_eff > 1:
        print("      动态源池: 池中 %s 个站 / 同时在岗 %d 个；"
              "吞吐低于 %.0f%% 最快源或低于 %s 即暂停"
              % (min(len(blob_order), MAX_SOURCES), src_eff, slow_ratio * 100,
                 human_bytes(int(min_rate)) + "/s"))

    report["download"] = {
        "mode": "multi-source-parallel" if src_eff > 1 else "single-stream-failover",
        "sources_per_blob": src_eff,
        "sources_requested": want_sources,
        "segments_per_blob": seg_eff or "auto(2×源数，上限 %d)" % MAX_SEGMENTS,
        "layer_jobs": layer_jobs,
        "chain": blob_names,
        "governor": {
            "enabled": bool(use_gov and src_eff > 1),
            "slow_ratio": slow_ratio,
            "min_rate_bytes_per_sec": min_rate,
            "resume_cooldown_sec": cooldown,
        },
    }
    retry_budget = max(1, int(getattr(args, "retries", DEFAULT_NO_PROGRESS)
                              or DEFAULT_NO_PROGRESS))
    gov_lock = threading.Lock()
    gov_events: List[Dict[str, Any]] = []
    try:
        # 1) config
        cfg_path = os.path.join(blob_dir, "config.blob")
        download_blob(blob_regs, config_digest, cfg_path, "config", progress, "cfg",
                      expected_size=config_descriptor.get("size"), names=blob_names,
                      resume_cooldown=cooldown, no_progress_limit=retry_budget)
        with open(cfg_path, "rb") as f:
            config_bytes = f.read()
        alg, expect_hex = split_digest(config_digest)
        if not is_digest(alg, config_bytes, expect_hex):
            raise DipError("config blob 校验失败: %s" % config_digest)
        cfg = json.loads(config_bytes.decode("utf-8"))

        # 2) layers（并行）
        def job(idx_layer: Tuple[int, Dict[str, Any]]) -> Tuple[int, str, str]:
            i, l = idx_layer
            dg = l["digest"]
            dest = os.path.join(blob_dir, dg.replace(":", "_"))
            label = "层 %d/%d" % (i, len(layers))
            key = "L%d" % i
            gov_stats: Dict[str, Any] = {}

            def _gov_log(msg: str, _lb: str = label) -> None:
                print("      %s | %s" % (_lb, msg), flush=True)

            try:
                download_blob_multisource(blob_regs, dg, dest, label, progress, key,
                                          expected_size=l.get("size"), names=blob_names,
                                          sources=src_eff, segments=seg_eff,
                                          initial_speeds=initial_speeds,
                                          use_governor=use_gov,
                                          slow_ratio=slow_ratio, min_rate=min_rate,
                                          cooldown=cooldown,
                                          gov_log=_gov_log, stats=gov_stats,
                                          no_progress_limit=retry_budget)
            except Exception as e:  # noqa: BLE001
                progress.finish(key, "失败")
                if gov_stats.get("governor"):
                    with gov_lock:
                        gov_events.append(gov_stats["governor"])
                raise
            if gov_stats.get("governor"):
                with gov_lock:
                    gov_events.append(gov_stats["governor"])
            progress.finish(key)
            return i, dg, dest

        results: Dict[int, Tuple[str, str]] = {}
        errors: List[str] = []

        def run_layer_pass(items: List[Tuple[int, Dict[str, Any]]]) -> None:
            with concurrent.futures.ThreadPoolExecutor(max_workers=layer_jobs) as ex:
                futs = {ex.submit(job, (i, l)): i for i, l in enumerate(items, 1)}
                for fut in concurrent.futures.as_completed(futs):
                    try:
                        i, dg, dest = fut.result()
                        results[i] = (dg, dest)
                    except Exception as e:  # noqa: BLE001
                        errors.append("%s" % e)

        run_layer_pass([(i, l) for i, l in enumerate(layers, 1)])

        # 整层补下：一个 4.7GB 的层可能已经下到 90%，一次抖动就整张图失败太亏。
        # 重新构造 Registry 还有个副作用是好的：上一轮被标记 range_ok=False 的站
        # 会重新获得机会 —— Range 是「按 blob」的属性，换个层可能就支持了。
        layer_rounds = max(0, int(getattr(args, "layer_retries", LAYER_RETRY_ROUNDS)
                                  if getattr(args, "layer_retries", None) is not None
                                  else LAYER_RETRY_ROUNDS))
        for rnd in range(1, layer_rounds + 1):
            if not errors:
                break
            pending_layers = [(i, l) for i, l in enumerate(layers, 1)
                              if i not in results]
            if not pending_layers:
                break
            print("      第 %d/%d 轮补下：%d 个层上次失败，换新连接重试"
                  % (rnd, layer_rounds, len(pending_layers)), flush=True)
            time.sleep(min(10.0 * rnd, 30.0))
            errors = []
            blob_regs = [Registry(c.url, image.path, timeout=args.timeout, retries=2,
                                  proxy=c.effective_proxy) for c in blob_order]
            run_layer_pass(pending_layers)
        # 某层下载失败不一定致命：它的 layer.tar 可能早已解压好并通过校验
        # （典型的「打包/解压阶段中断」场景）。这里只对**确实还没搞定**的层报错，
        # 已经能靠 tar 复用的层放行 —— 否则已有的成果又会被整体放弃。
        if errors:
            _dids = (cfg.get("rootfs") or {}).get("diff_ids") or []
            fatal: List[str] = []
            for i in range(1, len(layers) + 1):
                if i in results:
                    continue
                tp = os.path.join(layer_dir, "layer%d.tar" % i)
                if _dids and os.path.exists(tp):
                    gh, _gz = sha256_file(tp)
                    if gh == split_digest(_dids[i - 1])[1]:
                        continue          # 有可用 tar，这一层不必重下
                fatal.append("层 %d" % i)
            if fatal:
                raise DipError("层下载失败:\n  " + "\n  ".join(errors[:5]))
            print("      有 %d 个层下载失败，但其 layer.tar 已在盘中且校验通过，"
                  "继续复用" % len(errors))

        # 汇总各层的源池统计：暂停了多少次慢源、主动关闭了多少次在途传输、
        # 每个站各扛了多少片 —— 这就是「动态调度确实在起作用」的证据。
        if gov_events:
            pauses = sum(int(s.get("pauses") or 0)
                         for e in gov_events for s in (e.get("sources") or []))
            kills = sum(int(s.get("kills_inflight") or 0)
                        for e in gov_events for s in (e.get("sources") or []))
            served: Dict[str, int] = {}
            for e in gov_events:
                for s in (e.get("sources") or []):
                    served[s["name"]] = served.get(s["name"], 0) + int(
                        s.get("segments_done") or 0)
            report["download"]["governor"].update({
                "pauses_total": pauses,
                "kills_inflight_total": kills,
                "segments_by_source": served,
            })
            print("      源池调度: 暂停慢源 %d 次，主动关闭在途传输 %d 次"
                  % (pauses, kills))
        progress.close()

        # 3) 逐层校验 + 解压
        print("      校验 %d 个层..." % len(layers))
        diff_ids = (cfg.get("rootfs") or {}).get("diff_ids") or []
        if diff_ids and len(diff_ids) != len(layers):
            raise DipError("config.diff_ids(%d) 与 manifest.layers(%d) 数量不一致"
                           % (len(diff_ids), len(layers)))
        layer_files: List[str] = []
        for i in range(1, len(layers) + 1):
            check_cancel()
            tar_path = os.path.join(layer_dir, "layer%d.tar" % i)
            d_alg, d_hex = (split_digest(diff_ids[i - 1]) if diff_ids
                            else ("", ""))
            dg = layers[i - 1]["digest"]           # manifest 里这一层的摘要
            dest = os.path.join(blob_dir, dg.replace(":", "_"))

            # 已解压好的 layerN.tar 若能通过 diff_id 校验，就直接复用，不碰压缩层。
            # 这救的正是「打包/解压阶段中断」这种情况：压缩层已按旧版本删掉，
            # 但解压产物完好（实测：37 层里已有 15 层 tar 校验通过、约 3.9 GB），
            # 没有这一步的话这些成果会被迫重下。
            if d_hex and os.path.exists(tar_path):
                got_hex, got_sz = sha256_file(tar_path)
                if got_hex == d_hex:
                    print("      layer%d  解压产物已在盘中且校验通过，直接复用 (%s)"
                          % (i, human_bytes(got_sz)))
                    layer_files.append(tar_path)
                    continue
                print("      layer%d  已有 layer.tar 但校验不符，重新解压" % i)

            # 没有可用 tar -> 这一层必须靠压缩 blob 产出，缺了就报错说清楚要求重下
            if dg is None or not os.path.exists(dest):
                raise DipError(
                    "层 %d 缺少可用的压缩数据（%s 不存在），且没有可复用的 layer.tar。\n"
                    "  该层需要重新下载；已下载的其它层会保留，重跑时会自动复用。"
                    % (i, dg or "blob"))
            alg, expect_hex = split_digest(dg)
            actual_hex, sz = sha256_file(dest)
            if actual_hex != expect_hex:
                raise DipError("层 %d 内容校验失败\n  expect %s\n  actual sha256:%s"
                               % (i, dg, actual_hex))
            kind = layer_kind((layers[i - 1].get("media_type") or ""), dest)
            decompress_layer(dest, tar_path, kind)
            if d_hex:
                got_hex, got_sz = sha256_file(tar_path)
                if got_hex != d_hex:
                    raise DipError("层 %d 解压后校验失败 (diff_id 不匹配)\n  expect %s\n  actual sha256:%s"
                                   % (i, diff_ids[i - 1], got_hex))
                note = "OK  %s" % human_bytes(got_sz)
            else:
                note = "OK  (config 无 diff_ids，仅做内容寻址校验)"
            print("      layer%d  sha256 %s  %s" % (i, expect_hex[:16], note))
            # 默认**保留**压缩层字节。这不是为了省事：删掉它等于把这张镜像的
            # 下载进度清零 —— 下次再跑同一个输出名，37 层全部重下（实测踩过：
            # 已下 15 层 / 若干 GB，因为这一行被删光，重跑从 0 开始）。
            # 压缩层是内容寻址的，留着就能在下次运行时被 _reusable() 直接认出来复用。
            # 用完想回收空间再显式 --clean-blobs（或事后手动删 .work）。
            if args.clean_blobs:
                try:
                    os.remove(dest)
                except OSError:
                    pass
            layer_files.append(tar_path)

        # 4) 打包
        repo_tag = args.repo_tag or image.repo_tag()
        name, _, tag = repo_tag.rpartition(":")
        if not name or not tag:
            name, tag = repo_tag, "latest"
        config_name = hashlib.sha256(config_bytes).hexdigest() + ".json"
        manifest_json = [{
            "Config": config_name,
            "RepoTags": ["%s:%s" % (name, tag)],
            "Layers": ["layer%d/layer.tar" % i for i in range(1, len(layer_files) + 1)],
        }]
        repositories = {name: {tag: config_name[:-5]}}

        print("      打包 -> %s" % out)
        build_archive(out, args.compress, args.compress_level, manifest_json,
                      repositories, config_name, config_bytes, layer_files, progress)

        out_sha, out_size = sha256_file(out)
        report["output"]["size"] = out_size
        report["output"]["sha256"] = out_sha
        report["output"]["repo_tags"] = manifest_json[0]["RepoTags"]
        report["output"]["uncompressed_layer_bytes"] = sum(os.path.getsize(p) for p in layer_files)
        report["verification"] = {
            "blob_sha256_matches_manifest": True,
            "layer_diff_ids_match_config": bool(diff_ids),
            "layer_count": len(layers),
            "config_digest_verified": True,
        }

        # 5) 自校验（必须在写报告之前，否则自校验结论进不了报告）
        if not args.no_verify:
            print("\n  归档自校验:")
            v = verify_archive(out, quiet=False)
            report["self_check"] = {"result": v["result"], "checks": v["checks"]}
            if v["result"] != "PASS":
                raise DipError("成品归档自校验未通过，请勿使用")

        # 6) 落盘报告
        if not args.no_report:
            sha_path = out + ".sha256"
            # 必须用二进制写、显式 LF：Windows 文本模式会写成 CRLF，
            # 而 GNU sha256sum 解析文件名时会连 \r 一起吃进去，导致校验报 "FAILED open or read"。
            with open(sha_path, "wb") as f:
                f.write(("%s  %s\n" % (out_sha, os.path.basename(out))).encode("ascii"))
            rep_path = out + ".verify.json"
            with open(rep_path, "w", encoding="utf-8", newline="\n") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
                f.write("\n")
            print("      校验清单: %s" % sha_path)
            print("      完整性报告: %s" % rep_path)
            report["report_path"] = rep_path

        # 工作目录只在**成功**之后、且用户没要求保留时才清理。
        # 失败时务必留下：里面是下次续传的本钱（已下的压缩层 + 已解压的 tar）。
        # 以前无论成功失败都在 finally 里 rmtree，导致「重跑 = 从零重下」。
        if args.keep_work:
            print("      工作目录保留: %s" % work)
        else:
            shutil.rmtree(work, ignore_errors=True)

        print("\n完成  %s  (%s)" % (out, human_bytes(out_size)))
        print("  sha256 : %s" % out_sha)
        print("  镜像   : %s  (%s/%s)" % (manifest_json[0]["RepoTags"][0], os_, arch))
        print("  信任锚 : %s (置信度 %s)" % (trust.get("basis"), trust.get("confidence")))
        print("  导入   : docker load -i %s" % os.path.basename(out))
        emit("done",
             ok=True,
             out=out,
             size=out_size,
             sha256=out_sha,
             repo_tag=manifest_json[0]["RepoTags"][0],
             platform="%s/%s" % (os_, arch),
             trust_basis=trust.get("basis"),
             trust_confidence=trust.get("confidence"),
             governor=report["download"].get("governor"),
             layer_count=len(layers),
             uncompressed_bytes=report["output"]["uncompressed_layer_bytes"],
             import_cmd="docker load -i %s" % os.path.basename(out))
        return 0
    finally:
        progress.close()


# --------------------------------------------------------------------------
# 子命令: verify
# --------------------------------------------------------------------------

def cmd_verify(args: argparse.Namespace) -> int:
    print("dip %s  —  校验归档" % __version__)
    emit("stage", name="校验归档", step=1, total_steps=1,
         detail=os.path.basename(args.archive))
    online = None
    if args.online or args.expect_digest or args.image:
        image = None
        if args.image:
            image = ImageRef(args.image)
        else:
            resp = None
            for cand in (args.archive + ".verify.json",
                         args.archive.replace(".tar.gz", "").replace(".tar", "") + ".verify.json"):
                if os.path.exists(cand):
                    with open(cand, "r", encoding="utf-8") as f:
                        resp = json.load(f)
                    break
            if resp and resp.get("image"):
                image = ImageRef(resp["image"])
        if image is None:
            eprint("提示: 未提供 --image 且旁边没有 .verify.json，跳过联网复核")
        else:
            mirrors = load_mirrors(args.mirrors_file, args.mirror, args.no_builtin_mirrors)
            apply_candidate_proxy(mirrors, args.proxy if args.proxy_all else None, args.proxy)
            for c in mirrors:
                handshake(c, args.probe_timeout)
            alive = [c for c in mirrors if c.rtt is not None]
            for c in alive:
                qualify(c, image, image.reference, args.os, args.arch, args.variant,
                        args.probe_timeout * 2.5, args.proxy if args.proxy_all else None)
            capable = [c for c in alive if c.capable]
            if capable:
                online = trust_check(image, image.reference, capable, 
                                     args.upstream or DEFAULT_UPSTREAM, args.proxy,
                                     args.expect_digest, args.os, args.arch, args.variant,
                                     args.probe_timeout * 4)
            else:
                eprint("提示: 当前没有可用镜像站，跳过联网复核")
    res = verify_archive(args.archive, online_report=online)
    print("\n结论: %s" % res["result"])
    if online:
        print("  信任锚: %s (置信度 %s)" % (online.get("basis"), online.get("confidence")))
        for n in online.get("notes") or []:
            print("  注意: %s" % n)
    emit("done", ok=(res["result"] == "PASS"), kind="verify", verdict=res["result"],
         out=res["file"], size=res.get("size"), checks=res["checks"],
         online=online, uncompressed_bytes=res.get("uncompressed_size"),
         layer_count=len(res.get("layer_diff_ids") or []))
    return 0 if res["result"] == "PASS" else 1


# --------------------------------------------------------------------------
# 子命令: bench / mirrors
# --------------------------------------------------------------------------

def cmd_bench(args: argparse.Namespace) -> int:
    raw = args.image or "alpine:3.20"
    image = ImageRef(raw)
    print("dip %s  —  镜像站测速 (以 %s 为样本)" % (__version__, image))
    mirrors = load_mirrors(args.mirrors_file, args.mirror, args.no_builtin_mirrors)
    apply_candidate_proxy(mirrors, args.proxy if args.proxy_all else None, args.proxy)
    os_, arch, variant = args.os, args.arch, args.variant
    t0 = time.time()
    winner, capable = select_fastest(
        mirrors, image, image.reference, os_, arch, variant,
        top_k=args.top_k, probe_bytes=args.probe_bytes, probe_timeout=args.probe_timeout,
        proxy=args.proxy if args.proxy_all else None, jobs=args.jobs,
        bench_budget=args.bench_budget,
    )
    print("\n排名（按实测吞吐）:")
    order = sorted(capable, key=lambda c: (-(c.speed or 0), c.rtt or 9e9))
    for i, c in enumerate(order, 1):
        mark = "  <== 最快" if c is winner else ""
        sp = "%.2f MB/s" % ((c.speed or 0) / MB) if c.speed else "测速失败"
        print("  %d) %-12s %-34s rtt %5.0fms  %s%s"
              % (i, c.name, c.url, (c.rtt or 0) * 1000, sp, mark))
    print("\n耗时 %.1fs" % (time.time() - t0))
    print("强制只用该站: --mirror %s --no-builtin-mirrors" % winner.url)
    emit("done", ok=True, kind="bench", out="", sha256="",
         ranking=[{"name": c.name, "url": c.url, "speed": c.speed, "rtt": c.rtt,
                   "selected": c is winner} for c in order])
    return 0


def cmd_mirrors(args: argparse.Namespace) -> int:
    print("dip %s  —  镜像站候选" % __version__)
    mirrors = load_mirrors(args.mirrors_file, args.mirror, args.no_builtin_mirrors)
    apply_candidate_proxy(mirrors, args.proxy if args.proxy_all else None, args.proxy)
    for c in mirrors:
        handshake(c, args.probe_timeout)
    for c in mirrors:
        st = ("%6.0f ms" % (c.rtt * 1000)) if c.rtt else ("不可达  %s" % (c.error or ""))
        print("  %-12s %-38s %s" % (c.name, c.url, st))
    print("\n配置来源: 内置默认 -> mirrors.json -> --mirror")
    return 0


# --------------------------------------------------------------------------
# Web 界面
#
# 设计：Web 层只是 CLI 的一层皮 —— 前端表单 -> 拼出 argv -> 交给同一套
# argparse 解析 -> 调用同一个 args.func。所以界面上的行为与命令行完全一致，
# 默认值、校验规则、报错信息都只有一份实现。
# --------------------------------------------------------------------------

WEB_PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>dip · Docker 镜像离线下载器</title>
<style>
  :root{
    --bg:#f5f6f8; --panel:#fff; --line:#e4e7ec; --text:#1c2430; --muted:#6b7480;
    --accent:#2563eb; --accent-soft:#eef3ff; --ok:#15803d; --ok-soft:#eaf7ee;
    --fail:#c02626; --fail-soft:#fdeeee; --warn:#b45309; --warn-soft:#fdf5e6;
    --mono:"Cascadia Mono","Consolas","SFMono-Regular","Menlo",monospace;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);
       font:14px/1.55 -apple-system,"Segoe UI","Microsoft YaHei",system-ui,sans-serif}
  header{background:#fff;border-bottom:1px solid var(--line);padding:12px 22px;
         display:flex;align-items:center;gap:12px;position:sticky;top:0;z-index:9}
  header h1{font-size:16px;margin:0;font-weight:650;letter-spacing:.2px}
  header .ver{font-size:12px;color:var(--muted);font-family:var(--mono)}
  header .spacer{flex:1}
  .dot{width:8px;height:8px;border-radius:50%;background:#c9ced6;display:inline-block}
  .dot.run{background:var(--accent);animation:pulse 1.2s infinite}
  .dot.done{background:var(--ok)} .dot.fail{background:var(--fail)}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}
  main{display:grid;grid-template-columns:400px minmax(0,1fr);gap:16px;
       padding:16px 22px 40px;align-items:start;max-width:1600px}
  @media(max-width:1080px){main{grid-template-columns:1fr}}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;
        margin-bottom:14px;overflow:hidden}
  .card > h2{margin:0;padding:10px 14px;font-size:13px;font-weight:650;
             border-bottom:1px solid var(--line);background:#fafbfc;
             display:flex;align-items:center;gap:8px}
  .card > .body{padding:14px}
  label{display:block;font-size:12px;color:var(--muted);margin:0 0 4px}
  .field{margin-bottom:11px}
  input[type=text],input[type=number],select{
    width:100%;padding:7px 9px;border:1px solid var(--line);border-radius:7px;
    font:13px var(--mono);background:#fff;color:var(--text)}
  input:focus,select:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-soft)}
  .row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
  .row3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px}
  .hint{font-size:11.5px;color:var(--muted);margin-top:4px}
  button{font:13px inherit;padding:8px 14px;border-radius:7px;border:1px solid var(--line);
         background:#fff;color:var(--text);cursor:pointer;transition:.12s}
  button:hover{border-color:#c3cad4;background:#fafbfc}
  button:disabled{opacity:.45;cursor:not-allowed}
  button.primary{background:var(--accent);border-color:var(--accent);color:#fff;font-weight:600}
  button.primary:hover{background:#1d4ed8;border-color:#1d4ed8}
  button.danger{color:var(--fail);border-color:#f0c9c9}
  .actions{display:flex;gap:8px;margin-top:4px}
  .actions button{flex:1}
  table{width:100%;border-collapse:collapse;font-size:12.5px}
  th{text-align:left;color:var(--muted);font-weight:600;font-size:11.5px;
     padding:6px 8px;border-bottom:1px solid var(--line);white-space:nowrap}
  td{padding:6px 8px;border-bottom:1px solid #f1f3f5;vertical-align:middle}
  tr:last-child td{border-bottom:none}
  td.mono,.mono{font-family:var(--mono);font-size:12px}
  .tag{display:inline-block;padding:1px 7px;border-radius:20px;font-size:11px;font-weight:600}
  .tag.ok{background:var(--ok-soft);color:var(--ok)}
  .tag.fail{background:var(--fail-soft);color:var(--fail)}
  .tag.warn{background:var(--warn-soft);color:var(--warn)}
  .tag.info{background:var(--accent-soft);color:var(--accent)}
  .tag.mute{background:#f1f3f5;color:var(--muted)}
  .bar{height:7px;background:#eef0f3;border-radius:5px;overflow:hidden;margin-top:3px}
  .bar > i{display:block;height:100%;background:var(--accent);border-radius:5px;transition:width .3s}
  .bar.ok > i{background:var(--ok)}
  .prog{margin-bottom:9px}
  .prog .top{display:flex;justify-content:space-between;font-size:12px;gap:10px}
  .prog .top .l{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .prog .top .r{color:var(--muted);font-family:var(--mono);font-size:11.5px;white-space:nowrap}
  .steps{display:flex;gap:6px;flex-wrap:wrap}
  .step{font-size:11.5px;padding:3px 9px;border-radius:20px;background:#f1f3f5;color:var(--muted)}
  .step.cur{background:var(--accent);color:#fff;font-weight:600}
  .step.past{background:var(--ok-soft);color:var(--ok)}
  pre.log{margin:0;padding:12px 14px;background:#fbfcfd;border-top:1px solid var(--line);
          max-height:340px;overflow:auto;font:11.5px/1.6 var(--mono);color:#3a4250;
          white-space:pre-wrap;word-break:break-all}
  .kv{display:grid;grid-template-columns:auto 1fr;gap:4px 12px;font-size:12.5px}
  .kv dt{color:var(--muted)} .kv dd{margin:0;font-family:var(--mono);font-size:12px;word-break:break-all}
  .copy{font-size:11px;padding:2px 7px;margin-left:6px;border-radius:5px}
  .muted{color:var(--muted)}
  .empty{color:var(--muted);font-size:13px;padding:14px;text-align:center}
  .checkline{display:flex;gap:9px;padding:4px 0;font-size:12.5px;align-items:baseline}
  .checkline .st{font-family:var(--mono);font-size:11px;font-weight:700;flex:0 0 34px}
  .checkline .st.PASS{color:var(--ok)} .checkline .st.FAIL{color:var(--fail)}
  .checkline .nm{flex:0 0 auto} .checkline .dt{color:var(--muted);font-size:11.5px}
  .banner{padding:10px 14px;border-radius:8px;font-size:13px;margin-bottom:14px;display:none}
  .banner.show{display:block}
  .banner.ok{background:var(--ok-soft);color:#14532d;border:1px solid #c9e9d4}
  .banner.fail{background:var(--fail-soft);color:#7f1d1d;border:1px solid #f3cfcf}
  .banner.warn{background:var(--warn-soft);color:#78350f;border:1px solid #f0dcb4}
  .fl{display:flex;align-items:center;gap:7px;font-size:12.5px;color:var(--text);margin-bottom:8px}
  .fl input{width:auto}
  .file{display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid #f1f3f5;font-size:12.5px}
  .file:last-child{border-bottom:none}
  .file .nm{flex:1;font-family:var(--mono);font-size:11.5px;word-break:break-all}
</style>
</head>
<body>
<header>
  <span class="dot" id="statusDot"></span>
  <h1>Docker 镜像离线下载器</h1>
  <span class="ver" id="verTag">dip</span>
  <span class="spacer"></span>
  <span class="ver" id="stageTag"></span>
</header>
<main>
  <div>
    <div class="card">
      <h2>下载任务</h2>
      <div class="body">
        <div class="field">
          <label>镜像名</label>
          <input type="text" id="image" value="alpine:3.20" placeholder="nginx:1.27-alpine">
        </div>
        <div class="row">
          <div class="field">
            <label>操作系统</label>
            <select id="os"><option>linux</option><option>windows</option></select>
          </div>
          <div class="field">
            <label>架构</label>
            <select id="arch">
              <option>amd64</option><option>arm64</option>
              <option>arm</option><option>386</option><option>ppc64le</option>
            </select>
          </div>
        </div>
        <div class="field">
          <label>输出文件（留空自动命名）</label>
          <input type="text" id="out" placeholder="自动：&lt;名称&gt;-&lt;标签&gt;-&lt;平台&gt;.tar.gz">
        </div>
        <div class="row3">
          <div class="field">
            <label>压缩</label>
            <select id="compress"><option value="gzip">gzip</option><option value="none">不压缩</option></select>
          </div>
          <div class="field">
            <label>并发层数</label>
            <input type="number" id="jobs" value="3" min="1" max="8">
          </div>
          <div class="field">
            <label>并行源数（0=自动）</label>
            <input type="number" id="sources" value="0" min="0" max="8">
          </div>
        </div>
        <div class="row3">
          <div class="field">
            <label>分片数（0=自动）</label>
            <input type="number" id="segments" value="0" min="0" max="8">
          </div>
          <div class="field">
            <label>测速采样 KB</label>
            <input type="number" id="probeKb" value="512" min="64" step="64">
          </div>
        </div>
        <div class="row3">
          <div class="field">
            <label>慢源阈值 %（0=关源池）</label>
            <input type="number" id="slowRatio" value="25" min="0" max="100">
          </div>
          <div class="field">
            <label>复活冷却 秒</label>
            <input type="number" id="cooldown" value="15" min="3" max="120">
          </div>
        </div>
        <div class="field">
          <label>权威 manifest digest（可选，用于官方比对）</label>
          <input type="text" id="expect" placeholder="sha256:…">
        </div>
        <div class="fl"><input type="checkbox" id="offline"><label style="margin:0">跳过联网复核（只装包不比对）</label></div>
        <div class="actions">
          <button class="primary" id="btnPull">开始下载</button>
          <button id="btnBench">只测速</button>
          <button class="danger" id="btnCancel" disabled>取消</button>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>已有归档</h2>
      <div class="body" id="archiveBox"><div class="empty">载入中…</div></div>
    </div>
  </div>

  <div>
    <div class="banner" id="banner"></div>

    <div class="card" id="resCard" style="display:none">
      <h2>结果</h2>
      <div class="body" id="resBody"></div>
    </div>

    <div class="card">
      <h2>阶段</h2>
      <div class="body"><div class="steps" id="steps"></div></div>
    </div>

    <div class="card">
      <h2>镜像站 <span class="muted" id="mirrorNote" style="font-weight:400"></span></h2>
      <div class="body" style="padding:0"><table id="mirrorTable"></table></div>
    </div>

    <div class="card" id="trustCard" style="display:none">
      <h2>完整性比对（信任锚）</h2>
      <div class="body"><dl class="kv" id="trustBody"></dl></div>
    </div>

    <div class="card" id="progCard" style="display:none">
      <h2>传输进度</h2>
      <div class="body" id="progBody"></div>
    </div>

    <div class="card" id="checkCard" style="display:none">
      <h2>校验清单</h2>
      <div class="body" id="checkBody"></div>
    </div>

    <div class="card">
      <h2>运行日志</h2>
      <pre class="log" id="log">就绪。填好参数后点「开始下载」。</pre>
    </div>
  </div>
</main>
<script>
var STEPS = ["探测镜像站连通性","校验镜像可用性","实测下载速度","完整性信任锚比对","下载并打包"];
var jobId = null, timer = null, logLen = 0, lastMirrors = "";

function $(id){return document.getElementById(id)}
function esc(s){return String(s==null?"":s).replace(/[&<>]/g,function(c){
  return {"&":"&amp;","<":"&lt;",">":"&gt;"}[c]})}
function fmtBytes(n){
  if(n==null||isNaN(n)) return "?";
  var u=["B","KiB","MiB","GiB","TiB"],i=0;n=Number(n);
  while(n>=1024&&i<u.length-1){n/=1024;i++}
  return (i===0?Math.round(n):n.toFixed(1))+" "+u[i];
}
function banner(cls,html){
  var b=$("banner");
  if(!cls){b.className="banner";b.innerHTML="";return}
  b.className="banner show "+cls; b.innerHTML=html;
}
function renderMirrors(rows){
  var key = JSON.stringify(rows);
  if(key===lastMirrors) return; lastMirrors=key;
  var t=$("mirrorTable");
  if(!rows||!rows.length){t.innerHTML='<tr><td class="empty">等待探测…</td></tr>';return}
  var h='<tr><th>镜像站</th><th>地址</th><th>延迟</th><th>实测速度</th><th>状态</th></tr>';
  rows.forEach(function(m){
    var st;
    if(m.selected) st='<span class="tag info">★ 已选用</span>';
    else if(m.capable) st='<span class="tag ok">可用</span>';
    else if(m.rtt!=null) st='<span class="tag warn">无此镜像</span>';
    else st='<span class="tag fail">不可达</span>';
    var sp = m.speed>0 ? "<b>"+ (m.speed/1048576).toFixed(2) +" MB/s</b>"
                       : '<span class="muted">'+(m.rtt!=null?"未测出":"-")+'</span>';
    h+='<tr><td><b>'+esc(m.name)+'</b></td>'
      +'<td class="mono muted" style="font-size:11px">'+esc(m.url)+'</td>'
      +'<td class="mono">'+(m.rtt!=null?(m.rtt*1000).toFixed(0)+" ms":"-")+'</td>'
      +'<td>'+sp+'</td><td>'+st+'</td></tr>';
  });
  t.innerHTML=h;
}
function renderSteps(step){
  var h="";STEPS.forEach(function(s,i){
    var n=i+1, cls = n<step?"past":(n===step?"cur":"");
    h+='<span class="step '+cls+'">'+n+'. '+s+'</span>';
  });
  $("steps").innerHTML=h;
}
function renderTrust(t){
  if(!t||!t.basis){$("trustCard").style.display="none";return}
  $("trustCard").style.display="";
  var BASIS={ "user-provided-digest":"用户指定 digest",
              "official-registry":"官方 registry",
              "cross-mirror-consensus":"多镜像站共识",
              "single-mirror-only":"单站自洽（无法交叉验证）" };
  var CONF={high:'<span class="tag ok">高</span>',medium:'<span class="tag warn">中</span>',
            low:'<span class="tag fail">低</span>'};
  var h="";
  h+="<dt>信任锚</dt><dd>"+esc(BASIS[t.basis]||t.basis)+"</dd>";
  h+="<dt>置信度</dt><dd>"+(CONF[t.confidence]||esc(t.confidence||"-"))+"</dd>";
  if(t.official_registry) h+="<dt>官方源</dt><dd>"+(t.official_registry.reachable
      ? esc(t.official_registry.manifest_digest||"可达") : '<span class="muted">不可达 · '+esc(t.official_registry.error||"")+'</span>')+"</dd>";
  if(t.expected_digest) h+="<dt>基准 digest</dt><dd>"+esc(t.expected_digest)+"</dd>";
  if(t.consensus_count) h+="<dt>一致站数</dt><dd>"+t.consensus_count+"</dd>";
  if(t.mirrors_compared&&t.mirrors_compared.length) h+="<dt>参与比对</dt><dd>"+esc(t.mirrors_compared.join(", "))+"</dd>";
  if(t.download_chain&&t.download_chain.length) h+="<dt>下载链</dt><dd>"+esc(t.download_chain.join(" → "))+"</dd>";
  (t.notes||[]).forEach(function(n){h+="<dt>注意</dt><dd>"+esc(n)+"</dd>"});
  $("trustBody").innerHTML=h;
}
function renderProgress(rows){
  if(!rows||!rows.length){$("progCard").style.display="none";return}
  $("progCard").style.display="";
  var total=0,done=0;
  rows.forEach(function(r){total+=r.total||0;done+=r.done||0});
  var h="";
  if(total>0){
    var pct=Math.min(100,done*100/total);
    h+='<div class="prog"><div class="top"><span class="l"><b>总进度</b></span>'
      +'<span class="r">'+fmtBytes(done)+' / '+fmtBytes(total)+' · '+pct.toFixed(1)+'%</span></div>'
      +'<div class="bar'+(pct>=100?' ok':'')+'"><i style="width:'+pct+'%"></i></div></div>';
  }
  rows.forEach(function(r){
    var p = r.total>0 ? Math.min(100,r.done*100/r.total) : 0;
    h+='<div class="prog"><div class="top"><span class="l">'+esc(r.label)+'</span>'
      +'<span class="r">'+fmtBytes(r.done)+(r.total?" / "+fmtBytes(r.total):"")+'</span></div>'
      +'<div class="bar'+(p>=100?' ok':'')+'"><i style="width:'+p+'%"></i></div></div>';
  });
  $("progBody").innerHTML=h;
}
function renderChecks(checks){
  if(!checks||!checks.length){$("checkCard").style.display="none";return}
  $("checkCard").style.display="";
  $("checkBody").innerHTML = checks.map(function(c){
    return '<div class="checkline"><span class="st '+c.status+'">'+c.status+'</span>'
      +'<span class="nm">'+esc(c.name)+'</span><span class="dt">'+esc(c.detail||"")+'</span></div>';
  }).join("");
}
function renderLog(lines){
  if(!lines) return;
  var el=$("log");
  if(lines.length < logLen){logLen=0;el.textContent=""}
  if(lines.length===logLen) return;
  var add=lines.slice(logLen); logLen=lines.length;
  el.textContent += (el.textContent && el.textContent!=="就绪。填好参数后点「开始下载」。" ? "\n":"")+add.join("\n");
  el.scrollTop = el.scrollHeight;
}
function renderResult(r){
  if(!r || (!r.sha256 && !r.ranking && r.kind!=="verify")){
    $("resCard").style.display="none"; return;
  }
  $("resCard").style.display="";
  var h='<dl class="kv">';
  if(r.kind==="verify"){
    h+='<dt>结论</dt><dd>'+(r.verdict==="PASS"
        ?'<span class="tag ok">PASS</span>':"<span class=\"tag fail\">FAIL</span>")+'</dd>';
    h+='<dt>文件</dt><dd>'+esc(r.out)+'</dd>';
    h+='<dt>大小</dt><dd>'+fmtBytes(r.size)+'</dd>';
    h+='<dt>层数</dt><dd>'+(r.layer_count||0)+'</dd>';
    h+='<dt>解压后</dt><dd>'+fmtBytes(r.uncompressed_bytes)+'</dd>';
    if(r.online){
      h+='<dt>信任锚</dt><dd>'+esc(r.online.basis||"-")+' / '+esc(r.online.confidence||"-")+'</dd>';
      if(r.online.expected_digest) h+='<dt>基准 digest</dt><dd>'+esc(r.online.expected_digest)+'</dd>';
      (r.online.notes||[]).forEach(function(n){h+='<dt>注意</dt><dd>'+esc(n)+'</dd>'});
    }
  } else if(r.sha256){
    h+='<dt>镜像</dt><dd>'+esc(r.repo_tag)+'</dd>';
    h+='<dt>文件</dt><dd>'+esc(r.out)+'</dd>';
    h+='<dt>大小</dt><dd>'+fmtBytes(r.size)+'</dd>';
    h+='<dt>解压后</dt><dd>'+fmtBytes(r.uncompressed_bytes)+'</dd>';
    h+='<dt>层数</dt><dd>'+r.layer_count+'</dd>';
    h+='<dt>信任锚</dt><dd>'+esc(r.trust_basis)+' / '+esc(r.trust_confidence)+'</dd>';
    if(r.governor&&r.governor.enabled){
      h+='<dt>源池</dt><dd>暂停慢源 '+esc(r.governor.pauses_total||0)+' 次 · 主动关闭在途传输 '
         +esc(r.governor.kills_inflight_total||0)+' 次</dd>';
      var segMap=r.governor.segments_by_source||{}, segParts=[];
      for(var gk in segMap){ if(segMap[gk]>0) segParts.push(gk+' '+segMap[gk]+' 片'); }
      if(segParts.length) h+='<dt>分片归属</dt><dd>'+esc(segParts.join(' · '))+'</dd>';
    }
    h+='<dt>sha256</dt><dd>'+esc(r.sha256)
      +'<button class="copy" onclick="copyText(\''+esc(r.sha256)+'\')">复制</button></dd>';
    h+='<dt>导入命令</dt><dd>'+esc(r.import_cmd)
      +'<button class="copy" onclick="copyText(\''+esc(r.import_cmd)+'\')">复制</button></dd>';
  } else {
    h+='<dt>测速</dt><dd>完成，见上方镜像站表格（★ 为选用）</dd>';
  }
  h+='</dl>';
  $("resBody").innerHTML=h;
}
function copyText(t){
  if(navigator.clipboard&&navigator.clipboard.writeText){navigator.clipboard.writeText(t)}
  else{var a=document.createElement("textarea");a.value=t;document.body.appendChild(a);a.select();
       document.execCommand("copy");document.body.removeChild(a)}
}
function setRunning(on){
  $("btnPull").disabled=on; $("btnBench").disabled=on; $("btnCancel").disabled=!on;
  $("statusDot").className="dot"+(on?" run":"");
  ["image","os","arch","out","compress","jobs","probeKb","expect"].forEach(function(id){
    $(id).disabled=on});
}
function commonArgv(){
  var a=[];
  if($("out").value.trim()) a.push("-o",$("out").value.trim());
  var os=$("os").value, arch=$("arch").value;
  if(os!=="linux"||arch!=="amd64") a.push("--platform",os+"/"+arch);
  a.push("--compress",$("compress").value);
  a.push("--jobs",String($("jobs").value||3));
  var src=parseInt($("sources").value||"0",10);
  if(src>0) a.push("--sources",String(src));
  var seg=parseInt($("segments").value||"0",10);
  if(seg>0) a.push("--segments",String(seg));
  var sr=parseInt($("slowRatio").value||"25",10);
  if(sr<=0){ a.push("--no-governor"); } else { a.push("--slow-ratio",String(sr/100)); }
  var cd=parseInt($("cooldown").value||"15",10);
  if(cd>0) a.push("--resume-cooldown",String(cd));
  var kb=parseInt($("probeKb").value||"512",10);
  if(kb>0) a.push("--probe-bytes",String(kb*1024));
  if($("expect").value.trim()) a.push("--expect-digest",$("expect").value.trim());
  return a;
}
function start(kind){
  var img=$("image").value.trim();
  if(!img){banner("fail","请填写镜像名");return}
  var argv = kind==="bench" ? ["bench",img].concat(commonArgv())
                            : ["pull",img].concat(commonArgv());
  if($("offline").checked) argv.push("--no-verify");
  banner(null);
  $("resCard").style.display="none"; $("trustCard").style.display="none";
  $("progCard").style.display="none"; $("checkCard").style.display="none";
  $("log").textContent=""; logLen=0; lastMirrors="";
  setRunning(true);
  fetch("/api/run",{method:"POST",headers:{"Content-Type":"application/json"},
      body:JSON.stringify({argv:argv})})
    .then(function(r){return r.json()})
    .then(function(d){
      if(d.error){banner("fail",esc(d.error));setRunning(false);return}
      jobId=d.job; if(timer) clearInterval(timer);
      timer=setInterval(poll,450); poll();
    })
    .catch(function(e){banner("fail","请求失败: "+esc(e));setRunning(false)});
}
function poll(){
  if(!jobId) return;
  fetch("/api/state?job="+encodeURIComponent(jobId))
    .then(function(r){return r.json()})
    .then(function(d){
      var j=d.job; if(!j) return;
      $("stageTag").textContent = j.stage||"";
      if(j.step) renderSteps(j.step);
      renderMirrors(j.mirrors);
      renderTrust(j.trust);
      renderProgress(j.progress);
      renderChecks(j.checks);
      renderLog(j.log);
      renderResult(j.result);
      if(j.status!=="running"){
        clearInterval(timer); timer=null; setRunning(false);
        if(j.status==="done"){
          if(j.result && j.result.ok === false && j.result.kind==="verify"){
            $("statusDot").className="dot fail";
            banner("fail","<b>校验未通过</b> · 归档内容与元数据不一致，请勿使用");
          }else{
            $("statusDot").className="dot done";
            banner("ok","<b>完成</b>"+(j.result&&j.result.kind==="verify"?" · 校验通过":" · "+(j.stage||"")));
          }
        }else if(j.status==="cancelled"){
          $("statusDot").className="dot"; banner("warn","已取消");
        }else{
          $("statusDot").className="dot fail";
          banner("fail","<b>失败</b> · "+esc(j.error||"未知错误"));
        }
        loadArchives();
      }
    }).catch(function(){});
}
function cancel(){
  fetch("/api/cancel",{method:"POST"}).then(function(){banner("warn","正在取消…")});
}
function loadArchives(){
  fetch("/api/archives").then(function(r){return r.json()}).then(function(d){
    var box=$("archiveBox");
    if(!d.files||!d.files.length){box.innerHTML='<div class="empty">输出目录下暂无归档</div>';return}
    box.innerHTML = d.files.map(function(f){
      return '<div class="file"><span class="nm">'+esc(f.name)+'</span>'
        +'<span class="muted">'+fmtBytes(f.size)+'</span>'
        +'<button data-p="'+esc(f.path)+'" class="vf">校验</button>'
        +'<a href="/api/download?path='+encodeURIComponent(f.path)+'"><button>下载</button></a></div>';
    }).join("");
    Array.prototype.forEach.call(box.querySelectorAll(".vf"),function(b){
      b.addEventListener("click",function(){verifyPath(b.getAttribute("data-p"))});
    });
  }).catch(function(){});
}
function verifyPath(p){
  banner(null);
  $("log").textContent=""; logLen=0;
  $("checkCard").style.display="none";
  setRunning(true);
  fetch("/api/verify",{method:"POST",headers:{"Content-Type":"application/json"},
      body:JSON.stringify({path:p,online:!$("offline").checked})})
    .then(function(r){return r.json()})
    .then(function(d){
      if(d.error){banner("fail",esc(d.error));setRunning(false);return}
      jobId=d.job; if(timer) clearInterval(timer);
      timer=setInterval(poll,450); poll();
    });
}
$("btnPull").addEventListener("click",function(){start("pull")});
$("btnBench").addEventListener("click",function(){start("bench")});
$("btnCancel").addEventListener("click",cancel);
$("image").addEventListener("keydown",function(e){if(e.key==="Enter")start("pull")});
renderSteps(0);
loadArchives();
</script>
</body>
</html>
"""


class Job:
    """一次 Web 任务的全部可见状态。事件回调往这里写，前端轮询读。"""

    LOG_MAX = 1200

    def __init__(self, jid: str, argv: List[str], label: str = ""):
        self.id = jid
        self.argv = argv
        self.label = label or " ".join(argv)
        self.status = "running"
        self.stage = "启动中"
        self.step = 0
        self.total_steps = 5
        self.mirrors: List[Dict[str, Any]] = []
        self.trust: Dict[str, Any] = {}
        self.progress: List[Dict[str, Any]] = []
        self.checks: List[Dict[str, Any]] = []
        self.result: Dict[str, Any] = {}
        self.error: Optional[str] = None
        self.log: List[str] = []
        self.started = time.time()
        self.finished: Optional[float] = None
        self._lock = threading.Lock()
        self._buf = ""
        self._real_stdout = sys.stdout
        self._real_stderr = sys.stderr

    # -- 事件 ------------------------------------------------------------
    def on_event(self, event: str, data: Dict[str, Any]) -> None:
        with self._lock:
            if event == "stage":
                self.stage = data.get("name", self.stage)
                if data.get("step"):
                    self.step = int(data["step"])
                self.total_steps = int(data.get("total_steps") or self.total_steps)
                if data.get("detail"):
                    self.stage += " · " + str(data["detail"])
            elif event == "mirrors":
                self.mirrors = data.get("rows") or []
            elif event == "trust":
                self.trust = data.get("info") or {}
            elif event == "progress":
                self.progress = data.get("rows") or []
            elif event == "check":
                self.checks.append({"name": data.get("name"), "status": data.get("status"),
                                    "detail": data.get("detail")})
            elif event == "speedtest":
                self.banner_note = data.get("note") or ""
            elif event == "done":
                self.result = data

    def add_log(self, line: str) -> None:
        with self._lock:
            self.log.append(line.rstrip())
            if len(self.log) > self.LOG_MAX:
                del self.log[: len(self.log) - self.LOG_MAX]

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "id": self.id, "label": self.label, "status": self.status,
                "stage": self.stage, "step": self.step, "total_steps": self.total_steps,
                "mirrors": self.mirrors, "trust": self.trust, "progress": self.progress,
                "checks": self.checks, "result": self.result, "error": self.error,
                "log": self.log, "elapsed": (self.finished or time.time()) - self.started,
            }


class _LogWriter:
    """把 stdout/stderr 同时写进 Job 日志和真实终端。"""

    def __init__(self, job: Job, stream: Any):
        self.job = job
        self.stream = stream

    def write(self, s: str) -> int:
        if not s:
            return 0
        self.job._buf += s
        while "\n" in self.job._buf:
            line, _, self.job._buf = self.job._buf.partition("\n")
            self.job.add_log(line)
        try:
            self.stream.write(s)
        except Exception:  # noqa: BLE001
            pass
        return len(s)

    def flush(self) -> None:
        try:
            self.stream.flush()
        except Exception:  # noqa: BLE001
            pass

    def isatty(self) -> bool:
        return False

    def fileno(self) -> int:
        return self.stream.fileno()


class WebState:
    """全局单任务调度 + 归档目录扫描。"""

    def __init__(self, workdir: str):
        self.workdir = os.path.abspath(workdir)
        self.job: Optional[Job] = None
        self.lock = threading.Lock()

    def busy(self) -> bool:
        j = self.job
        return bool(j and j.status == "running")

    def start(self, argv: List[str], label: str = "") -> Job:
        with self.lock:
            if self.busy():
                raise DipError("已有任务在运行，请先等待完成或点「取消」")
            jid = "job%d" % int(time.time() * 1000)
            job = Job(jid, argv, label)
            self.job = job
            clear_cancel()
            t = threading.Thread(target=self._run, args=(job,), daemon=True)
            t.start()
            return job

    def _run(self, job: Job) -> None:
        set_event_handler(job.on_event)
        sys.stdout = _LogWriter(job, job._real_stdout)
        sys.stderr = _LogWriter(job, job._real_stderr)
        rc = 1
        try:
            job.status = "running"
            args = build_parser().parse_args(job.argv)
            rc = int(args.func(args) or 0)
            # verify 返回 1 表示"校验结论为 FAIL"，任务本身是正常跑完的
            if job.argv and job.argv[0] == "verify":
                job.status = "done"
            else:
                job.status = "done" if rc == 0 else "failed"
            if rc != 0 and job.status == "failed":
                job.error = "进程退出码 %d" % rc
        except Cancelled as e:
            job.status = "cancelled"
            job.error = str(e)
        except DipError as e:
            job.status = "failed"
            job.error = str(e)
        except SystemExit as e:            # argparse 参数错误
            job.status = "failed"
            job.error = "参数错误（退出码 %s），请检查输入" % getattr(e, "code", "?")
        except Exception as e:  # noqa: BLE001
            job.status = "failed"
            job.error = "%s: %s" % (type(e).__name__, e)
        finally:
            job._real_stdout.write("\n")
            job._real_stdout.flush()
            sys.stdout = job._real_stdout
            sys.stderr = job._real_stderr
            set_event_handler(None)
            job.finished = time.time()
            if job._buf.strip():
                job.add_log(job._buf.strip())
            job.add_log("—— 任务结束，状态 %s ——" % job.status)

    def archives(self, limit: int = 60) -> List[Dict[str, Any]]:
        out = []
        try:
            for name in sorted(os.listdir(self.workdir), reverse=True):
                if not (name.endswith(".tar.gz") or name.endswith(".tar")):
                    continue
                p = os.path.join(self.workdir, name)
                if not os.path.isfile(p):
                    continue
                st = os.stat(p)
                out.append({"name": name, "path": p, "size": st.st_size,
                            "mtime": st.st_mtime})
                if len(out) >= limit:
                    break
        except OSError:
            pass
        return out


def _make_handler(state: WebState):
    class Handler(http.server.BaseHTTPRequestHandler):
        server_version = "dip/" + __version__
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *a):  # 静音访问日志
            pass

        # -- 输出helper --------------------------------------------------
        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def _json(self, obj: Any, code: int = 200) -> None:
            self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8"),
                       "application/json; charset=utf-8")

        def _body(self) -> Dict[str, Any]:
            n = int(self.headers.get("Content-Length") or 0)
            if n <= 0:
                return {}
            raw = self.rfile.read(n)
            try:
                d = json.loads(raw.decode("utf-8"))
                return d if isinstance(d, dict) else {}
            except ValueError:
                return {}

        def _query(self) -> Dict[str, str]:
            q = urllib.parse.urlparse(self.path).query
            return {k: v[0] for k, v in urllib.parse.parse_qs(q).items()}

        @property
        def path_only(self) -> str:
            return urllib.parse.urlparse(self.path).path

        # -- 路由 --------------------------------------------------------
        def do_GET(self) -> None:  # noqa: N802
            p = self.path_only
            if p in ("/", "/index.html"):
                html = WEB_PAGE.replace('id="verTag">dip<', 'id="verTag">dip %s<' % __version__)
                self._send(200, html.encode("utf-8"), "text/html; charset=utf-8")
            elif p == "/api/state":
                q = self._query()
                j = state.job
                if not j or (q.get("job") and q["job"] != j.id):
                    self._json({"job": None})
                else:
                    self._json({"job": j.snapshot()})
            elif p == "/api/archives":
                self._json({"dir": state.workdir, "files": state.archives()})
            elif p == "/api/download":
                self._serve_file(self._query().get("path", ""))
            else:
                self._json({"error": "not found"}, 404)

        def do_POST(self) -> None:  # noqa: N802
            p = self.path_only
            try:
                if p == "/api/run":
                    argv = self._body().get("argv") or []
                    if not isinstance(argv, list) or not argv:
                        self._json({"error": "缺少 argv"}, 400)
                        return
                    argv = [str(x) for x in argv]
                    job = state.start(argv)
                    self._json({"job": job.id})
                elif p == "/api/verify":
                    d = self._body()
                    path = os.path.abspath(str(d.get("path") or ""))
                    if not os.path.isfile(path):
                        self._json({"error": "文件不存在: %s" % path}, 400)
                        return
                    argv = ["verify", path, "--quiet"]
                    if d.get("online"):
                        argv.append("--online")
                    job = state.start(argv, label="校验 " + os.path.basename(path))
                    self._json({"job": job.id})
                elif p == "/api/cancel":
                    request_cancel()
                    self._json({"ok": True})
                else:
                    self._json({"error": "not found"}, 404)
            except DipError as e:
                self._json({"error": str(e)}, 409)
            except Exception as e:  # noqa: BLE001
                self._json({"error": "%s: %s" % (type(e).__name__, e)}, 500)

        def _serve_file(self, path: str) -> None:
            path = os.path.abspath(path)
            root = state.workdir
            if not (path == root or path.startswith(root + os.sep)) or not os.path.isfile(path):
                self._json({"error": "拒绝访问：只能下载输出目录内的文件"}, 403)
                return
            size = os.path.getsize(path)
            self.send_response(200)
            self.send_header("Content-Type", "application/gzip")
            self.send_header("Content-Length", str(size))
            self.send_header("Content-Disposition",
                             "attachment; filename=\"%s\"" % os.path.basename(path))
            self.end_headers()
            try:
                with open(path, "rb") as f:
                    shutil.copyfileobj(f, self.wfile, 1024 * 512)
            except (BrokenPipeError, ConnectionResetError):
                pass

    return Handler


def _pick_port(host: str, port: int, tries: int = 25) -> int:
    for i in range(tries):
        p = port + i
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, p))
            s.close()
            return p
        except OSError:
            continue
    raise DipError("端口 %d~%d 都被占用" % (port, port + tries - 1))


def cmd_web(args: argparse.Namespace) -> int:
    host = args.host
    if host not in ("127.0.0.1", "localhost", "::1"):
        eprint("警告: 绑定到 %s 会让同网段其他人也能访问本界面，请确认这是你要的" % host)
    port = _pick_port(host, args.port)
    workdir = os.path.abspath(args.dir or os.getcwd())
    os.makedirs(workdir, exist_ok=True)
    state = WebState(workdir)

    httpd = http.server.ThreadingHTTPServer((host, port), _make_handler(state))
    httpd.daemon_threads = True
    url = "http://%s:%d/" % ("127.0.0.1" if host in ("localhost", "::1") else host, port)
    print("dip %s  Web 界面已启动" % __version__)
    print("  地址   : %s" % url)
    print("  输出目录: %s" % workdir)
    print("  停止   : Ctrl+C")
    print("WEB_URL=%s" % url, flush=True)
    if args.open:
        try:
            import webbrowser
            threading.Timer(0.6, lambda: webbrowser.open(url)).start()
        except Exception:  # noqa: BLE001
            pass
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")
    finally:
        httpd.server_close()
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dip",
        description="下载 Docker 镜像为 docker load 可用的 tar.gz，自动选最快镜像站，并与官方做完整性比对。",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""示例:
  dip.py pull nginx:1.27-alpine
  dip.py pull alpine:3.20 -o /data/img/alpine.tar.gz --jobs 4
  dip.py pull verdaccio/verdaccio:latest --expect-digest sha256:xxxx
  dip.py pull redis:7 --platform linux/arm64 --compress none
  dip.py bench alpine:3.20
  dip.py verify alpine-3.20-linux-amd64.tar.gz --image alpine:3.20
""",
    )
    p.add_argument("-V", "--version", action="version", version="dip %s" % __version__)
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--mirror", action="append", default=[], metavar="URL",
                        help="额外镜像站，会被排到候选最前面（优先级最高），可重复")
        sp.add_argument("--mirrors-file", metavar="PATH",
                        help="镜像站 JSON 配置（默认自动查找 ./mirrors.json）")
        sp.add_argument("--no-builtin-mirrors", action="store_true", help="不使用内置镜像站列表")
        sp.add_argument("--upstream", metavar="URL",
                        help="官方 registry 地址（默认 https://registry-1.docker.io）")
        sp.add_argument("--proxy", metavar="URL",
                        help="代理（默认只用于官方源）；写 env 表示读环境变量")
        sp.add_argument("--proxy-all", action="store_true", help="代理也用于镜像站")
        sp.add_argument("--proxy-from-env", action="store_true",
                        help="从 https_proxy/http_proxy/all_proxy 读代理")
        sp.add_argument("--retries", type=int, default=DEFAULT_NO_PROGRESS,
                        help="连续多少次毫无进展才放弃（默认 %d）" % DEFAULT_NO_PROGRESS)
        sp.add_argument("--layer-retries", type=int, default=LAYER_RETRY_ROUNDS,
                        help="整层失败后补下几轮（默认 %d，0 表示不补）"
                             % LAYER_RETRY_ROUNDS)
        sp.add_argument("--os", default="linux")
        sp.add_argument("--arch", default="amd64")
        sp.add_argument("--variant", default=None)
        sp.add_argument("--platform", metavar="OS/ARCH[/VAR]",
                        help="等价于 --os/--arch/--variant，如 linux/arm64/v8")
        sp.add_argument("--timeout", type=float, default=30.0,
                        help="单次请求的 socket 超时秒数（默认 30，卡住会走续传）")
        sp.add_argument("--probe-timeout", type=float, default=8.0,
                        help="探测/握手超时秒数（默认 8）")
        sp.add_argument("--probe-bytes", type=int, default=1024 * 1024,
                        help="吞吐测速采样字节数（默认 1 MiB）")
        sp.add_argument("--bench-budget", type=float, default=20.0,
                        help="每个镜像站测速的时间预算秒数（默认 20）")
        sp.add_argument("--top-k", type=int, default=4, help="进入吞吐测速的站数（默认 4）")
        sp.add_argument("--jobs", type=int, default=3, help="层下载并发数（默认 3）")
        sp.add_argument("--sources", type=int, default=0,
                        help="多源并行下载：每个 blob 同时使用几个镜像站（pull 生效；"
                             "0=自动，1=关闭分片退回单流）")
        sp.add_argument("--segments", type=int, default=0,
                        help="多源并行时每个 blob 切成几片（pull 生效；0=自动，约 2×源数，上限 8）")
        sp.add_argument("--no-governor", action="store_true",
                        help="关闭动态源池（退回静态分配：源固定绑给 worker，失败才换站）")
        sp.add_argument("--slow-ratio", type=float, default=GOV_SLOW_RATIO, metavar="0-1",
                        help="慢源判定：吞吐低于「最快源 × 该比例」即暂停该源（默认 0.25）")
        sp.add_argument("--min-rate", type=int, default=GOV_MIN_RATE // 1024, metavar="KB/s",
                        help="慢源判定的绝对速率地板（默认 64 KB/s）")
        sp.add_argument("--resume-cooldown", type=float, default=GOV_RESUME_COOLDOWN,
                        metavar="SEC",
                        help="源被暂停后冷却多少秒才探测复活（默认 15）")
        sp.add_argument("--expect-digest", metavar="SHA256:...",
                        help="权威 manifest digest，用于官方比对")
        sp.add_argument("--quiet", action="store_true")

    sp = sub.add_parser("pull", help="下载镜像并打包")
    sp.add_argument("image", help="镜像名，如 nginx:1.27 / library/alpine:3.20")
    sp.add_argument("-o", "--out", help="输出文件（默认 <name>-<tag>-<os>-<arch>.tar.gz）")
    sp.add_argument("--repo-tag", help="写进 manifest.json 的镜像名，默认同输入")
    sp.add_argument("--compress", choices=["gzip", "none"], default="gzip")
    sp.add_argument("--compress-level", type=int, default=6, choices=range(0, 10),
                    metavar="0-9", help="gzip 压缩级别，默认 6（低级别更快）")
    sp.add_argument("--work-dir", help="临时工作目录（默认 <输出>.work，可指向大容量盘）")
    sp.add_argument("--keep-work", action="store_true", help="保留工作目录")
    sp.add_argument("--clean-blobs", action="store_true",
                    help="打包完成后删除压缩层字节（默认保留以支持续传/复用；"
                         "注意删掉后下次运行会重下全部层）")
    sp.add_argument("--no-report", action="store_true", help="不生成 .sha256 / .verify.json")
    sp.add_argument("--no-verify", action="store_true", help="跳过成品归档自校验")
    sp.add_argument("--plan-only", action="store_true", help="只做选站与比对，不下载")
    common(sp)
    sp.set_defaults(func=cmd_pull)

    sp = sub.add_parser("verify", help="校验已有归档")
    sp.add_argument("archive", help="要校验的 tar / tar.gz")
    sp.add_argument("--image", help="用于联网复核的镜像引用")
    sp.add_argument("--online", action="store_true", help="联网复核 manifest digest")
    common(sp)
    sp.set_defaults(func=cmd_verify)

    sp = sub.add_parser("bench", help="只做镜像站测速排名")
    sp.add_argument("image", nargs="?", default="alpine:3.20")
    common(sp)
    sp.set_defaults(func=cmd_bench)

    sp = sub.add_parser("web", help="启动图形界面（浏览器操作）")
    sp.add_argument("--host", default="127.0.0.1", help="监听地址（默认仅本机 127.0.0.1）")
    sp.add_argument("--port", type=int, default=8865, help="监听端口，被占用会自动往后找")
    sp.add_argument("--dir", help="输出目录 / 归档列表的扫描目录（默认当前目录）")
    sp.add_argument("--open", dest="open", action="store_true", default=True,
                    help="启动后自动打开浏览器（默认开启）")
    sp.add_argument("--no-open", dest="open", action="store_false", help="不自动打开浏览器")
    sp.set_defaults(func=cmd_web)

    sp = sub.add_parser("mirrors", help="列出镜像站并测连通性")
    common(sp)
    sp.set_defaults(func=cmd_mirrors)

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    p = getattr(args, "proxy", None)
    if p == "env" or (getattr(args, "proxy_from_env", False) and not p):
        args.proxy = env_proxy()
        if not args.proxy:
            eprint("错误: 环境变量里没有 https_proxy / http_proxy / all_proxy")
            return 2
    try:
        return int(args.func(args) or 0)
    except DipError as e:
        eprint("错误: %s" % e)
        return 2
    except KeyboardInterrupt:
        eprint("\n已中断")
        return 130
    except BrokenPipeError:
        return 0


if __name__ == "__main__":
    sys.exit(main())
