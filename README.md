# dip · Docker 镜像离线下载器

在**没有 docker / podman / skopeo** 的机器上，直接从 Docker Registry v2 HTTP API
下载镜像，**自动挑出最快的镜像站**，打包成 `docker load` 可直接导入的 `.tar.gz`，
并与官方做**密码学级别的完整性比对**。带图形界面。

- 纯 Python 标准库，**零第三方依赖**，Python 3.8+ 可用
- 单文件 `dip.py`，拷到任何机器上就能跑
- 命令行 + 浏览器界面，两套入口共用同一份逻辑与参数

---

## 快速开始

### 图形界面

Windows 双击 **`start-ui.bat`**；Linux / macOS 跑 `./start-ui.sh`。等价于：

```bash
python dip.py web                 # 自动打开浏览器 http://127.0.0.1:8765
python dip.py web --port 9000 --dir D:/dev/mirror     # 指定端口与输出目录
```

界面里能做三件事：**下载镜像**、**只测镜像站速度**、**校验已有归档**，实时看到
阶段进度、镜像站测速表、传输进度条、校验清单和运行日志。

### 命令行

```bash
python dip.py pull alpine:3.20                       # 输出 alpine-3.20-linux-amd64.tar.gz
python dip.py pull nginx:1.27-alpine -o /data/nginx.tar.gz --jobs 4
python dip.py pull redis:7 --platform linux/arm64    # 指定平台
python dip.py pull verdaccio/verdaccio:latest --expect-digest sha256:xxxx
python dip.py bench  alpine:3.20                     # 只测镜像站速度并排名
python dip.py verify out/alpine-3.20-linux-amd64.tar.gz
python dip.py mirrors                                # 列出候选站并测连通性
```

导入到目标机器：

```bash
docker load -i alpine-3.20-linux-amd64.tar.gz
```

---

## 输出文件

每次下载会产出三个文件：

| 文件 | 内容 |
|---|---|
| `xxx.tar.gz` | docker-archive 格式，`docker load` 直接可用 |
| `xxx.tar.gz.sha256` | `sha256sum -c` 兼容格式，用于跨机器核对 |
| `xxx.tar.gz.verify.json` | 完整性报告：所有 digest、信任锚、逐项校验结论 |

归档内部布局（成员顺序固定，便于流式校验）：

```
manifest.json          <- 先写，校验时一次顺序读就能完成
repositories
<config-sha256>.json
layer1/layer.tar
layer2/layer.tar
...
```

**输出是可复现的**：gzip 的 mtime 置 0，tar 成员的 mtime / uid / gid / uname 全部归零，
同一镜像在任何镜像站、任何时间下载，产出的 `.tar.gz` 字节完全一致。
实测 `alpine:3.20` 三次独立运行都是 `sha256:2e4ea533…`。
这个 sha256 可以直接对外公布给同事核对。

---

## 镜像站是怎么挑的

分三级筛选，**最终以实测吞吐为准**：

1. **握手延迟** —— 并发探 `/v2/`，取两次里较快的一次
2. **可用性** —— 真拉一次 manifest，确认该站确实转发了这个镜像
3. **实测吞吐** —— 从最大层上 Range 拉一段真实数据，测 MB/s

第 3 步是**串行**跑的。并发测速会互相抢带宽，测出来的数字是假的。
计时口径也只统计"首字节 → 末字节"的传输窗口，不含建连耗时和尾部停滞——
否则测出来的是"卡了多久"而不是"有多快"。

### 为什么不能只看延迟（本机实测）

| 镜像站 | 握手延迟 | 实测吞吐 |
|---|---|---|
| daocloud | 161 ms | 0.04 MB/s |
| 1ms | 180 ms | 0.05 MB/s |
| dockerproxy | 836 ms | **1.38 MB/s** |
| 1panel / rat.dev | 1.7 s / 0.5 s | 经常直接超时 |

延迟最低的 daocloud 比 dockerproxy **慢 35 倍**。只按延迟排会挑到最差的站。

### 故障转移

选定最快的站之后，其余可用站会按速度排成一条**故障转移链**。某个站传一半卡住或
报 502，会自动切到下一个站，并用 `Range` 从已下载的字节处**继续传**——
因为 blob 是内容寻址的，换到哪个站拿到的都是同一串字节。

```
故障转移链: dockerproxy -> 1ms -> daocloud -> rat.dev
层 1/1: OSError 在 daocloud 上连续失败，换 1ms 续传（已下 768.0 KiB）
```

失败退出时工作目录会保留（`<输出>.work`），重新跑同一条命令即可断点续传。

---

## 完整性是怎么保证的

三层校验，互相独立，**任何一层不过就不交付**：

**L1 传输校验（内容寻址）**
每个 blob 下载后重算 sha256，必须等于 manifest 里声明的 digest。
这就是 Registry v2 的信任模型：digest 就是内容的身份。

**L2 解包校验**
解压出的 `layer.tar` 重算 sha256，必须等于 config 里 `rootfs.diff_ids[i]`。
这一层能抓住"压缩包正确但解压结果不对"的问题（压缩算法、截断等）。

**L3 官方比对（信任锚）**
manifest 的 digest 要有一个外部真值来对照，按优先级：

| 优先级 | 信任锚 | 置信度 | 说明 |
|---|---|---|---|
| 1 | `--expect-digest` | 高 | 你自己给的权威 digest，最可靠 |
| 2 | 官方 registry 可达 | 高 | 逐字节对比官方 manifest digest |
| 3 | 多镜像站共识 | 高 / 中 | ≥2 个独立镜像站返回同一 digest |
| 4 | 单站自洽 | 低 | 仅能自洽，会明确告警 |

同时**只有 manifest digest 与信任锚一致的镜像站才允许参与下载**。
如果某个站返回了不同的 digest，会被直接排除并打印原因，绝不会拿它的数据。

用了哪种信任锚、置信度多少，都会写进 `.verify.json` 并在界面上显示。

### 关于官方源

`registry-1.docker.io` 在国内通常直连不通，`hub.docker.com` 的 API 也不通。
如果官方源不可达，会自动降级到"多镜像站共识"——4 个互相独立的镜像站同时返回
同一个 256 位 sha256 摘要，要同时伪造几乎不可能，所以置信度仍然给"高"。
若你有可用的代理或内网官方源：

```bash
python dip.py pull nginx:1.27 --proxy http://192.168.1.23:7890   # 代理只用于官方源
python dip.py pull nginx:1.27 --upstream https://your-harbor     # 用内网源当真值
```

---

## 为什么它会「下载一半又从头开始」（1.3.0 已修）

大层反复下到一半就清零、永远下不完 —— 根因有两个，都是实测出来的。

### 根因一：最快的那个镜像站不支持断点续传

`dockerproxy.net` **完全无视 HTTP Range**，任何范围请求都返回 200 + 整份文件：

```
Range 0-65535  -> HTTP 200   Content-Range=None
Range 中部      -> HTTP 200   Content-Range=None
Range 越界      -> HTTP 200   （连 416 都不给）
```

而它恰恰是测速里最快的站之一（实测 1.38 MB/s），所以最容易被选中。旧代码拿到 200
就当成「服务端让我从头下」，把已下的几百 MB 截断重来；多源阶段更狠 —— 直接删光
全部分片、退回单流从头下。**一个站就能让下载永远停在原地。**

现在：划段前先用一次 64 KiB 小范围请求**预检**每个站是否真支持续传，不支持的直接
剔出源池；单流阶段也跳过这类站，并且**绝不再把已下的字节截断**。

### 根因二：令牌过期把已下的档当垃圾删了

各站 token 是有寿命的，实测：

| 站 | expires_in |
|---|---|
| daocloud | 3600 s |
| 1ms / rat.dev | 1500 s |
| dockerproxy | 匿名，不要 token |

而 `vllm/vllm-openai` 这类镜像单层能到 4.7 GB，在本机链路上要下近一小时，**必然
跨过好几个令牌周期**。旧代码把 token 无限缓存，过期后每个新请求都撞 401，而 401
分支的处理是「删掉 `.part` 从零开始」—— 于是每 25 分钟清盘一次，永远下不完。

现在：token 按 `expires_in` **提前 120 秒刷新**；401 只重取凭证、从原地续传，一个
字节都不丢。

### 顺带四条

- **重试预算按「无进展」计，不按次数计。** 令牌轮换、服务端按寿命掐断连接这类
  *有进展*的中断不该耗尽重试次数。
- **模式切换不再丢进度。** 以前「先单流下了一半、再改成分片」会把 `.part` 扔掉，
  现在分片会接管它作为前几片的前缀。
- **`.part` 长度已够且 sha256 相符时直接扶正为成品**，一个网络请求都不打。
- **测速样本要过可信度检查。** 采样量太小时，数据其实早躺在内核接收缓冲里，计时
  窗口被压成几微秒，会算出 `62500.00 MB/s` 这种物理上不可能的吞吐。危害不止选错
  站 —— 这个假值会当源池的初值 `peak`，而慢源阈值是「最快源 × 25%」，阈值被抬到
  十几 GB/s 后**所有真实源都会被判成慢源踢光**。现在：采样量下限 4 MiB；窗口短于
  50 ms 或速率超过 201 MB/s 即判为假象，放大样本重测；源池入口再钳一道。

回归测试在 `_test_resume.py`：本地起假 registry 扮演「无视 Range 的站」「按令牌
寿命掐断的站」，断言全程不出现 `start=0` 的续传请求（那正是「从头重下」的定义）。

## 常用参数

```
--platform linux/arm64/v8   平台（等价于 --os/--arch/--variant）
-o, --out PATH              输出文件
--jobs N                    层下载并发数（默认 3）
--probe-bytes N             吞吐测速采样字节数（默认 1 MiB）
--bench-budget N            每个站测速的时间预算秒数（默认 20）
--top-k N                   进入吞吐测速的站数（默认 4）
--timeout N                 单次请求 socket 超时（默认 30）
--probe-timeout N           探测/握手超时（默认 8）
--expect-digest SHA256:…    权威 digest
--mirror URL                追加镜像站并排到最前，可重复
--no-builtin-mirrors        只用自己指定的站
--mirrors-file PATH         镜像站配置（默认自动找 ./mirrors.json）
--compress gzip|none        外层压缩（默认 gzip）
--compress-level 0-9        gzip 级别，默认 6，调低更快
--work-dir DIR              临时目录，建议指向大容量盘
--keep-work                 保留工作目录（默认成功即删）
--keep-blobs                保留已下载的压缩层
--plan-only                 只做选站与比对，不下载
--no-verify                 跳过成品自校验
```

---

## 常见问题

**下载很慢或频繁超时**
先跑 `python dip.py bench <镜像>` 看真实排名。国内到这些镜像站的链路质量波动很大，
同一个站早上 1.4 MB/s、下午可能就 0.04 MB/s。工具会自动测速并故障转移，
实在不行可以 `--mirror <某个站> --no-builtin-mirrors` 锁死一个站。

**环境里设了 `HTTP_PROXY`**
工具默认**显式绕过**环境代理（`ProxyHandler({})`），因为国内镜像站直连更快。
只有 `--proxy` 指定的代理会生效，且默认只作用于官方源；要让代理也走镜像站加 `--proxy-all`。

**磁盘不够**
峰值占用约是压缩层大小的 3 倍（压缩层 + 解压层 + 成品）。用 `--work-dir` 指向大盘，
或加 `--compress none` 省掉一次重压缩。工具会在开始前检查可用空间并告警。

**报 zstd 相关的错**
某些镜像用 zstd 压缩层，Python 3.13 及以前的标准库不支持。升级到 Python 3.14+，
或 `pip install zstandard`。

**跑大镜像时进程被「批量删除」安全钩子杀掉**

`dip.py` 是「下载 -> 校验 -> 解压 -> 打包」的流水线，每个大层都会删掉临时分片、
处理完再删掉 blob 字节。像 `vllm/vllm-openai` 这种 37 层的镜像，一轮下来会有
两三百次删除操作。本机环境的沙箱有「单轮删除次数上限」的保护钩子，超了就会把
进程掐掉（表现为日志停在删分片那一步，退出码 1）。

绕过办法是让它**一次删除都不做**：

```bash
python dip.py pull <镜像> -o <输出>.tar.gz --keep-blobs --keep-work
```

- `--keep-blobs`：处理完某层后不删它的压缩字节
- `--keep-work`：成功后不删整个工作目录

代价是临时文件全部留在 `<输出>.work/` 里（8 GB 的镜像大约占用 25 GB）。
确认归档没问题后可以自己把这个目录删掉。**注意：这不会影响归档的正确性** ——
留下的只是中间产物。

**`docker load` 报错**
先用 `python dip.py verify <文件>` 确认归档完好。校验会顺序读一遍并重算每层 sha256，
比 `docker load` 的报错信息清楚得多。

**Windows 上 Git Bash 里 curl 写文件失败**
这是 Git Bash 的已知问题：它自带的 `curl` 是 Windows 程序，`-o /d/tmp/x` 这种
MSYS 路径写不进去且会静默返回 exit 23。用 `-o D:/tmp/x` 形式，或干脆用本工具的
Web 界面下载归档。

---

## 界面结构

```
start-ui.bat          一键启动界面（Windows）
start-ui.sh          一键启动界面（Linux / macOS）
dip.py                全部实现（核心 + 命令行 + Web 界面），单文件零依赖
mirrors.example.json  镜像站配置样例，改名为 mirrors.json 即生效
out/                  默认输出目录
```

界面只是命令行的一层皮：前端表单拼出 argv，交给**同一套 argparse** 解析，
再调用**同一个函数**。所以界面上的默认值、校验规则、报错文案与命令行完全一致，
不存在两套实现走偏的问题。
