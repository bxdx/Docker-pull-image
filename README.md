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
python dip.py web --port 9000 --dir /data/mirror     # 指定端口与输出目录
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

### 为什么不能只看延迟（实测数据）

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

### 续传的三条硬规则

- **不支持 Range 的站不参与续传。** 划段前用一次 64 KiB 小范围请求预检，凡是
  「请求了 Range 却回 200 整份文件」的站会被剔出源池——它仍可贡献全新下载的带宽，
  但拿不到任何续传任务。
- **凭证过期只刷新，不丢字节。** token 按 `expires_in` 提前 120 秒刷新；遇到 401
  先换凭证，再从盘上已有长度续传，`.part` 不会被截断。
- **重试预算按「连续无进展」计，不按次数计。** 令牌轮换、服务端按寿命掐断连接这类
  *有进展*的中断，不会耗尽重试次数。

模式切换（单流 ↔ 多源）会接管已有的 `.part` 作为前缀，不会因为换了下法就丢进度。

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
python dip.py pull nginx:1.27 --proxy http://127.0.0.1:7890    # 代理只用于官方源
python dip.py pull nginx:1.27 --upstream https://your-harbor    # 用内网源当真值
```

---

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
--keep-work                 成功也保留工作目录（失败时总是保留，用于续传）
--clean-blobs               层处理完即删压缩字节（默认保留至本次运行结束）
--plan-only                 只做选站与比对，不下载
--no-verify                 跳过成品自校验
--no-report                 不生成 .sha256 与 .verify.json
```

---

## 测试

全部用本地假 registry，不联网、不依赖第三方库：

```bash
python _test_resume.py       # 断点续传 / 凭证轮换（断言不出现从头重下的请求）
python _test_multisource.py  # 多源分片与故障转移
python _test_governor.py     # 源池调度与慢源剔除
python _test_reuse.py        # 已有 blob / 已解压层的复用
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

**`docker load` 报错**
先用 `python dip.py verify <文件>` 确认归档完好。校验会顺序读一遍并重算每层 sha256，
比 `docker load` 的报错信息清楚得多。

---

## 目录结构

```
start-ui.bat          一键启动界面（Windows）
start-ui.sh           一键启动界面（Linux / macOS）
dip.py                全部实现（核心 + 命令行 + Web 界面），单文件零依赖
mirrors.example.json  镜像站配置样例，改名为 mirrors.json 即生效
_test_*.py            回归测试
out/                  默认输出目录
```

界面只是命令行的一层皮：前端表单拼出 argv，交给**同一套 argparse** 解析，
再调用**同一个函数**。所以界面上的默认值、校验规则、报错文案与命令行完全一致，
不存在两套实现走偏的问题。
