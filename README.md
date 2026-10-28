# QUNL RPA 网关

独立的设备接入、任务派发和执行回执服务。第一版采用**隔离试运行**：没有 Chatwoot、查件服务或官方存档的连接凭证，没有读取旧 RPA 队列的代码，也不会操作企业微信、剪贴板或桌面。正式客户端继续按原有方式工作。

## 当前部署

- 主机：`chatwoot-prod`（uat4）。
- 目录：`/opt/ai/rpa-gateway`。
- Compose 项目：`qunl-rpa-gateway`，唯一服务 `gateway`。
- 入口：服务器本机 `http://127.0.0.1:31080`。
- 独立 SQLite：`runtime/data/gateway.sqlite3`，WAL + FULL 同步。
- 独立 Docker bridge 网络，不加入 Chatwoot 的网络；应用未配置任何业务后端连接。
- 未新增公网入口或域名，未改动现有 Nginx、安全组和防火墙配置；Docker 仅建立本网关的回环端口映射。

从开发机访问：

```bash
ssh -N -L 31080:127.0.0.1:31080 chatwoot-prod
```

另一个终端：

```bash
curl --fail http://127.0.0.1:31080/healthz
```

客户端正式接入前必须配置可信 HTTPS；开发和 SSH 隧道允许回环 HTTP。

## 支持的功能

| 功能 | 行为 |
| --- | --- |
| 设备鉴权 | 独立随机设备令牌，数据库只存令牌摘要；支持撤销和轮换 |
| 权限 | 租户隔离、设备隔离，收件箱和群 ID 成对授权 |
| 握手 | 协议版本、安装实例、运行版本和能力；与健康心跳分开 |
| 健康 | worker、桌面解锁、企微登录、自动发送四项均正常，且心跳未过期才派发 |
| 调度 | 同一设备跨群只执行一个任务；支持并发领取，原子租约 |
| 去重 | 租户、收件箱、原始消息、运单、任务种类组成唯一键；冲突内容拒绝覆盖 |
| 批量图片 | 每单一个“文字 + 1～9 张原图”任务，返回全部图片清单及哈希 |
| 多运单 | 同一次请求可按运单创建多个任务，50 单去重用例已覆盖 |
| 图片 | 只接收实际可解码的 PNG/JPEG，按内容识别 MIME，保持原始字节 |
| 回执 | 提交前意图、已提交、待核对、存档确认分别记录，不能把已提交当送达 |
| 通知 | WebSocket 只通知任务变化，HTTPS 领取和回执；支持纯轮询 |
| 送达核验 | 独立 verifier 身份，严格比对文字、图片数量、MD5、字节数、群、机器人、时间及存档事件唯一性 |
| 审计 | 每次任务状态转换保留时间线；人工关闭不等于成功，也不自动重发 |

`recipient_id` 必须由受信后端完成官方身份校验后填写。本网关目前不查询官方通讯录；不能拿客户端昵称、心跳版本或前端自报身份替代官方校验。verifier 是服务端适配器专用权限，设备不能自证送达。

## 任务状态和防重复规则

```text
queued -> leased -> submitting -> submitted -> confirmed
              |          |            |
              |          +--超时------+--> unknown
              |
              +--租约过期且未到请求截止时间--> queued（换新令牌）
              +--安全失败--> failed
queued / leased --请求截止--> expired
unknown --存档补齐--> confirmed
unknown --人工审计关闭--> closed_unconfirmed
```

1. 原生客户端先预检群、身份、草稿、原图；本地持久保存任务及提交事件 ID。
2. 提交前调用 `submitting`，只有首次成功确认且状态仍为 `submitting` 才允许一次原生 Enter。
3. Enter 前在本地持久记录不可重试标记。进程崩溃、网络超时或提交结果不确定时，恢复程序只对账，不重新按 Enter。
4. `submitting`、`submitted` 超时均进入 `unknown`，不会重新入队，并继续占用该设备执行槽，待存档或人工核对。
5. 设备每个安装实例必须有本地进程互斥锁，禁止多个进程共享一个运行会话控制桌面。网关不能阻止不遵守协议的程序私自操作桌面。
6. 租约令牌、会话、设备凭证均核验。旧租约不能提交；提交意图已记录后也不能标记“未尝试，可重发”。

这是在原生 UI 无事务能力下尽量避免重复发送的协议，不能宣称网络系统能保证桌面操作绝对恰好一次。网关不生成 URL 兜底消息：将来业务适配器须消费 `failed/expired/unknown`，按原始请求做有时限、可去重的兜底；兜底文本送达不能算原图送达。

## 时限及资源

| 项目 | 默认值 |
| --- | --- |
| 建议设备心跳 | 15 秒 |
| 健康有效期 | 45 秒 |
| 提交前租约 | 30 秒，可续期但不能超过请求截止时间 |
| 请求有效期 | 180 秒，可设 15～3600 秒 |
| 提交/存档等待 | 90 秒；超时进入待核对，不重发 |
| 存档匹配时间 | 意图前最多 2 秒偏差，意图后 10 分钟内发生；证据可稍后到达 |
| 单张原图 | 12 MiB、最多 3000 万像素 |
| 一次任务 | 最多 9 张、合计 50 MiB |
| HTTP 请求体 | 实际读取最多 16 MiB，含流式请求 |
| 容器 | 0.5 CPU、384 MiB 内存、80 个进程、8 个并发请求 |

超过 9 张需后续适配器明确拆批，不能偷偷退回逐张发送。50 单是 50 个可追踪任务，不把 50 单全部照片放进同一个桌面输入框。生产批次编排及尾部时限需在接入时单独验收。

## 本地开发

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

开发用临时目录通过 `Settings(database, admin_token, backend_token, verifier_token)` 启动；三种控制令牌必须各不相同、至少 32 字符。仓库无内置默认密码。测试自建临时数据库，不读已安装 RPA 的配置。

## 部署及停用

`requirements.txt` 固定运行依赖版本；`requirements-dev.txt` 增加测试依赖。服务仅支持单容器单 worker；当前没有跨主机高可用或横向扩容。

首次部署，在新目录中执行：

```bash
python3 scripts/init_runtime.py
sudo chown 10001:10001 runtime/data runtime/secrets/*_token
docker compose -f deploy/compose.yaml build gateway
docker compose -f deploy/compose.yaml up -d --no-deps gateway
```

令牌原文件位于私有的 `runtime/secrets/`，不输出到日志或 Git。初始化脚本不会覆盖已有密钥和数据。三个控制令牌分别供设备管理员、业务适配器、存档核验适配器使用，不能发给普通客户端；目前仅保存在服务器，尚未配置到任何业务。

查看及停止本服务：

```bash
cd /opt/ai/rpa-gateway
docker compose -f deploy/compose.yaml ps
docker compose -f deploy/compose.yaml logs --tail 50 gateway
docker compose -f deploy/compose.yaml stop gateway
```

只停止本网关即可撤销试运行，不操作其他 Compose 项目，不删除 `runtime`。运行中的 SQLite 不可直接复制主文件备份，使用 SQLite online backup；恢复前停网关并保留当前数据，不能恢复旧快照后重放已提交任务。密钥目录另做加密备份。

首版保留任务和图片用于审计，没有自动清理或自动扩盘。接入生产前须确定媒体保存期限、备份、磁盘监控和告警；不应无限存储业务照片。

## 独立模拟验收

在容器内以标准输入执行 `scripts/smoke.py`：

```bash
docker compose -f deploy/compose.yaml exec -T gateway python - < scripts/smoke.py
```

脚本生成独立 `synthetic_` 租户、模拟设备和三张小图，跑完整接入、上传、领取、下载校验、回执及**模拟**存档确认，结束后撤销设备凭证。没有向企微发消息。真实图片/手机链路需要之后单独选择测试群验收，当前不接入正式业务。

接口和渐进接入见 [API.md](API.md) 与 [INTEGRATION.md](INTEGRATION.md)。
