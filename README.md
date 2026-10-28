# QUNL RPA 网关

v0.3.0 提供设备接入、持久任务队列、事务通知、Redis 在线状态、图文原图任务、S3 直传下载、共享流控、多实例接入和监控。当前采用**隔离试运行**：正式 Mac/Windows RPA 继续原接入方式，网关没有读取旧 RPA 队列的消费者，也不操作企微桌面或剪贴板。

## 当前部署

- 服务器：`chatwoot-prod`（uat4），目录 `/opt/ai/rpa-gateway`。
- Compose 项目：`qunl-rpa-gateway-scale`，部署文件 `deploy/scale.compose.yaml`。
- 两个 ASGI 网关实例、独立媒体服务和后台 worker；另有专用 Redis、私有 S3 兼容 SeaweedFS、Nginx 与 Prometheus。
- 与 Chatwoot 共用 PostgreSQL 实例，但独立数据库和账号为 `rpa_gateway`。不使用 Chatwoot 账号或业务表。池上限按进程 4+4+4+2，共 14；账号最多 16 个连接。
- 独立 bridge 网络，PostgreSQL 保留原 Chatwoot 网络并额外连接该网络。没有改动正式 RPA、Chatwoot API/Nginx 配置，没有新增公网入口。
- 回环入口：HTTP `31080`、HTTPS/WSS `31443`、对象存储 HTTPS `31444`、监控 `31090`。
- TLS 试运行证书只覆盖 localhost/127.0.0.1，未加入系统信任库；正式接入需有效域名、可信证书及对象存储外部地址。

```bash
cd /opt/ai/rpa-gateway
docker compose -f deploy/scale.compose.yaml ps
curl --fail http://127.0.0.1:31080/healthz
curl --fail --cacert runtime/tls/cert.pem https://127.0.0.1:31443/healthz
```

## 保证的业务边界

| 项目 | 行为 |
| --- | --- |
| 设备与权限 | 租户、设备、收件箱/群绑定；设备随机令牌可撤销/轮换，库里仅摘要 |
| 握手与健康 | 能力握手独立于心跳，四项就绪状态和 45 秒 TTL 都满足才派发 |
| 调度 | 同一设备串行，设备间并行，PostgreSQL 行锁和部分唯一索引兜底 |
| 幂等 | 按来源消息、运单、类型去重，回执及存档事件独立唯一约束 |
| 图文任务 | 每单一个文字加至多 9 张原图任务，最多 50 MiB；多单按单号独立追踪 |
| 原图 | 实际 PNG/JPEG 解码、像素、大小、SHA256/MD5 校验，保持原始字节 |
| 送达 | 提交意图、已提交、未知、官方存档确认分开，不把 Enter 当作送达 |
| 恢复 | 未提交租约可重新领取；提交后超时/掉线不自动重发，等待存档或人工核对 |
| 通知 | 事务 outbox、跨实例推送、合并槽、revision 补偿；通知本身不能执行任务 |
| 运维 | 指标鉴权、告警规则、共享限流、媒体/事件保留与可审计清理 |

`recipient_id` 必须由可信后端完成官方身份校验后填写；网关不以昵称、心跳或客户端自报替代官方身份。verifier 是服务端权限，设备不能自证送达。

原生桌面没有跨网络事务，“恰好一次 Enter”仍需客户端本地提交日志和互斥锁配合：只有首次提交意图成功确认才允许按一次 Enter；响应丢失、崩溃或已提交后恢复只做对账，不重按。图文实发和真实存档桥尚未切换到本网关。

## 文档与客户端

- [API.md](API.md)：接口、角色、图片直传、同步与 SDK。
- [SCALE-OPERATIONS.md](SCALE-OPERATIONS.md)：组件、流控、TLS、监控、保留策略、故障和部署回退。
- [CAPACITY-1000.md](CAPACITY-1000.md)：负载假设与验收边界。
- `rpa_gateway/client.py`：可选网关传输 SDK，带证书校验、完整性校验、随机退避和补同步；不会启动或更新已安装 RPA。

## 验证

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
RPA_GATEWAY_TEST_DSN_FILE=runtime/scale-local/test_database_url .venv/bin/python -m pytest -q
```

依赖需预先启动在专用回环地址：测试 PostgreSQL 的 `rpa_gateway_test`、Redis `36379` 和 S3 模拟服务 `39000`。对象测试配置 `runtime/scale-local/objects.json` 可通过 `scripts/local_test_config.py` 生成；测试使用临时 schema、Redis namespace 和对象 prefix，不接触正式库。S3 模拟器仅用于本机测试，服务器使用真实 SeaweedFS。

`start_local_scale.py` 启动独立双实例与 TLS 入口；`load_scale.py` 测量真实连接与完整模拟任务，`fault_scale.py` 只操作本次创建的本地依赖，`stop_local_scale.py` 按进程归属停止本次服务。绝不对承载业务的 uat4 做千台压测。

## 备份和维护

服务连接串与私钥在 `runtime/secrets/`、`runtime/tls/`，不输出日志或进入 Git。PostgreSQL 连接失败不自动回退 SQLite；旧 SQLite 和切换前快照仅作备份留档。

数据库用 `pg_dump` 或实例备份，原图目录/对象存储和私密配置独立备份。禁止恢复旧任务快照后自动重放。外置原图后不能直接退回 0.2.0：故障回退维持 0.3.0 账本与对象存储，把流量收敛到一个健康实例。`rolling_scale_host.py` 支持逐个移出上游、更新、就绪再加入，过程中另一个网关保持接入。

旧 `deploy/compose.yaml` 与 `deploy_isolated.sh` 是 0.2.0 历史部署参考，不作为当前版本的启动入口。

当前仍是同宿主机上的进程冗余，不具备跨宿主机容灾。短时千台测试不能代替 1 小时/24 小时稳定性验收，也不代表真实物流查询、企微上传和官方存档可达到相同延迟。
