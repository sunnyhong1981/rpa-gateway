# QUNL RPA 网关

**0.6.0 候选已在 uat4 按全局 4 单准备并发运行**，拆分采集、准备、核验和回执角色，保留每群 2 单限制。来源开放本机 RPA测试群及 rpa-prod 的优急递外部群、JS签收图片自动获取测试群；Chatwoot Sidekiq 队列拆分配置尚未部署。详见 [BACKEND-WORKERS.md](BACKEND-WORKERS.md)。

当前提供设备接入、持久任务队列、事务通知、Redis 在线状态、图文原图任务、S3 下载、共享流控、多实例接入和监控告警。Mac RPA测试群与 Windows rpa-prod 两群的文字、签收原图已接网关。Windows 客户端为 **2026.10.30.3**，两个逻辑设备共用一台桌面顺序执行；CSV/XLSX、原生通知和监控群继续兼容通道。Windows 215项与网关157项测试通过，原图、文字及两种文件已实发核验。接入范围、人工接管状态和回退步骤见 [WINDOWS-INTEGRATION.md](WINDOWS-INTEGRATION.md)。代码尚未提交或推送。

## 当前部署

- 服务器：`chatwoot-prod`（uat4），目录 `/opt/ai/rpa-gateway`。
- Compose 项目：`qunl-rpa-gateway-scale`，部署文件 `deploy/scale.compose.yaml`。
- 两个 ASGI 网关实例、媒体服务、worker、来源适配器和告警发布器；另有专用 Redis、私有 S3 兼容 SeaweedFS、Nginx、Prometheus 与 Alertmanager。
- 与 Chatwoot 共用 PostgreSQL 实例，任务库和账号为独立的 `rpa_gateway`。当前四并发应用池上限 3+3+2+5+3+1，共 17，账号最多 20 个连接。来源读取账号最多 12 个连接（含来源池、发送栅栏和监控读取），只读视图限定 account871 的三个父会话（inbox469/467/531）及监控告警；基础 messages 表未开放。
- 公网 HTTPS/WSS 入口为 `https://uat4.qunl.com/rpa-gateway`，原图签名入口为同域 `/rpa-gateway-originals`；原 Nginx 添加专用路径并 reload。
- 回环入口：HTTP `31080`、HTTPS/WSS `31443`、对象存储 HTTPS `31444`、监控 `31090`。
- 回环 TLS 使用独立试验证书；公网入口复用原有可信域名证书，未修改客户端系统信任库。

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
| 调度 | 同一桌面执行串行；验证提交完成后可释放桌面，后台最多等待 3 单存档；未知结果继续阻止派发 |
| 幂等 | 按来源消息、运单、类型去重，回执及存档事件独立唯一约束 |
| 图文任务 | 每单一个文字加至多 9 张原图任务，最多 50 MiB；多单按单号独立追踪 |
| 原图 | 实际 PNG/JPEG 解码、像素、大小、SHA256/MD5 校验，保持原始字节 |
| 送达 | 提交意图、已提交、未知、官方存档确认分开，不把 Enter 当作送达 |
| 恢复 | 未提交租约可重新领取；提交后超时/掉线不自动重发，等待存档或人工核对 |
| 通知 | 事务 outbox、跨实例推送、合并槽、revision 补偿；通知本身不能执行任务 |
| 运维 | 指标鉴权、告警规则、共享限流、媒体/事件保留与可审计清理 |

`recipient_id` 必须由可信后端完成官方身份校验后填写；网关不以昵称、心跳或客户端自报替代官方身份。verifier 是服务端权限，设备不能自证送达。

原生桌面没有跨网络事务，“恰好一次 Enter”仍需客户端本地提交日志和互斥锁配合：只有首次提交意图成功确认才允许按一次 Enter；响应丢失、崩溃或已提交后恢复只做对账，不重按。真实图文和存档桥现开放本机测试群及 rpa-prod 两群；文件与原生通知的兼容范围见接入说明。

## 文档与客户端

- [PIPELINE.md](PIPELINE.md)：桌面释放回执、后台存档、重连补偿和本机提速验收。
- [MAC-PILOT.md](MAC-PILOT.md)：试接范围、发送保护、真实验收和故障回退。
- [WINDOWS-INTEGRATION.md](WINDOWS-INTEGRATION.md)：rpa-prod 两群、文件兼容、正式运行核对和回退边界。
- [INTEGRATION.md](INTEGRATION.md)：接入现状及扩大范围条件。
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

`start_local_scale.py` 启动独立双实例与 TLS 入口；`load_scale.py` 测量真实连接与完整模拟任务，支持 UAT 隔离栈的多条 SSH/WSS 隧道分流；`fault_scale.py` 只操作本次创建的本地依赖，`stop_local_scale.py` 按进程归属停止本次服务。长稳运行时不可停止本机测试依赖。uat4 不承担千台压测。

## 备份和维护

服务连接串与私钥在 `runtime/secrets/`、`runtime/tls/`，不输出日志或进入 Git。PostgreSQL 连接失败不自动回退 SQLite；旧 SQLite 和切换前快照仅作备份留档。

数据库用 `pg_dump` 或实例备份，原图目录/对象存储和私密配置独立备份。禁止恢复旧任务快照后自动重放。外置原图后不能直接退回 0.2.0：故障回退维持 0.3.0 账本与对象存储，把流量收敛到一个健康实例。`rolling_scale_host.py` 支持逐个移出上游、更新、就绪再加入，过程中另一个网关保持接入。

旧 `deploy/compose.yaml` 与 `deploy_isolated.sh` 是 0.2.0 历史部署参考，不作为当前版本的启动入口。

当前仍是同宿主机上的进程冗余，不具备跨宿主机容灾。短时千台测试不能代替 1 小时/24 小时稳定性验收，也不代表真实物流查询、企微上传和官方存档可达到相同延迟。
