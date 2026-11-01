# 后台并发与资源隔离

当前候选 0.6.0.dev0（健康接口 0.6.0-local）已在 uat4 以全局 4 单准备并发运行，只有一个 4 线程准备进程，每群最多 2 单、下载 4 路、解码 2 路。2026-10-30 已接入 rpa-prod 的 inbox467/531，保留本机 inbox469；Windows 运行 2026.10.30.3，详见 [WINDOWS-INTEGRATION.md](WINDOWS-INTEGRATION.md)。下文 8 并发双准备进程是扩展模板，不是当前 uat4 的运行数量。

## 处理分工

| 角色 | 执行内容 | 网关库池上限 | 来源只读池上限 |
| --- | --- | --- | --- |
| gateway-a / gateway-b | 控制请求、设备派发、发送栅栏 | 3 + 3 | 发送栅栏期间最多 6 个临时来源连接 |
| media | 原图 API | 2 | 0 |
| source-capture | 来源消息游标和任务登记 | 1 | 1 |
| source-prepare-a / b | 每进程 4 个准备任务 | 2 + 2 | 1 + 1 |
| source-archive | 精确文字及全部原图存档核验 | 1 | 1 |
| source-receipts | Chatwoot 送达/失败回执，2 个有界请求 | 1 | 1 |
| dispatch | outbox 通知与执行租约到期处理 | 1 | 0 |
| validation | staging 图片校验和旧 BLOB 转对象 | 1 | 0 |
| maintenance | 清理、保留、孤儿对象扫描 | 1 | 0 |
| alert-delivery | 既有监控群告警发布 | 1 | 既有监控视图按需连接 |

网关库合计最多 19 个连接，保留在原角色上限 20 内。来源常驻池最多 5，另有最多 6 个 API 栅栏临时连接与监控读取，候选角色预算为 12；部署前需审核 `deploy/source-role-budget.sql`，本次没有执行此 SQL。不能把每个群复制为一套进程和连接池。新来源/后台 worker 的容器健康检查只读进程生成的本地健康文件，不额外占用数据库连接；文件只在既有池完成心跳写入后更新。原告警发布器最多保留一个额外健康探针连接，网关角色预算含探针为 20。

`deploy/backend-workers.compose.yaml` 是单独的候选 override，未改生产 `deploy/scale.compose.yaml`。默认关闭旧 source/worker（profile 且 replicas=0），定义上述独立服务，应用 CPU 上限合计不超过 2 核、逐进程设置内存上限。这是共享宿主机的初始限额，不是资源预留或性能承诺。

```bash
docker compose -f deploy/scale.compose.yaml -f deploy/backend-workers.compose.yaml config
```

本机 Docker 引擎未启动。本次执行了配置渲染与预算检查，没有启动这些候选容器；功能与多进程隔离用现有本机测试 PostgreSQL、Redis 和 S3 模拟服务验证。Docker CPU／内存限额在容器内的实际压力表现尚未验证。

## 准备任务与恢复

采集只登记任务并原子推进游标，使用每群捕获租约，不做图片下载。多个准备 worker 通过 PostgreSQL 短事务与 SKIP LOCKED 领取，按群最近领取时间轮转。跨进程上限为 8 个准备任务、每群最多 2 个；图片网络并发全局 8、解码全局 2，每个任务最多提交 3 个待处理图片 future，防止单任务占满整个图片池。

已验证原图按租户和 SHA256 复用。每次下载仍校验来源摘要与字节数，首次图片实际解码保留，原始字节和 MIME 识别规则不变。缓存复用不重新上传对象。

每设备默认最多保留 16 项准备中/待发送任务；达到上限后保留来源账本等待，其他设备可继续准备。并发准备也算入预留量，最终创建时在设备锁内再核对容量，避免同时完成越过上限。

默认排队窗口 300 秒、单次准备执行预算 60 秒、租约 15 秒、最多 3 次尝试。排队窗口从来源回复创建时间计算，UTC 无时区值显式按 UTC 处理；排队和执行分开，不再以 180 秒消息年龄直接认定图片失败。存在绝对业务截止，不会无限延长。

worker 定期续租。所有权丢失、准备超时、路由暂停/换代后，旧 worker 不能发布任务。设备锁/任务锁等待之后重新检查当前时间。网关任务创建与来源任务链接同一事务提交；提交 ACK 丢失不能导致第二个任务或错误兜底。

正常排队不立刻发链接；达到明确截止或有界失败后，由独立回执角色处理一次失败回执。先检查源消息是否仍可处理，并修复旧任务/link 分离遗留，已提交/未知的原图不能被替换。原图仍在 pending/preparing/queued/leased/submitting/submitted/unknown 时，链接兜底继续等待；原图已确认时，链接兜底只标记为取消。提交结果未知不自动重发；官方存档仍为最终送达依据。

共享限额保存在 source_runtime_limits。进程配置不一致会启动失败；调整限额需要先停止准备角色、核对无在途准备，再统一更新候选配置和策略。不能在滚动期间让 worker 自行使用不同上限。

schema 4 增加来源租约、期限、回执租约和调度表；同版本进程启动跳过 DDL，避免重复 ALTER 与在线查询死锁。旧 schema 1/2/3 可迁移，不能在有进行中工作时直接降回旧程序或恢复旧账本后重发。

## 运行配置

源码入口 `python -m rpa_gateway.source_pipeline`；原 `source_bridge` 入口仍可调用新流水线。`RPA_GATEWAY_SOURCE_CONFIG_FILE` 可包含一份私密配置或配置列表；只读取配置中明确列出的 tenant/inbox，源只读视图也必须已授权。此功能没有自动扩大任何正式群权限。

`RPA_GATEWAY_SOURCE_ROLE` 为 capture / prepare / archive / receipts / all。`all` 便于独立本机测试，以独立循环和有界线程执行；候选容器分别运行单角色进程。

`RPA_GATEWAY_SOURCE_WORKERS`、GLOBAL_JOBS、PER_GROUP_JOBS、NETWORK_SLOTS、DECODE_SLOTS、IMAGE_WORKERS、REPORT_WORKERS、READY_PER_DEVICE 均为有上限整数；SOURCE_POOL_SIZE 与 SOURCE_READ_POOL_SIZE 分别控制两种数据库池。`RPA_GATEWAY_WORKER_ROLE` 为 dispatch / validation / maintenance / all，后台 worker 默认 all 仍兼容现有入口。

```sql
SELECT tenant,inbox,state,count(*),min(enqueued_at)
FROM source_jobs GROUP BY tenant,inbox,state;
SELECT name,updated,last_progress,active_jobs,error_type FROM worker_health;
```

heartbeat 是进程活跃指标，last_progress 是实际检查/完成时间，两者分开；忙碌任务并不等于无进展。错误只记录类型，不记录令牌或私密 URL。

## Chatwoot 队列隔离候选

`deploy/chatwoot/` 提供三个互斥的 Sidekiq 队列配置及 compose override：实时（包含 agent_bot/default）、scheduled_jobs 定时检查、低优先级报表/清理（包含 low）。完整覆盖当前原队列清单，旧 sidekiq 默认关闭且 replicas=0，避免新旧消费者同时启动。

配置并发及 SIDEKIQ_CONCURRENCY 同时设为 4 / 2 / 1，匹配当前 Chatwoot 数据库池实际使用的环境变量。背景进程额外限制 PostgreSQL 单语句 30 秒、关闭单查询并行、work_mem 8 MB；没有改共享数据库全局参数。

```bash
CHATWOOT_BASE_COMPOSE=/opt/ai/chatwoot/docker-compose.yaml docker compose   -f /opt/ai/chatwoot/docker-compose.yaml -f deploy/chatwoot/workers.compose.yaml config
```

这是待审核配置，不是本次部署命令。完整 Chatwoot 应用的混合业务尚未在此拓扑跑验收；本机实际运行的是两个 Sidekiq 进程与测试任务，证明慢报告不会把实时队列占住。上游物流接口、模型推理和图片下载的外部耗时不因队列隔离自动缩短。

## 本机验证范围

真实本机 HTTP/HMAC、PNG 原图下载/摘要、Pillow、PostgreSQL、Redis 与 S3 模拟器用于准备阶段对照；人为注入身份接口与文件网络等待，不冒充真实供应商测试。1/4/8 并发交替三轮，所有样本使用相同已验证缓存条件；新下载字节仍重新校验，准备吞吐不是用户提问到企微送达的倍数。

10 个模拟设备、50 群各 1200 条入站消息，共 60000 条，加 100 个可准备回复，核对群/设备映射和游标。这不是 60000 次图片回复或每天 12000 次查件的完整容量认证。

另有四个独立 Python 角色进程测试、worker 崩溃/旧租约/丢 ACK/暂停/Redis 故障/毒图片/跨进程共享并发测试。结果与来源哈希见当前本机任务报告。无真实企微消息、无生产连接，不操作正式 RPA。

## uat4 双网络地址

来源 worker 同时连接网关与 Chatwoot 网络，Redis 连接串必须使用唯一主机名 `qunl-rpa-gateway-scale-redis-1`，不能使用会在两张网络重复的 `redis` 别名。不得为绕过认证错误关闭 Redis 密码。uat4 首轮实机发现此别名歧义；本机单网络隔离测试不能覆盖该项，服务器验收必须同时核对唯一主机名和 PING。

## uat4 四并发试运行

当前使用 `deploy/uat4-four-workers.compose.yaml` 的四并发参数生成运行拓扑：`source-prepare-a` 一个进程、4 个线程，`source-prepare-b` 不启动。准备/核验/采集/回执与 dispatch/validation/maintenance 独立运行；旧 source/worker 已停止。

服务器的 `/opt/ai/rpa-gateway/deploy/scale.compose.yaml` 已保存为自包含渲染配置，普通运维用这一份文件；不要把带 extends 的候选 override 再叠加到这份已渲染配置上。重新生成拓扑需要使用本轮备份的原始模板并重新审核路径。源码仓库的 base 模板保留原样，两者用途不同。

```bash
cd /opt/ai/rpa-gateway
docker compose -f deploy/scale.compose.yaml ps
docker compose -f deploy/scale.compose.yaml logs --tail 30 source-prepare-a source-archive source-receipts
```

首次容器健康检查在 0.05 核限额下超时，脚本自动回退，未还原任务快照。已改用 `python -S` 读取健康文件 mtime、90 秒启动宽限和 180 秒汇合等待，并将低配来源/派发/维护角色调到 0.1 核；告警发布器的重型数据库健康探针另将 CPU 上限从 0.05 调到 0.15 核，无重启调整。

四并发角色网关库池总预算 17（主账号上限 20），来源池 4 加 API 栅栏最多 6、监控读取 1，来源只读账号上限 12，未扩大 SELECT 授权。

服务器隔离样本在 0.5 核/512 MiB 容器中完成 16 个任务，实际准备峰值 4、下载峰值 4，未访问 Chatwoot 业务或真实供应商。一次成对测量中，8 单串行 3.3591 秒、4 并发 2.0651 秒，约 1.627 倍；不是同样资源下多轮统计或完整用户响应倍数。

真实复验四单后台准备约 0.7～1.4 秒；首轮双网络 Redis 别名问题导致的图片失败、以及复验一单原生图片预检失败均保留。复验三单 6 张原图成功，另一单全新补测 3 张成功，官方归档合计确认 9 张原图；不将四单复验写成一轮全过。临时测试 schema/角色和独有对象已清理，测试数据库保留，历史业务任务未重放。详细证据见本轮 uat4 验收报告。
