# RPA 网关 API v1

所有认证使用 `Authorization: Bearer`；令牌不放 URL。除 `/healthz` 外均需要对应权限。管理端提供经过鉴权的 `/openapi.json`，包含精确请求结构。

## 角色与入口

| 角色 | 请求 | 用途 |
| --- | --- | --- |
| 无 | GET `/healthz` | 存活及 PostgreSQL 连接探测、版本、存储类型、隔离模式 |
| admin | GET `/openapi.json` | OpenAPI 结构 |
| admin | POST `/v1/admin/{tenant}/devices` | 创建设备，仅首次返回随机设备 token |
| admin | POST `/v1/admin/{tenant}/devices/{id}/rotate` | 轮换，旧令牌和会话立即失效 |
| admin | POST `/v1/admin/{tenant}/devices/{id}/revoke` | 撤销 |
| admin | GET `/v1/admin/{tenant}/status` | 各状态任务数量、设备握手和健康信息 |
| admin | GET `/v1/admin/{tenant}/tasks/{id}` | 业务内容及审计时间线，不含租约令牌 |
| admin | POST `/v1/admin/{tenant}/tasks/{id}/review` | 取消未提交任务，或审计关闭未知结果 |
| backend | POST `/v1/backend/{tenant}/media` | 请求体为原始 PNG/JPEG 字节，返回 id/sha256/md5/size/mime |
| backend | POST `/v1/backend/{tenant}/tasks` | 幂等创建任务 |
| verifier | POST `/v1/verifier/{tenant}/tasks/{id}/confirm` | 受信存档适配器提交实际证据 |
| device | POST `/v1/device/hello` | 建立/恢复稳定会话 |
| device | POST `/v1/device/heartbeat` | 设备就绪状态 |
| device | POST `/v1/device/tasks/claim` | 原子领取至多一项 |
| device | GET `/v1/device/active?session_id=...` | 获取本会话活动任务，只对账，不触发重发 |
| device | GET `/v1/device/tasks/{id}/media/{media_id}` | 活动任务原图，核验清单 SHA256，禁止重定向 |
| device | POST `/v1/device/tasks/{id}/receipts` | 续租、提交意图、提交结果等 |
| device | WS `/v1/device/notifications` | 通知变化；正式领取仍走 HTTP |

控制端三种令牌暂是安装级受信服务权限，可管理指定路径中的租户。**不是可直接交给不同租户用户的多租户控制台凭证**。每台设备令牌才限制在已绑定租户、设备和群范围内。

## 请求示例

以下是协议数据结构，不包含可用密钥。

创建设备：

```json
{"device_id":"dedicated-desktop-01","bindings":[{"inbox_id":"test-inbox","room_id":"test-room"}]}
```

同一物理设备 ID 全安装唯一；不能重复注册到多个租户绕过串行。不能从不同安装实例抢占仍有任务的设备。轮换密钥不会把进行中或未知任务重新排队，需要存档或人工核对。

握手：

```json
{"protocol":1,"installation_id":"persistent-install-01","runtime_version":"gateway-adapter-1","capabilities":["text_v1","caption_images_v1","official_identity_v1"]}
```

握手返回 `session_id`。随后每 15 秒上报：

```json
{"session_id":"server-returned-session","worker_alive":true,"desktop_unlocked":true,"wecom_logged_in":true,"auto_send":true}
```

创建任务（先上传图片取得 `media_ids`，纯文字传空列表）：

```json
{"device_id":"dedicated-desktop-01","inbox_id":"test-inbox","room_id":"test-room","source_message_id":"source-123","waybill":"JSE_TEST","recipient_id":"official-member-id","robot_id":"official-robot-id","caption":"运单号：JSE_TEST\n签收图片共3张","media_ids":[],"ttl_seconds":180}
```

领取：

```json
{"session_id":"server-returned-session"}
```

返回 `task: null` 表示暂无任务、设备忙/待确认或能力不满足，不代表发送失败。设备不健康或心跳陈旧返回 409。领取结果包含 `lease_token`、`lease_until` 和独立原图清单 `media`。

回执：

```json
{"session_id":"server-returned-session","lease_token":"server-returned-lease-token","event_id":"persisted-local-event-id","state":"submitting","reason":""}
```

`state` 可为 `renew / submitting / submitted / unknown / failed_pre_submit`。重试同一个事件必须使用相同 ID 和内容；冲突返回 409。确认提交只接受从 leased 到 submitting 的首次转移。网络异常时切勿新造事件 ID 尝试重复发送。参考 SDK 的 `begin_submit()` 会拒绝重复回执授权 Enter。

证据确认示例结构：

```json
{"events":[{"event_id":"archive-caption-id","room_id":"test-room","actor_id":"official-robot-id","occurred_at":1793200000.0,"kind":"text","text":"运单号：JSE_TEST\n签收图片共3张"}]}
```

图片任务必须同时提供所有原图事件，`kind: image`，带 `md5` 和 `size`。示例仅展示文字事件结构，单独用于三图任务会被拒绝。verifier 必须在真实官方存档里读取这些字段；设备上报“我已发出”不能作为存档证据。每个存档事件只能确认一个任务。

WebSocket 建连后 5 秒内，首帧 JSON 为 `token` 和 `session_id`。服务只推任务 ID、状态及更新时间，无租约或媒体内容。后续可发文本 `ping` 收 `pong`；每次轮询都重新检查令牌撤销和会话。连接断开不改变任务状态。

## HTTP 状态

- 401：令牌缺失、错误、撤销或轮换。
- 403：收件箱与群不在设备授权绑定中。
- 404：资源不存在或不属于当前设备/租户。
- 409：设备未就绪、幂等冲突、旧会话/租约或状态冲突；不可盲目重发。
- 413：请求或图片过大。
- 415：无法解码的图片或不支持的实际格式。
- 422：协议字段或存档证据不符合要求。

SDK 在 `rpa_gateway/client.py`，只提供传输，不带桌面监听和发送代码，不能直接替换现有正式 RPA。SDK 不自动重试提交，UI 适配器必须持久记录本地提交边界。

## v0.3.0 新增接口

| 角色 | 接口 | 用途 |
| --- | --- | --- |
| admin | GET `/metrics` | Prometheus 指标，route 模板标签不含设备/运单信息 |
| admin | GET `/v1/admin/infrastructure` | Redis、对象存储和连接池状态，不含密钥 |
| backend | POST `/v1/backend/{tenant}/media/uploads` | body 为 `{sha256,md5,size}`，取得 5 分钟的指定 staging 对象上传授权 |
| backend | POST `/v1/backend/{tenant}/media/uploads/{id}/complete` | 通知后台开始验证，只排队，不直接允许派发 |
| backend | GET `/v1/backend/{tenant}/media/uploads/{id}` | 查询 pending/queued/validating/ready/rejected；ready 才返回可引用的 media_id |
| device | GET `/v1/device/sync?session_id=...` | 全量同步当前活动任务和单调递增设备 revision，不重新领取 |
| device | GET `/v1/device/tasks/{task}/media/{media}/download` | 确认当前任务归属后返回 120 秒对象下载授权，SDK 不转发设备 bearer |

旧原图上传/下载接口保留，由 Nginx 送到独立媒体服务；控制网关拒绝直接处理这些原图。媒体格式、哈希、大小仍验证，签名链接过期后需重新向网关鉴权。撤销设备不能立即撤销已签出且未到期的 URL；要求立即撤销时使用独立媒体鉴权代理。

v0.3.0 通知包括 `cursor`（设备 revision）。客户端可以始终先 sync，推送只作同步提示；同一设备最多两个跨实例通知连接。SDK `notifications()` 带随机退避和低频同步，不自动领取或发送；可传 `ca_file` 指定信任试运行证书，禁止关闭证书验证。

限流返回 429 与 Retry-After；Redis 就绪状态不可用时，新派发/新提交返回 503，已有提交回执和官方核验仍可写 PostgreSQL。依赖失败不会自动退回本地数据库或自动重放任务。
