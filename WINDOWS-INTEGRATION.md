# 查看和维护 rpa-prod 的网关接入

2026-10-30 已完成 rpa-prod 接入，客户端 **2026.10.30.3**。文字与签收原图走 uat4 网关；CSV/XLSX和原生@通知继续兼容通道。网关准备为全局4并发，Windows桌面顺序执行。

**优急递群洪波会话1816仍保留原人工模式**（2026-10-16 customer_requested）。该会话的提问不会自动生成机器人回复；本次用明确标注的受控回复验收该群传输，未修改人工接管策略。

## 核对正式运行

在 Windows PowerShell 执行：

```powershell
Get-ScheduledTask -TaskName 'QUNL WeCom RPA' | Select-Object TaskName, State
Get-Content C:/app/rpa/logs/windows-rpa-status.json -Raw -Encoding UTF8
Get-Content C:/app/rpa/logs/wecom-session-health.json -Raw -Encoding UTF8
```

状态应为版本2026.10.30.3、multi_group，两群archive_send_only且queued=0；企微logged_in。原14条客户review记录保留，不能据此重发旧回复。Windows正式目录为C:/app/rpa，原supervisor、入口及状态库保留。

| 群 | inbox | 逻辑设备 | 官方机器人 |
| --- | --- | --- | --- |
| 优急递外部群 | 467 | windows-rpa-prod-467 | aiKeFu |
| JS签收图片自动获取测试群 | 531 | windows-rpa-prod-531 | aiKeFu |

两个设备使用独立凭证，共用一台Windows桌面。私密配置为C:/app/rpa/gateway-configs/467.json、531.json，限定任务用户、SYSTEM与Administrators。不要输出device_token。目录权限继承到子文件；仅适用于目录的OI/CI标志不应直接递归替代文件读取权限。

在 uat4 检查：

```bash
cd /opt/ai/rpa-gateway
docker compose -f deploy/scale.compose.yaml -p qunl-rpa-gateway-scale ps
curl --fail http://127.0.0.1:31080/healthz
```

网关健康版本字符串为0.6.0-local；source_config有469、467、531三条配置。四个来源角色共享4单上限。source只读账号通过安全视图访问三个父会话及对应官方机器人存档，不读取基础messages表。

## 核对发送及异常

图片只有服务端confirmed、来源delivered与对应官方文字/全部原图证明齐全才算送达。Windows客户端先持久化提交边界，获得授权后复查群和完整草稿，按一次发送键；输入清空后可释放桌面，继续由后台核验存档。

当前企微文件直接进入编辑框。新增windows_inline_file.py校验新鲜的type=3原生文件草稿、单一完整路径、原始SHA256、空正文及5MiB内CSV/XLSX，再提交一次，最后等待官方群、文件名、MD5和时间证明。旧独立文件弹窗继续兼容。其他文件格式保持原校验范围。

群、原图或草稿改变、身份不明、授权响应不确定时都停止提交。结果未知保留待核对，不自动重按。需要回退时先暂停对应来源派发，核对已提交和unknown记录；仅有证据证明未提交的任务可回旧通路。状态库原位保留，不能恢复旧SQLite快照再启动旧发送器。

## 已验证的证据

| 证据 | 结论 | 操作意义 |
| --- | --- | --- |
| Windows 215项、后端157项回归 | 当前适用用例通过 | 覆盖提交栅栏、隔离、文件与故障模拟 |
| JS群13573/13575/13576 | 一次问询3单共7图确认 | 每单图文一次提交，官方存档齐全 |
| JS群13584、13588 | 单图、无图回复通过 | 保留运单与回复对象 |
| 优急递群13603 | 3张原图与文字确认 | 人工模式下受控传输通过 |
| JS群13604、13605 | CSV/XLSX官方文件确认 | 内嵌文件路径通过 |
| 安装/恢复记录 | 原14条客户review完整保留 | 不重放历史；本轮5条失败测试另有审计与终止记录 |

本轮真实实发最多3单/3图，未作为50单群内实发、9图群内实发或千台生产长稳结论。群告警沿原本机Mac监控通道，Mac锁屏/离线时会排队。

完整报告与无凭证补丁包位于当前任务工作区outputs/rpa-prod-gateway-20261030/REPORT.md、windows-gateway-2026.10.30.3.zip；release-manifest.json保存安装基线及最终代码摘要。本轮源码未提交或推送Git。
