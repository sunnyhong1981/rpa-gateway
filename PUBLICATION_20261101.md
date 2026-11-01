# RPA 网关源码汇总：2026-11-01

汇总官方归档来源队列、资源准备与角色拆分、通知和独立健康探测、Windows 接入，以及已部署的进程 owner/session 防护与未知投递隔离释放修复。归档按实际发生时间划分文字和原图批次，核对文字、原图 MD5 与大小，防止复用其他任务的证据。

提交前 pytest 回归 185 项通过；测试使用专用 rpa_gateway_test 数据库中的临时 schema、独立 Redis 命名空间和临时本机对象存储。原生适配器另有 13 项通过。测试不连接生产任务表，也不向企微发送原生按键。

可选原生适配器测试依赖客户端仓库。设置 RPA_GATEWAY_CLIENT_SOURCE 为客户端源码目录，再运行 python -m unittest discover -s native_client/tests。普通网关测试依赖的私密连接文件由 RPA_GATEWAY_TEST_DSN_FILE、RPA_GATEWAY_TEST_OBJECT_CONFIG 指定；运行数据不提交 Git。

本次代码提交没有切换生产镜像或重启服务；生产配置、凭证、对象文件和投递数据库不包含在提交中。
