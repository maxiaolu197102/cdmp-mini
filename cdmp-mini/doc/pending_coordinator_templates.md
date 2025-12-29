# PendingCoordinator 配置模板与多服务约定

## 使用默认模板
- 针对每个服务，推荐使用 `usercache.DefaultPendingCoordinatorConfig("service_name")` 生成基础模板，组件名会同时用于 metrics 标签与 Redis key 前缀隔离。
- 可在返回值上按需覆盖阈值、窗口、延迟曲线等字段：

```go
cfg := usercache.DefaultPendingCoordinatorConfig("order")
cfg.BackpressureSoftLimit = 800
cfg.BackpressureHardLimit = 1200
coord := usercache.NewPendingCoordinator(redis, cfg)
```

## 通过 TrafficHooks 统一装配
- 若仅需在路由侧快速接入：

```go
hooks := common.NewTrafficHooks(common.TrafficHookConfig{
    Component:         "order",
    Redis:             redis,
    WriteLimit:        200,
    WriteLimitGlobal:  400,
    PendingMetricsKey: "order:pending:active",
    UserMetricsPrefix: "order:pending:depth:",
})
router.POST("/orders", hooks.LagProtect, hooks.WriteLimit, handler)
```

## Key 前缀约定
- `MetricsKey`：建议 `service:pending:active`，内部会追加 hash-tag，保证跨分片计数。
- `UserMetricsPrefix`：建议 `service:pending:depth:`，通过 normalize 加一层冒号，避免与其他服务冲突。

## 何时需要自定义
- 多服务共享 Redis 时，务必指定各自的 `Component/MetricsKey/UserMetricsPrefix` 以隔离指标和计数。
- 若已有自定义采样或租约 key 结构，直接构造 `PendingCoordinatorConfig` 传入 `NewPendingCoordinator`，或通过 `TrafficHookConfig.Coordinator` 传入现成实例。
