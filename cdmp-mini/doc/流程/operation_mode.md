# 用户异步/同步切换运维手册

本手册汇总 OperationMode 相关的运维入口、观测指标与常见操作步骤，适用于逐步 rollout 阶段的值班同学。

## 管理接口

所有接口均挂载在 `/admin/users` 下，默认仅限 Debug/本地访问；若在 `server_run_options` 中配置了 `adminToken`，调用时需在请求头追加 `X-Admin-Token`。

- `GET /admin/users/operation-mode`
  - 功能：返回当前生效的 OperationMode 配置快照（模式、rollout 百分比、sticky header、队列/黑白名单）。
  - 返回示例：
    ```json
    {
      "config": {
        "mode": "queue",
        "rolloutPercent": 100,
        "stickyHeader": "x-rollout-key",
        "queueKinds": ["create","update","delete","batch"],
        "allowUsers": [],
        "blockUsers": []
      }
    }
    ```
- `PUT /admin/users/operation-mode`
  - 功能：在运行中调整模式配置，未填写的字段沿用当前快照。
  - 请求示例：
    ```json
    {
      "mode": "rollout",
      "rolloutPercent": 30,
      "stickyHeader": "x-user-sticky",
      "allowUsers": ["ceo"],
      "blockUsers": ["legacy-admin"]
    }
    ```
  - 接口会自动归一化大小写、去重名单，并返回更新后的快照。

## Prometheus 指标

| 指标 | 说明 |
| ---- | ---- |
| `operation_mode_current{component,mode}` | 当前模式标识，命中模式的值为 1，其余为 0。|
| `operation_mode_rollout_percent{component}` | 当前 rollout 百分比。|
| `operation_mode_allowlist_size{component}` / `operation_mode_blocklist_size{component}` | 允许/阻止名单元素数量。|
| `operation_mode_decisions_total{component,kind,mode}` | 各操作类型在运行时选择同步/队列/灰度的决策次数。|

所有指标的 `component` 目前固定为 `user_service`，后续扩展其它资源时保持一致的命名约定。

## 常见场景

1. **快速切换至同步模式**
   ```bash
   curl -X PUT http://127.0.0.1:8088/admin/users/operation-mode \
     -H 'Content-Type: application/json' \
     -d '{"mode":"sync"}'
   ```
   适用于异步管道异常时的兜底，PUT 仅指定 `mode` 字段即可。

2. **按用户灰度开启队列**
   - 先写 allowlist：`{"mode":"rollout","rolloutPercent":0,"allowUsers":["pilot-user"]}`。
   - 观察 `operation_mode_decisions_total` 中 `mode="queue"` 的计数是否仅针对灰度用户增长。

3. **基于 header 灰度**
   - 配置 sticky header：`{"mode":"rollout","rolloutPercent":25,"stickyHeader":"x-sticky-id"}`。
   - 压测客户端需在请求中附带 `X-Sticky-Id`，实现同一租户粘性路由。

4. **监控回滚信号**
   - `operation_mode_current{mode="queue"}` 快速掉为 0，代表已切换回同步。
   - `operation_mode_blocklist_size` 增长通常意味着新增了强制同步名单，需要排查原因。

## 注意事项

- PUT 请求每次都会返回规范化后的配置，建议在自动化脚本中使用响应体作为下一步的基线，避免本地缓存旧值。
- rollout 百分比会被限制在 0~100 之间，名单字段自动去重并转为小写。
- 变更模式后可立即通过 `GET /admin/users/operation-mode` 与 Prometheus 指标确认生效。
- 若同时联动 Kafka、Redis 等组件进行大规模灰度，请提前关注队列深度与补偿 backlog 指标，防止瞬时切换造成雪崩。
