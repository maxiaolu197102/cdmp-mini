PUT /api/users/:name（乐观锁全量更新）
router.go 将请求路由到 UserController.Update。
控制器：读取 JSON → 校验用户名/版本号 → 调用 UserService.Update。
服务层：UpdateService 仍与以前一样走 Kafka：处理完校验后调用 Producer.SendUserUpdateMessage。
异步消费：UserConsumer/RetryConsumer 读取 Kafka，按 UserUpdateCommandFull 分支更新数据库并刷新缓存。
✔️ 保持原有“控制层 → service → Kafka → consumer”的异步链路。
新增 PATCH 链路

PATCH /api/users/:name/profile（单用户局部更新）

路由到 UserController.PatchProfile。
控制器：校验用户名、Patch 字段、可选版本号 → 构造 User 对象（Command=patch，Patch 填充 UserPatchSpec，ExpectedVersion 根据传入版本或由消费端回填）。
调用 UserService.Update，重用原 Kafka 生产逻辑（即仍写入同一更新 topic）。
Kafka 消费端：UserConsumer 根据 Command=patch，加载快照、调用 UserPatchSpec.Apply 合并字段、执行乐观锁校验并写库，刷新缓存。
PATCH /api/users（批量条件更新）

路由到 UserController.PatchCollection。
控制器：校验批量条件 UserConditions 与 Patch 字段 → 组装 User（Command=batch，携带 Patch 与 Conditions）。
调用 UserService.BatchPatch（新 service 方法），其核心是 Producer.SendUserUpdateMessage，只携带一条 Kafka 消息实现异步处理。
Kafka 消费端：UserConsumer 识别 Command=batch，根据条件查询匹配用户列表，逐个应用 UserPatchSpec.Apply、执行版本自增及缓存刷新；冲突行会跳过并记录告警。
两条 PATCH 路径都完全复用原先的异步更新架构：控制层只做校验和封装，服务层统一通过 Kafka 生产者投递，最终由异步消费者串联数据库与缓存更新。

GPT-5-Codex (Preview) • 1x
