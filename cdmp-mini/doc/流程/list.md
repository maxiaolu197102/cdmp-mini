功能概述

metav1.ListOptions 现新增 Identifiers/Status/Contact/Time/Extend 结构，能携带精确 ID、InstanceID、Name，状态数组、管理员标记，邮箱/手机号精确或模糊匹配，创建/更新/登录时间范围，以及 JSON 扩展字段的 equals/contains/in 条件。
控制层在 populateUserListFilters 中解析 HTTP Query：支持 id/instanceID/name、status 多值、isAdmin、email/phone 精确与 email[like]/phone[like] 模糊、createdAt[gte|lte]、updatedAt[gt|lt]、loginedAt[lt]，以及 extend.xxx 的 equals/contains/in 三类写法，并做字段合法性校验、时间多格式解析。
Service 层会把旧的 fieldSelector=name=xxx 自动转入新的 Identifiers.Name，并把所有过滤条件写入 trace tag，方便排查。
Store 层基于 database/sql 动态拼接 WHERE 子句：默认过滤 status=1，支持 IN、范围、LIKE、JSON_EXTRACT/JSON_CONTAINS 组合，且保留分页与统计。查询仍走读库（集群模式下），扫描结果用轻量结构提升性能。
测试情况

已编译通过控制层、服务层单元测试包（go test ./internal/apiserver/control/v1/user、go test ./internal/apiserver/service/v1/user）。
internal/apiserver/store/user 在 SQLite 环境下运行会报 UNION 语法不兼容；该行为和集成前一致，需在 MySQL 测试环境或调整语句后再行验证。

Preparing structured explanation
请求链路

internal/apiserver/control/v1/user/list_control.go::List
Gin 控制器入口：收集 X-Request-ID、操作人，开启链路追踪与审计。流程依次执行：

ctx.ShouldBindQuery(&opts)：把查询参数绑定到 metav1.ListOptions；失败直接返回 400 (code.ErrBind)。
populateUserListFilters(&opts, ctx.Request.URL.Query())：一站式解析 id/name/status/email[like]/extend.* 等自定义语法，做合法性校验并构建 ListOptions。内部调用关键点：
buildLikePattern：在未显式提供 % 时，仅追加尾部 % 并对 _/% 进行转义，保证索引友好；
parseExtendFilter / normalizeExtendField：约束扩展字段路径，只允许 A-Za-z0-9_；
时间参数统一走 parseTimeValue 支持多种格式。
u.validateListOptions(&opts)：业务级校验，例如 limit、offset 范围、非法字段组合。
设定超时（默认 30s），调用 u.srv.Users().List。
internal/apiserver/service/v1/user/list_service.go::List
Service 层负责聚合附加数据：

直接调用底层 store.Users().List 获取基础用户列表；
收集用户名批量调用 store.Policy().CountByUsernames，一次 SQL 汇总策略数量并写入每个用户的 TotalPolicy。
若需要还可在此层执行角色、扩展字段等二次组装。
internal/apiserver/store/user/list_store.go::List
最终与数据库交互的部分：

buildUserListFilters 根据 ListOptions 拼接 SQL WHERE 子句和参数数组，默认追加 status=1；
通过 ensureSQLCore() 拿到连接，构造 SELECT ... FROM user WHERE ... ORDER BY id DESC LIMIT ? OFFSET ?；
采用 dbscan.ScanUserLiteInto 将行映射到 v1.User；List 不再执行 COUNT(*)，避免大表聚合；
支持的过滤包括：
精确匹配：id/name/status/isAdmin/email/phone；
模糊：email LIKE ?、phone LIKE ?（已做转义与尾 %）；
时间范围：createdAt/updatedAt/loginedAt；
JSON 扩展字段：extend.equals/contains/in，转换为 JSON_EXTRACT 或 JSON_CONTAINS 语句。
关键辅助函数

控制层

populateUserListFilters：手写的查询解析器，覆盖数组参数、布尔/状态枚举、extend. 派生语法。
buildLikePattern / escapeLikeLiteral：控制模糊查询的转义策略。
parseExtendFilter：把 extend.foo[equals] 等表达式翻译成 ListOptions.Extend 结构。
Store 层

buildUserListFilters：集中构建 WHERE 条件，返回 (clause, args, filterCount, defaultStatus) 便于追踪。
jsonPathForKey：确保 JSONPath 安全，防止非法字段注入。
Service 层 Policy 相关

store/policy/list_store.go::CountByUsernames：SELECT username, COUNT(*) FROM policy WHERE username IN (?) GROUP BY username，解决 N+1。
Service 把结果写回 User.TotalPolicy，供 v1.ConvertToPublicUser 输出。
整体流程小结

控制器解析/校验 HTTP 查询，并记录审计与 tracing 信息。
Service 调用 Store 获取基础列表，并额外批量拉取策略数量。
Store 根据 filter 拼装 SQL，在 MySQL 中完成筛选和排序。
返回数据序列化为 PublicUser 下发，HTTP 层写响应。
通过这一三层结构，List API 能够灵活支持多条件过滤、扩展字段匹配，同时保持数据库查询高效（去掉总数统计、批量策略计数）并将接口语义与数据访问逻辑清晰分离。

性能验证与监控提示

- k6 压测脚本默认时长：baseline 5m、parallel_mixed_filters 10m、pagination 6m、invalid 4m、load 10m。若先前为了快速验证临时覆盖了 `BASELINE_DURATION` 等环境变量，重新压测时只需不设置这些变量即可恢复默认时长。
- 长时间压测期间建议同时关注数据库连接指标，例如定期执行 `SHOW GLOBAL STATUS LIKE 'Threads_connected';` 或 `SHOW GLOBAL STATUS LIKE 'Threads_running';`，并结合 `SHOW ENGINE INNODB STATUS` 观察锁等待。
- 本次 10 分钟基准压测总计完成 ~202k 次请求，整体 `http_req_duration` P95 ≈ 3.57ms，最高 96 并发用户均稳定在 40% 左右 CPU；慢日志仅记录人工触发的 `SELECT COUNT(*) FROM iam.user`，说明列表查询均落在 50ms 阈值以内。
- 压测结束后将 `long_query_time` 恢复为默认 10s，以免手工巡检语句持续写入 slow log，后续若需重新抓取慢查询可再调低阈值。
- 采样 `sar -u 1 5` 时 CPU 平均利用率约 20%（idle ≈ 78%），`iostat -x 1 5` 显示磁盘 iowait 与 `%util` 均保持低位，佐证列表查询在长压下对计算与 IO 资源的压力较小。
