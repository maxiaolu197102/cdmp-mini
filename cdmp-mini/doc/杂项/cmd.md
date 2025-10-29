 watch -n 2 'echo -n "总行数: "; wc -l 2025-10-18.json | awk "{print \$1}"; echo -n "文件大小: "; du -h 2025-10-18.json | awk "{print \$1}"'
----持续监测文件大小及行数

watch -n 2 'mysql -uroot -p'"'"'iam59!z$'"'"' -e "SELECT COUNT(*) FROM iam.user"'
---持续监测数据库表记录
✅ 调试时：通过 trace-id 查看完整调用链

✅ 监控时：通过 metrics 查看性能指标

✅ 审计时：通过 audit log 满足合规要求

✅ 排错时：通过 error log 快速定位问题
grep -E "createperf_serialbtp89a_77qq.*trace|trace.*crea.createperf_serialbtp89a_77qq" /var/log/iam/iam-apiserver.log

cd /home/mxl/cretem/cretem && grep -n "用户创建链路耗时超过200ms" log/iam-apiserver.log | tail

SELECT * FROM mysql.slow_log ORDER BY start_time DESC LIMIT 10;
ls -l --block-size=M

cd /home/mxl/cretem/cretem/log && python3 - <<'PY'

SET GLOBAL slow_query_log = 'ON';
SET GLOBAL long_query_time = 0.05;  -- 单位秒，按需要调整
grep -i 'error\|slow\|timeout\|latency\|cost\|耗时\|慢'  /var/log/iam/iam-apiserver.log | head -n 200  > 1
SELECT COUNT(*) FROM mysql.slow_log WHERE start_time > NOW() - INTERVAL 1 HOUR;
go tool pprof -alloc_space <http://localhost:8088/debug/pprof/heap>

以下情况需要手动执行 go mod vendor 来更新 vendor 目录：
新增依赖：在 go.mod 中添加 require 后，执行 go mod vendor 同步新依赖到 vendor。
升级 / 降级依赖版本：修改 go.mod 中的版本号后，执行 go mod vendor 拉取新的版本源码。
替换依赖（replace 指令）：修改 replace 后，执行 go mod vendor 同步替换后的依赖。
SHOW VARIABLES LIKE 'max_connections';
SHOW VARIABLES LIKE 'max_user_connections';
SHOW GLOBAL STATUS LIKE 'Max_used_connections';
SHOW GLOBAL STATUS LIKE 'Threads_connected';
go tool pprof -alloc_space <http://localhost:8088/debug/pprof/heap>

CPU 性能分析（最常用）# 实时采集（默认持续 30 秒，可通过 ?seconds=60 调整
go tool pprof <http://localhost:8088/debug/pprof/profile>

iam$ python3 ~/cretem/cretem/cdmp-mini/test/iam-apiserver/tools/analyze_tree.py --top 10 /var/log/iam/iam-apiserver.log

cd /home/mxl/cdmp-mini/cdmp-mini && IAM_APISERVER_E2E=1 go test ./test/iam-apiserver/user/change_passwd -run TestChangePasswordFunctional/self_change_success -v
go tool pprof -output cpu_profile.pprof <http://localhost:8088/debug/pprof/profile?seconds=30>

------------------

go tool pprof -output /home/cdmp-mini/pprof/cpu_profile.pprof <http://localhost:8088/debug/pprof/profile?seconds=30>
go tool pprof /home/mxl/pprof/pprof.apiserver.samples.cpu.006.pb.gz
--------------

IAM_APISERVER_E2E=1 IAM_APISERVER_PERF_EXTENDED=1 IAM_APISERVER_PERF_DATA=all go test ./test/iam-apiserver/user/update -run TestUpdatePerformance -v

说明：
IAM_APISERVER_E2E=1 IAM_APISERVER_PERF_EXTENDED=1 IAM_APISERVER_PERF_DATA=all go test ./test/iam-apiserver/user/update -run TestUpdatePerformance -v

IAM_APISERVER_E2E=1 启用端到端性能测试。
IAM_APISERVER_PERF_EXTENDED=1 让梯度/压力场景不再跳过。
IAM_APISERVER_PERF_DATA=all 会依次跑 small/medium/large 三档批量策略测试；可改成某一档例如 small 以缩短时间。
IAM_APISERVER_E2E=1 IAM_APISERVER_PERF_EXTENDED=1 IAM_APISERVER_PERF_DATA=all go test ./test/iam-apiserver/user/update  -v -timeout 1000m
