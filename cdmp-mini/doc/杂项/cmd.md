 watch -n 2 'echo -n "总行数: "; wc -l 2025-10-18.json | awk "{print \$1}"; echo -n "文件大小: "; du -h 2025-10-18.json | awk "{print \$1}"'
----持续监测文件大小及行数

watch -n 5 'mysql -uroot -p'"'"'iam59!z$'"'"' -e "SHOW GLOBAL STATUS LIKE \\\"Threads_connected\\\"; SHOW GLOBAL STATUS LIKE \\\"Threads_running\\\";"'
---轻量采集 MySQL 线程指标，避免 COUNT(*) 全表扫描
test/iam-apiserver/tools/collect_perf_metrics.sh --duration 600 --interval 5 --tag smoke
---批量收集 Threads、pidstat、iostat 指标，输出到 `log/perf/<timestamp>`
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
cdmp-mini$ cd /home/mxl/cdmp-mini/cdmp-mini && mysql -h 192.168.10.8 -uiam -piam59\!z$ -e "SET GLOBAL log_output='TABLE'; SET GLOBAL long_query_time=0.2; SET GLOBAL slow_query_log='ON';"
cdmp-mini$ cd /home/mxl/cdmp-mini/cdmp-mini && mysql -h 192.168.10.8 -uiam -piam59\!z$ -e "SHOW VARIABLES LIKE 'slow_query_log'; SHOW VARIABLES LIKE 'log_output'; SHOW VARIABLES LIKE 'long_query_time';"
cdmp-mini$ cd /home/mxl/cdmp-mini/cdmp-mini && mysql -h 192.168.10.8 -uiam -piam59\!z$ -e "SHOW GLOBAL STATUS LIKE 'Slow_queries';"
cd /home/mxl/cdmp-mini/cdmp-mini && mysql -h 192.168.10.8 -uiam -piam59\!z$ -e "SET GLOBAL long_query_time=0.05;"
cd /home/mxl/cdmp-mini/cdmp-mini && export PATH=$HOME/k6/bin:$PATH && export BASE_URL=<http://192.168.10.8:8088> && export ADMIN_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJodHRwczovL2dpdGh1Yi5jb20vbWF4aWFvbHUxOTgxL2NyZXRlbSIsImV4cCI6MTc2MTg4MDc3OCwiaWF0IjoxNzYxODc3MTc4LCJpc2FkbWluIjoiMSIsImlzcyI6Imh0dHBzOi8vZ2l0aHViLmNvbS9tYXhpYW9sdTE5ODEvY3JldGVtIiwianRpIjoiand0X2o1dnYxM3A0MXd3djYxIiwib3JpZ19pYXQiOjE3NjE4NzcxNzgsInJvbGUiOiJhZG1pbiIsInNlc3Npb25faWQiOiJzZXNzXzcxODU2MTA0MTRiYmI0NDgiLCJzdWIiOiJhZG1pbiIsInR5cGUiOiJhY2Nlc3MiLCJ1c2VyX2lkIjoiMSIsInVzZXJfc3RhdHVzIjoxLCJ1c2VybmFtZSI6ImFkbWluIn0.VgTpvH_nX6ZRKOLEg4-iv-p33nx7miS4LbbYL8euGvo && export LIST_DATASET='{"primaryName": "k6_primary_1761877151810243_66356", "multiDisabledName": "k6_multi_d_1761877153443735_90214", "multiEmailPrefix": "k6multi1761877152", "contactUsername": "k6_contact_1761877154006523_42650", "contactPhonePrefix": "138139", "paginationEmailPrefix": "k6page1761877154", "paginationExpected": ["k6_page_3_1761877156796135_58233", "k6_page_2_1761877155758845_50685", "k6_page_1_1761877155198212_93924", "k6_page_0_1761877154600393_96559"], "loadUserNames": ["k6_primary_1761877151810243_66356", "k6_contact_1761877154006523_42650", "k6_multi_a_1761877152374377_48551", "k6_multi_d_1761877153443735_90214", "k6_page_3_1761877156796135_58233", "k6_page_2_1761877155758845_50685", "k6_page_1_1761877155198212_93924", "k6_page_0_1761877154600393_96559"]}' && export IAM_APISERVER_DISABLE_CLIENT_RATE_LIMITER=true && k6 run --summary-export k6-summary-long.json test/iam-apiserver/user/list/k6/list.js
正在运行...
cd /home/mxl/cdmp-mini/cdmp-mini && export PATH=$HOME/k6/bin:$PATH && export BASE_URL=<http://192.168.10.8:8088> && export ADMIN_TOKEN=$(cat ~/secrets/iam_admin.token) && export CREATE_DATASET='{"usernamePrefix":"k6_create","basePassword":"InitPassw0rd!","emailDomain":"perf.local","phonePrefix":"138","phoneSuffixLength":8,"nicknamePool":["PerfOps","PerfQA","PerfSvc"],"extendTemplates":[{"department":"perf","tags":["baseline"]},{"department":"guard","tags":["parallel"]}],"labels":{"origin":"k6","module":"create"},"extras":{"team":"iam-perf"},"isAdminRatio":0.05,"defaultStatus":1}' && export IAM_APISERVER_DISABLE_CLIENT_RATE_LIMITER=true && k6 run --summary-export test/iam-apiserver/user/create/k6-summary.json test/iam-apiserver/user/create/k6/create.js
---create API k6 多场景压测（需要预先准备 CREATE_DATASET JSON）
cd /home/mxl/cdmp-mini/cdmp-mini && mysql -h 192.168.10.8 -uiam -piam59\!z$ -e "SHOW GLOBAL STATUS LIKE 'Threads_connected';"
cd /home/mxl/cdmp-mini/cdmp-mini && mysql -uroot -piam59\!z$ -e "SELECT start_time, query_time, lock_time, rows_examined, rows_sent, db, sql_text FROM mysql.slow_log ORDER BY start_time DESC LIMIT 20;"
cd /home/mxl/cdmp-mini/cdmp-mini && ADMIN_TOKEN=$(curl -s -H 'Content-Type: application/json' -d '{"username":"admin","password":"Admin@2021"}' <http://192.168.10.8:8088/login> | python3 -c "import sys,json; data=json.load(sys.stdin); print(data.get('data',{}).get('access_token',''));") && mysql -uroot -piam59\!z$ -B -N -e "SELECT name FROM iam.user WHERE name LIKE 'createperf%';" | while read -r name; do [ -z "$name" ] && continue; status=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE -H "Authorization: Bearer $ADMIN_TOKEN" <http://192.168.10.8:8088/v1/users/$name/force>); echo "$status $name"; done
