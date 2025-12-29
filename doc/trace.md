{
  "trace_id": "b110a681-481c-40d5-85ed-efe6b8ac9662",
  "level": "INFO",
  "timestamp": "2025-12-13T20:04:31.491644143Z",
  "service": "iam-apiserver",
  "component": "user-api",
  // 全局统一标签（入口注入）
  "global_tags": {
    "account_type": "standard",
    "operation_mode": "sync",
    "create_degraded": false,
    "pending_lease_id": "user:pending:demo-user-001",
    "pending_ttl": 599997,
    "contact_cache_ready": true,
    "preflight_executed": false,
    "env": "production",
    "build_version": "v0.0.0-master+$Format:%h$"
  },
  "call_chain": {
    "root_operation": "POST /v1/users",
    "start_time": 1765656271459,
    "end_time": 1765656271491,
    "spans": [
      // 🔴 根 Span：控制器入口（唯一 parent_id 为空）
      {
        "span_id": "b6393e25-8a98-42b1-b501-ae5907af327f",
        "parent_id": "",
        "component": "user-controller",
        "operation": "create_user",
        "start_time": 1765656271459,
        "end_time": 1765656271491,
        "duration_ms": 29.602157,
        "status": "success",
        "business_code": "100001",
        "tags": {
          "account_type": "standard",
          "operation_mode": "sync",
          "create_degraded": false
        },
        "details": {
          "request_id": "b110a681-481c-40d5-85ed-efe6b8ac9662",
          "created_user": "demo-user-001",
          "request_params": {
            "username": "demo-user-001",
            "email": "demo-user-001@example.com",
            "phone": "+8613800138000",
            "account_type": "standard"
          },
          "user_agent": "curl/7.81.0",
          "client_ip": "127.0.0.1"
        }
      },

      // 🟡 子 Span：模式决策（控制器触发）
      {
        "span_id": "a1b2c3d4-5678-90ef-ghij-klmnopqrstuv",
        "parent_id": "b6393e25-8a98-42b1-b501-ae5907af327f",
        "component": "user-service",
        "operation": "decide_operation_mode",
        "start_time": 1765656271459,
        "end_time": 1765656271460,
        "duration_ms": 0.352123,
        "status": "success",
        "business_code": "100001",
        "tags": {
          "mode": "sync",
          "queue_kinds_hit": false,
          "subject_block": false,
          "subject_allow": false,
          "rollout_sample": false
        },
        "details": {
          "queue_kinds": ["create", "update"],
          "block_users": ["blacklist_001", "test_*"],
          "allow_users": ["vip_001", "enterprise_001"],
          "rollout_percent": 0,
          "sticky_header": "X-Request-Id",
          "subject": "admin",
          "decision_reason": "mode_config_sync"
        }
      },

      // 🟡 子 Span：限流控制（控制器触发）
      {
        "span_id": "b2c3d4e5-6789-01fg-hijk-lmnopqrstuvw",
        "parent_id": "b6393e25-8a98-42b1-b501-ae5907af327f",
        "component": "rate-limiter",
        "operation": "preflight_limiter_wait",
        "start_time": 1765656271460,
        "end_time": 1765656271460,
        "duration_ms": 0.218765,
        "status": "success",
        "tags": {
          "limiter_name": "user_create_preflight",
          "permit_acquired": true
        },
        "details": {
          "wait_duration_ms": 0,
          "remaining_permits": 998,
          "timeout_ms": 100,
          "rate_limit": 1000
        }
      },

      // 🟡 子 Span：缓存预热检查（控制器触发）
      {
        "span_id": "c3d4e5f6-7890-12gh-ijkl-mnopqrstuvwx",
        "parent_id": "b6393e25-8a98-42b1-b501-ae5907af327f",
        "component": "user-service",
        "operation": "ensure_contact_cache_ready",
        "start_time": 1765656271460,
        "end_ti