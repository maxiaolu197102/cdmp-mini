# Task List

- [x] Search for all invocations of Users().List across the repository
- [x] Inspect each call to confirm argument signatures match (context, ListOptions, *options.Options)
- [x] Fix any incorrect invocations
- [x] gofmt updated files
- [x] go build ./... to ensure no compilation errors
- [x] Summarize findings and changes for the user
- [x] Run k6 performance tests for user list API
- [x] Collect and analyze slow query results during performance run
- [x] Enable slow query visibility (slow log or performance_schema) and capture top SQL during load
- [x] Clean up temporary k6 test users if not needed
- [x] Reset k6 environment durations and document connection monitoring plan
