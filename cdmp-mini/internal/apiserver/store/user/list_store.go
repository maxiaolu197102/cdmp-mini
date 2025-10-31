package user

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/dbscan"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	gormutil "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/util"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *Users) List(ctx context.Context, opts metav1.ListOptions, opt *options.Options) (*v1.UserList, error) {
	traceCtx, span := trace.StartSpan(ctx, "user-store", "list_users")
	if traceCtx != nil {
		ctx = traceCtx
	}

	spanStatus := "success"
	spanCode := strconv.Itoa(code.ErrSuccess)
	spanDetails := map[string]any{
		"field_selector": opts.FieldSelector,
	}
	if opt != nil && opt.MysqlOptions != nil && opt.MysqlOptions.LoadBalance {
		spanDetails["mysql_load_balance"] = true
	}
	defer func() {
		if span != nil {
			trace.EndSpan(span, spanStatus, spanCode, spanDetails)
		}
	}()

	if name := strings.TrimSpace(opts.Identifiers.Name); name != "" {
		spanDetails["identifier_name"] = name
	}
	if opts.Identifiers.ID != nil {
		spanDetails["identifier_id"] = *opts.Identifiers.ID
	}
	if instance := strings.TrimSpace(opts.Identifiers.InstanceID); instance != "" {
		spanDetails["identifier_instance"] = instance
	}
	if len(opts.Status.Statuses) > 0 {
		spanDetails["status_filters"] = opts.Status.Statuses
	}
	if opts.Status.IsAdmin != nil {
		spanDetails["is_admin_filter"] = *opts.Status.IsAdmin
	}

	ol := gormutil.Unpointer(opts.Offset, opts.Limit)
	if ol.Limit <= 0 || ol.Limit > gormutil.DefaultLimit {
		ol.Limit = gormutil.DefaultLimit
	}
	if opts.Limit != nil {
		spanDetails["limit"] = *opts.Limit
	}
	if opts.Offset != nil {
		spanDetails["offset"] = *opts.Offset
	}

	sqlCore, err := u.ensureSQLCore()
	if err != nil {
		spanStatus = "error"
		spanCode = strconv.Itoa(code.ErrDatabase)
		return nil, errors.WithCode(code.ErrDatabase, "获取数据库连接失败: %v", err)
	}

	whereClause, args, filterCount, defaultStatus := buildUserListFilters(opts)
	spanDetails["filter_count"] = filterCount
	if defaultStatus {
		spanDetails["default_status_applied"] = true
	}

	listQuery := "SELECT id, instanceID, name, nickname, email, phone, status, isAdmin, createdAt, updatedAt, loginedAt, version FROM `user`" + whereClause + " ORDER BY id DESC LIMIT ? OFFSET ?"
	listArgs := append(append([]interface{}{}, args...), ol.Limit, ol.Offset)
	rows, err := sqlCore.QueryContext(ctx, listQuery, listArgs...)
	if err != nil {
		spanStatus = "error"
		spanCode = strconv.Itoa(code.ErrDatabase)
		return nil, errors.WithCode(code.ErrDatabase, "查询用户列表失败: %v", err)
	}
	defer rows.Close()

	itemsStorage := make([]v1.User, 0, ol.Limit)
	items := make([]*v1.User, 0, ol.Limit)
	for rows.Next() {
		itemsStorage = append(itemsStorage, v1.User{})
		userPtr := &itemsStorage[len(itemsStorage)-1]
		if _, scanErr := dbscan.ScanUserLiteInto(rows, userPtr); scanErr != nil {
			spanStatus = "error"
			spanCode = strconv.Itoa(code.ErrDatabase)
			return nil, errors.WithCode(code.ErrDatabase, "扫描用户记录失败: %v", scanErr)
		}
		items = append(items, userPtr)
	}
	if err := rows.Err(); err != nil {
		spanStatus = "error"
		spanCode = strconv.Itoa(code.ErrDatabase)
		return nil, errors.WithCode(code.ErrDatabase, "遍历用户列表失败: %v", err)
	}

	spanDetails["returned_count"] = len(items)
	return &v1.UserList{Items: items}, nil
}

func buildUserListFilters(opts metav1.ListOptions) (string, []interface{}, int, bool) {
	parts := make([]string, 0, 12)
	args := make([]interface{}, 0, 24)
	defaultStatus := false

	if len(opts.Status.Statuses) == 0 {
		parts = append(parts, "status = ?")
		args = append(args, 1)
		defaultStatus = true
	} else {
		placeholders := make([]string, 0, len(opts.Status.Statuses))
		for _, status := range opts.Status.Statuses {
			placeholders = append(placeholders, "?")
			args = append(args, status)
		}
		parts = append(parts, fmt.Sprintf("status IN (%s)", strings.Join(placeholders, ",")))
	}

	if opts.Status.IsAdmin != nil {
		parts = append(parts, "isAdmin = ?")
		args = append(args, boolToTinyInt(*opts.Status.IsAdmin))
	}

	if opts.Identifiers.ID != nil {
		parts = append(parts, "id = ?")
		args = append(args, *opts.Identifiers.ID)
	}
	if instance := strings.TrimSpace(opts.Identifiers.InstanceID); instance != "" {
		parts = append(parts, "instanceID = ?")
		args = append(args, instance)
	}
	if name := strings.TrimSpace(opts.Identifiers.Name); name != "" {
		parts = append(parts, "name = ?")
		args = append(args, name)
	}

	if email := strings.TrimSpace(opts.Contact.Email); email != "" {
		if normalized := usercache.NormalizeEmail(email); normalized != "" {
			parts = append(parts, "email = ?")
			args = append(args, normalized)
		}
	}
	if emailLike := strings.TrimSpace(opts.Contact.EmailLike); emailLike != "" {
		parts = append(parts, "email LIKE ?")
		args = append(args, emailLike)
	}
	if phone := strings.TrimSpace(opts.Contact.Phone); phone != "" {
		if normalized := usercache.NormalizePhone(phone); normalized != "" {
			parts = append(parts, "phone = ?")
			args = append(args, normalized)
		}
	}
	if phoneLike := strings.TrimSpace(opts.Contact.PhoneLike); phoneLike != "" {
		parts = append(parts, "phone LIKE ?")
		args = append(args, phoneLike)
	}

	if opts.Time.CreatedAtGTE != nil {
		parts = append(parts, "createdAt >= ?")
		args = append(args, opts.Time.CreatedAtGTE.UTC())
	}
	if opts.Time.CreatedAtLTE != nil {
		parts = append(parts, "createdAt <= ?")
		args = append(args, opts.Time.CreatedAtLTE.UTC())
	}
	if opts.Time.UpdatedAtGT != nil {
		parts = append(parts, "updatedAt > ?")
		args = append(args, opts.Time.UpdatedAtGT.UTC())
	}
	if opts.Time.UpdatedAtLT != nil {
		parts = append(parts, "updatedAt < ?")
		args = append(args, opts.Time.UpdatedAtLT.UTC())
	}
	if opts.Time.LoginedAtLT != nil {
		parts = append(parts, "loginedAt < ?")
		args = append(args, opts.Time.LoginedAtLT.UTC())
	}

	if len(opts.Extend.Equals) > 0 {
		keys := make([]string, 0, len(opts.Extend.Equals))
		for key := range opts.Extend.Equals {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			path, ok := jsonPathForKey(key)
			if !ok {
				continue
			}
			parts = append(parts, fmt.Sprintf("JSON_UNQUOTE(JSON_EXTRACT(extendShadow, '%s')) = ?", path))
			args = append(args, strings.TrimSpace(opts.Extend.Equals[key]))
		}
	}

	if len(opts.Extend.Contains) > 0 {
		keys := make([]string, 0, len(opts.Extend.Contains))
		for key := range opts.Extend.Contains {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			path, ok := jsonPathForKey(key)
			if !ok {
				continue
			}
			parts = append(parts, fmt.Sprintf("JSON_CONTAINS(extendShadow, JSON_QUOTE(?), '%s')", path))
			args = append(args, strings.TrimSpace(opts.Extend.Contains[key]))
		}
	}

	if len(opts.Extend.In) > 0 {
		keys := make([]string, 0, len(opts.Extend.In))
		for key := range opts.Extend.In {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			path, ok := jsonPathForKey(key)
			if !ok {
				continue
			}
			values := opts.Extend.In[key]
			if len(values) == 0 {
				continue
			}
			placeholders := make([]string, 0, len(values))
			for _, value := range values {
				placeholders = append(placeholders, "?")
				args = append(args, strings.TrimSpace(value))
			}
			parts = append(parts, fmt.Sprintf("JSON_UNQUOTE(JSON_EXTRACT(extendShadow, '%s')) IN (%s)", path, strings.Join(placeholders, ",")))
		}
	}

	clause := ""
	if len(parts) > 0 {
		clause = " WHERE " + strings.Join(parts, " AND ")
	}
	return clause, args, len(parts), defaultStatus
}

var jsonKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

func jsonPathForKey(key string) (string, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", false
	}
	segments := strings.Split(key, ".")
	for _, segment := range segments {
		if !jsonKeyPattern.MatchString(segment) {
			return "", false
		}
	}
	return "$." + strings.Join(segments, "."), true
}

func boolToTinyInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (u *Users) ListAllUsernames(ctx context.Context) ([]string, error) {
	sqlCore, err := u.ensureSQLCore()
	if err != nil {
		return nil, errors.WithCode(code.ErrDatabase, "获取数据库连接失败: %v", err)
	}

	rows, err := sqlCore.QueryContext(ctx, "SELECT name FROM `user`")
	if err != nil {
		return nil, errors.WithCode(code.ErrDatabase, "查询用户名列表失败: %v", err)
	}
	defer rows.Close()

	usernames := make([]string, 0, 64)
	for rows.Next() {
		var name string
		if scanErr := rows.Scan(&name); scanErr != nil {
			return nil, errors.WithCode(code.ErrDatabase, "扫描用户名失败: %v", scanErr)
		}
		usernames = append(usernames, name)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.WithCode(code.ErrDatabase, "遍历用户名失败: %v", err)
	}

	return usernames, nil
}

func (u *Users) ListAll(ctx context.Context, username string) (*v1.UserList, error) {
	ret := &v1.UserList{}
	sqlCore, err := u.ensureSQLCore()
	if err != nil {
		return nil, errors.WithCode(code.ErrDatabase, "获取数据库连接失败: %v", err)
	}

	whereParts := []string{"status = 1"}
	args := make([]interface{}, 0, 1)
	if username != "" {
		whereParts = append(whereParts, "name LIKE ?")
		args = append(args, "%"+username+"%")
	}
	whereClause := strings.Join(whereParts, " AND ")
	query := fmt.Sprintf("SELECT id, instanceID, name, nickname, email, phone, status, isAdmin, createdAt, updatedAt, loginedAt, version FROM `user` WHERE %s ORDER BY id DESC", whereClause)
	rows, err := sqlCore.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.WithCode(code.ErrDatabase, "查询用户列表失败: %v", err)
	}
	defer rows.Close()

	itemsStorage := make([]v1.User, 0, 64)
	for rows.Next() {
		itemsStorage = append(itemsStorage, v1.User{})
		userPtr := &itemsStorage[len(itemsStorage)-1]
		if _, scanErr := dbscan.ScanUserLiteInto(rows, userPtr); scanErr != nil {
			return nil, errors.WithCode(code.ErrDatabase, "扫描用户记录失败: %v", scanErr)
		}
		ret.Items = append(ret.Items, userPtr)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.WithCode(code.ErrDatabase, "遍历用户记录失败: %v", err)
	}

	ret.TotalCount = int64(len(ret.Items))
	return ret, nil
}
