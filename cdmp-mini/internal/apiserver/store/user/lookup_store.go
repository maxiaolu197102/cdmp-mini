package user

import (
	"context"
	"database/sql"
	"strings"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

// GetByEmail locates a user record by email address.
func (u *Users) GetByEmail(ctx context.Context, email string, _ *options.Options) (*v1.User, error) {
	normalized := strings.TrimSpace(strings.ToLower(email))
	if normalized == "" {
		return nil, errors.WithCode(code.ErrInvalidParameter, "邮箱不能为空")
	}

	sqlCore, err := u.ensureSQLCore()
	if err != nil {
		return nil, errors.WithCode(code.ErrDatabase, "获取数据库连接失败: %v", err)
	}

	row := sqlCore.QueryRowContext(ctx, "SELECT name FROM `user` WHERE email = ? LIMIT 1", normalized)
	var name string
	if err := row.Scan(&name); err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.WithCode(code.ErrUserNotFound, "用户不存在")
		}
		return nil, errors.WithCode(code.ErrDatabase, "查询邮箱失败: %v", err)
	}

	return &v1.User{ObjectMeta: metav1.ObjectMeta{Name: name}}, nil
}

// GetByPhone locates a user record by phone number.
func (u *Users) GetByPhone(ctx context.Context, phone string, _ *options.Options) (*v1.User, error) {
	normalized := strings.TrimSpace(phone)
	if normalized == "" {
		return nil, errors.WithCode(code.ErrInvalidParameter, "手机号不能为空")
	}

	sqlCore, err := u.ensureSQLCore()
	if err != nil {
		return nil, errors.WithCode(code.ErrDatabase, "获取数据库连接失败: %v", err)
	}
	row := sqlCore.QueryRowContext(ctx, "SELECT name FROM `user` WHERE phone = ? LIMIT 1", normalized)
	var name string
	if err := row.Scan(&name); err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.WithCode(code.ErrUserNotFound, "用户不存在")
		}
		return nil, errors.WithCode(code.ErrDatabase, "查询手机号失败: %v", err)
	}
	return &v1.User{ObjectMeta: metav1.ObjectMeta{Name: name}}, nil
}

// PreflightConflicts detects existing records matching username/email/phone in a single round-trip.
func (u *Users) PreflightConflicts(ctx context.Context, username, email, phone string, _ *options.Options) (map[string]*v1.User, error) {
	if u == nil || u.db == nil {
		return nil, errors.WithCode(code.ErrDatabase, "用户存储未初始化")
	}

	normalizedName := strings.TrimSpace(username)
	normalizedEmail := usercache.NormalizeEmail(email)
	normalizedPhone := usercache.NormalizePhone(phone)

	queries := make([]string, 0, 3)
	args := make([]interface{}, 0, 3)

	if normalizedName != "" {
		queries = append(queries, "(SELECT 'username' AS scope, name, email, phone, status FROM `user` WHERE name = ? LIMIT 1)")
		args = append(args, normalizedName)
	}
	if normalizedEmail != "" {
		queries = append(queries, "(SELECT 'email' AS scope, name, email, phone, status FROM `user` WHERE email = ? LIMIT 1)")
		args = append(args, normalizedEmail)
	}
	if normalizedPhone != "" {
		queries = append(queries, "(SELECT 'phone' AS scope, name, email, phone, status FROM `user` WHERE phone = ? LIMIT 1)")
		args = append(args, normalizedPhone)
	}

	if len(queries) == 0 {
		return map[string]*v1.User{}, nil
	}

	sql := strings.Join(queries, " UNION ALL ")
	type conflictRow struct {
		Scope  string
		Name   string
		Email  string
		Phone  string
		Status int32
	}

	sqlCore, err := u.ensureSQLCore()
	if err != nil {
		return nil, errors.WithCode(code.ErrDatabase, "获取数据库连接失败: %v", err)
	}

	rows, err := sqlCore.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, errors.WithCode(code.ErrDatabase, "预检查查询失败: %v", err)
	}
	defer rows.Close()

	result := make(map[string]*v1.User, len(args))
	for rows.Next() {
		var row conflictRow
		if scanErr := rows.Scan(&row.Scope, &row.Name, &row.Email, &row.Phone, &row.Status); scanErr != nil {
			return nil, errors.WithCode(code.ErrDatabase, "预检查扫描失败: %v", scanErr)
		}
		scope := strings.TrimSpace(row.Scope)
		if scope == "" {
			continue
		}
		if _, exists := result[scope]; exists {
			continue
		}
		switch scope {
		case "username", "email", "phone":
			result[scope] = &v1.User{
				ObjectMeta: metav1.ObjectMeta{Name: row.Name},
				Email:      row.Email,
				Phone:      row.Phone,
				Status:     int(row.Status),
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.WithCode(code.ErrDatabase, "预检查遍历失败: %v", err)
	}
	return result, nil
}
