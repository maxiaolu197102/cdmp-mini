package user

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/dbscan"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	gormutil "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/util"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *Users) List(ctx context.Context, username string, opts metav1.ListOptions, opt *options.Options) (*v1.UserList, error) {
	traceCtx, span := trace.StartSpan(ctx, "user-store", "list_users")
	if traceCtx != nil {
		ctx = traceCtx
	}
	trace.AddRequestTag(ctx, "target_user", username)

	spanStatus := "success"
	spanCode := strconv.Itoa(code.ErrSuccess)
	spanDetails := map[string]any{
		"username": username,
	}
	defer func() {
		if span != nil {
			trace.EndSpan(span, spanStatus, spanCode, spanDetails)
		}
	}()

	ret := &v1.UserList{}
	ol := gormutil.Unpointer(opts.Offset, opts.Limit)
	if ol.Limit <= 0 || ol.Limit > gormutil.DefaultLimit {
		ol.Limit = gormutil.DefaultLimit
	}

	sqlCore, err := u.ensureSQLCore()
	if err != nil {
		spanStatus = "error"
		spanCode = strconv.Itoa(code.ErrDatabase)
		return nil, errors.WithCode(code.ErrDatabase, "获取数据库连接失败: %v", err)
	}

	whereParts := []string{"status = 1"}
	args := make([]interface{}, 0, 2)
	if username != "" {
		whereParts = append(whereParts, "name = ?")
		args = append(args, username)
	}
	whereClause := strings.Join(whereParts, " AND ")

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `user` WHERE %s", whereClause)
	if err := sqlCore.QueryRowContext(ctx, countQuery, args...).Scan(&ret.TotalCount); err != nil {
		spanStatus = "error"
		spanCode = strconv.Itoa(code.ErrDatabase)
		return nil, errors.WithCode(code.ErrDatabase, "统计用户数量失败: %v", err)
	}

	listQuery := fmt.Sprintf("SELECT id, instanceID, name, nickname, email, phone, status, isAdmin, createdAt, updatedAt, loginedAt, version FROM `user` WHERE %s  LIMIT ? OFFSET ?", whereClause)
	listArgs := append(append([]interface{}{}, args...), ol.Limit, ol.Offset)
	rows, err := sqlCore.QueryContext(ctx, listQuery, listArgs...)
	if err != nil {
		spanStatus = "error"
		spanCode = strconv.Itoa(code.ErrDatabase)
		return nil, errors.WithCode(code.ErrDatabase, "查询用户列表失败: %v", err)
	}
	defer rows.Close()
	itemsStorage := make([]v1.User, 0, ol.Limit)
	ret.Items = make([]*v1.User, 0, ol.Limit)
	for rows.Next() {
		itemsStorage = append(itemsStorage, v1.User{})
		userPtr := &itemsStorage[len(itemsStorage)-1]
		if _, scanErr := dbscan.ScanUserLiteInto(rows, userPtr); scanErr != nil {
			spanStatus = "error"
			spanCode = strconv.Itoa(code.ErrDatabase)
			return nil, errors.WithCode(code.ErrDatabase, "扫描用户记录失败: %v", scanErr)
		}
		ret.Items = append(ret.Items, userPtr)
	}
	if err := rows.Err(); err != nil {
		spanStatus = "error"
		spanCode = strconv.Itoa(code.ErrDatabase)
		return nil, errors.WithCode(code.ErrDatabase, "遍历用户列表失败: %v", err)
	}

	spanDetails["returned_count"] = len(ret.Items)
	return ret, nil
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
