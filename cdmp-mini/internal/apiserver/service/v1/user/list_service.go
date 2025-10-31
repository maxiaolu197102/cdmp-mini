package user

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/bytedance/gopkg/util/logger"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/fields"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *UserService) List(ctx context.Context, opts metav1.ListOptions, opt *options.Options) (result *v1.UserList, err error) {
	serviceCtx, serviceSpan := trace.StartSpan(ctx, "user-service", "list_users")
	if serviceCtx != nil {
		ctx = serviceCtx
	}

	spanStatus := "success"
	businessCode := strconv.Itoa(code.ErrSuccess)
	spanDetails := map[string]any{
		"field_selector": opts.FieldSelector,
	}
	outcomeStatus := "success"
	outcomeCode := businessCode
	outcomeMessage := ""
	outcomeHTTP := http.StatusOK

	defer func() {
		if err != nil {
			spanStatus = "error"
			outcomeStatus = "error"
			if c := errors.GetCode(err); c != 0 {
				businessCode = strconv.Itoa(c)
				outcomeCode = businessCode
			} else {
				businessCode = strconv.Itoa(code.ErrUnknown)
				outcomeCode = businessCode
			}
			if msg := errors.GetMessage(err); msg != "" {
				outcomeMessage = msg
			}
			if status := errors.GetHTTPStatus(err); status != 0 {
				outcomeHTTP = status
			} else {
				outcomeHTTP = http.StatusInternalServerError
			}
		}
		if opts.Limit != nil {
			spanDetails["limit"] = *opts.Limit
		}
		if opts.Offset != nil {
			spanDetails["offset"] = *opts.Offset
		}
		if serviceSpan != nil {
			trace.EndSpan(serviceSpan, spanStatus, businessCode, spanDetails)
		}
		trace.RecordOutcome(ctx, outcomeCode, outcomeMessage, outcomeStatus, outcomeHTTP)
	}()

	startTime := time.Now()

	// 兼容字段选择器语法，自动转换为标识符过滤
	if opts.FieldSelector != "" {
		selector, err := fields.ParseSelector(opts.FieldSelector)
		if err != nil {
			return nil, err
		}
		if exactName, ok := selector.RequiresExactMatch("name"); ok && exactName != "" && opts.Identifiers.Name == "" {
			opts.Identifiers.Name = exactName
		}
	}

	// 将关键过滤条件记录到链路，便于排查
	if opts.Identifiers.Name != "" {
		trace.AddRequestTag(ctx, "target_user", opts.Identifiers.Name)
		spanDetails["target_user"] = opts.Identifiers.Name
	}
	if opts.Identifiers.ID != nil {
		spanDetails["target_id"] = *opts.Identifiers.ID
	}
	if opts.Identifiers.InstanceID != "" {
		spanDetails["target_instance"] = opts.Identifiers.InstanceID
	}
	if len(opts.Status.Statuses) > 0 {
		spanDetails["status_filters"] = opts.Status.Statuses
	}
	if opts.Status.IsAdmin != nil {
		spanDetails["is_admin_filter"] = *opts.Status.IsAdmin
	}
	if opts.Contact.Email != "" {
		spanDetails["email_filter"] = opts.Contact.Email
	}
	if opts.Contact.EmailLike != "" {
		spanDetails["email_like_filter"] = opts.Contact.EmailLike
	}
	if opts.Contact.Phone != "" {
		spanDetails["phone_filter"] = opts.Contact.Phone
	}
	if opts.Contact.PhoneLike != "" {
		spanDetails["phone_like_filter"] = opts.Contact.PhoneLike
	}

	// 步骤1：查询原始用户列表
	users, err := u.Store.Users().List(ctx, opts, u.Options)
	if err != nil {
		log.Errorf("List users from storage failed: %v", err)
		return nil, errors.WithCode(code.ErrDatabase, "query raw users failed: %v", err)
	}

	if len(users.Items) == 0 {
		logger.Debugf("没有发现用户，返回空列表")
		return &v1.UserList{ListMeta: users.ListMeta, Items: []*v1.User{}}, nil
	}

	logger.Debugf("发现 %d 用户列表, 开始批量填充关联数据", len(users.Items))

	policyCounts, err := u.batchPolicyCounts(ctx, users.Items)
	if err != nil {
		logger.Errorf("batch fetch policy counts failed: %v", err)
		return nil, errors.WithCode(code.ErrDatabase, "batch query policy totals failed: %v", err)
	}

	resultItems := make([]*v1.User, len(users.Items))
	for i, user := range users.Items {
		if user == nil {
			continue
		}
		copyUser := *user
		if count, ok := policyCounts[user.Name]; ok {
			copyUser.TotalPolicy = count
		} else {
			copyUser.TotalPolicy = 0
		}
		resultItems[i] = &copyUser
	}

	processingTime := time.Since(startTime)
	logger.Debugf("Successfully processed %d users in %v", len(resultItems), processingTime)

	result = &v1.UserList{
		ListMeta: users.ListMeta,
		Items:    resultItems,
	}
	spanDetails["returned_count"] = len(resultItems)
	return result, nil
}

func (u *UserService) ListWithBadPerformance(ctx context.Context, opts metav1.ListOptions, opt *options.Options) (*v1.UserList, error) {
	return nil, nil
}

func (u *UserService) batchPolicyCounts(ctx context.Context, users []*v1.User) (map[string]int64, error) {
	counts := make(map[string]int64, len(users))
	if u == nil || u.Store == nil {
		return counts, nil
	}
	names := make([]string, 0, len(users))
	for _, user := range users {
		if user == nil || user.Name == "" {
			continue
		}
		names = append(names, user.Name)
	}
	if len(names) == 0 {
		return counts, nil
	}
	store := u.Store.Polices()
	if store == nil {
		return counts, nil
	}
	return store.CountByUsernames(ctx, names)
}
