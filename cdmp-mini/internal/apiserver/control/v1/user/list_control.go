package user

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/audit"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/common"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/fields"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *UserController) List(ctx *gin.Context) {

	traceCtx := ctx.Request.Context()
	operator := common.GetUsername(traceCtx)
	trace.SetOperator(traceCtx, operator)

	controllerCtx, controllerSpan := trace.StartSpan(traceCtx, "user-controller", "list_users")
	if controllerCtx == nil {
		controllerCtx = traceCtx
	} else {
		ctx.Request = ctx.Request.WithContext(controllerCtx)
	}
	trace.SetOperator(controllerCtx, operator)
	trace.AddRequestTag(controllerCtx, "controller", "list_users")

	controllerStatus := "success"
	controllerCode := strconv.Itoa(code.ErrSuccess)
	controllerDetails := map[string]interface{}{
		"request_id": ctx.Request.Header.Get("X-Request-ID"),
		"operator":   operator,
	}
	defer func() {
		if controllerSpan != nil {
			trace.EndSpan(controllerSpan, controllerStatus, controllerCode, controllerDetails)
		}
	}()

	outcomeStatus := "success"
	outcomeCode := strconv.Itoa(code.ErrSuccess)
	outcomeMessage := ""
	outcomeHTTP := http.StatusOK

	auditLog := func(outcome string, err error, meta map[string]any) {
		event := audit.BuildEventFromRequest(ctx.Request)
		event.Action = "user.list"
		event.ResourceType = "user"
		event.ResourceID = "collection"
		event.Actor = operator
		event.Outcome = outcome
		if err != nil {
			event.ErrorMessage = err.Error()
		}
		if len(meta) > 0 {
			event.Metadata = meta
		}
		submitAudit(ctx, event)
	}

	err := metrics.MonitorBusinessOperation("user_service", "list", "http", func() error {
		var opts metav1.ListOptions
		if err := ctx.ShouldBindQuery(&opts); err != nil {
			errWrap := errors.WithCode(code.ErrBind, "传入的参数错误")
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errWrap))
			if controllerCode == "0" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errWrap)
			outcomeHTTP = errors.GetHTTPStatus(errWrap)
			core.WriteResponse(ctx, errWrap, nil)
			auditLog("fail", errWrap, map[string]any{"stage": "bind"})
			return errWrap
		}

		if err := populateUserListFilters(&opts, ctx.Request.URL.Query()); err != nil {
			errWrap := errors.WrapC(err, code.ErrInvalidParameter, "%s", err.Error())
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errWrap))
			if controllerCode == "0" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errWrap)
			outcomeHTTP = errors.GetHTTPStatus(errWrap)
			core.WriteResponse(ctx, errWrap, nil)
			auditLog("fail", errWrap, map[string]any{"stage": "parse_filters"})
			return errWrap
		}

		if opts.Identifiers.ID != nil {
			controllerDetails["identifier_id"] = *opts.Identifiers.ID
		}
		if opts.Identifiers.InstanceID != "" {
			controllerDetails["identifier_instance"] = opts.Identifiers.InstanceID
		}
		if opts.Identifiers.Name != "" {
			controllerDetails["identifier_name"] = opts.Identifiers.Name
		}
		if len(opts.Status.Statuses) > 0 {
			controllerDetails["status_filters"] = opts.Status.Statuses
		}
		if opts.Status.IsAdmin != nil {
			controllerDetails["is_admin_filter"] = *opts.Status.IsAdmin
		}
		if opts.Contact.Email != "" {
			controllerDetails["email_filter"] = opts.Contact.Email
		}
		if opts.Contact.EmailLike != "" {
			controllerDetails["email_like_filter"] = opts.Contact.EmailLike
		}
		if opts.Contact.Phone != "" {
			controllerDetails["phone_filter"] = opts.Contact.Phone
		}
		if opts.Contact.PhoneLike != "" {
			controllerDetails["phone_like_filter"] = opts.Contact.PhoneLike
		}
		if opts.Time.CreatedAtGTE != nil {
			controllerDetails["created_at_gte"] = opts.Time.CreatedAtGTE.Format(time.RFC3339)
		}
		if opts.Time.CreatedAtLTE != nil {
			controllerDetails["created_at_lte"] = opts.Time.CreatedAtLTE.Format(time.RFC3339)
		}
		if opts.Time.UpdatedAtGT != nil {
			controllerDetails["updated_at_gt"] = opts.Time.UpdatedAtGT.Format(time.RFC3339)
		}
		if opts.Time.UpdatedAtLT != nil {
			controllerDetails["updated_at_lt"] = opts.Time.UpdatedAtLT.Format(time.RFC3339)
		}
		if opts.Time.LoginedAtLT != nil {
			controllerDetails["logined_at_lt"] = opts.Time.LoginedAtLT.Format(time.RFC3339)
		}

		// 尝试从 fieldSelector 中解析用户名，并记录在链路中
		if opts.FieldSelector != "" {
			if controllerDetails != nil {
				controllerDetails["field_selector"] = opts.FieldSelector
			}
			if selector, err := fields.ParseSelector(opts.FieldSelector); err == nil {
				if username, ok := selector.RequiresExactMatch("name"); ok {
					trace.AddRequestTag(controllerCtx, "target_user", username)
					controllerDetails["target_user"] = username
				}
			}
		}
		if opts.Limit != nil {
			controllerDetails["limit"] = *opts.Limit
		}
		if opts.Offset != nil {
			controllerDetails["offset"] = *opts.Offset
		}

		errs := u.validateListOptions(&opts)
		if len(errs) > 0 {
			errDetails := make(map[string]string, len(errs))
			for _, fieldErr := range errs {
				errDetails[fieldErr.Field] = fieldErr.ErrorBody()
			}
			detailStr := fmt.Sprintf("参数错误:%+v", errDetails)
			errValidate := errors.WrapC(nil, code.ErrInvalidParameter, "%s", detailStr)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errValidate))
			if controllerCode == "0" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errValidate)
			outcomeHTTP = errors.GetHTTPStatus(errValidate)
			core.WriteResponse(ctx, errValidate, nil)
			auditLog("fail", errValidate, map[string]any{"stage": "validate"})
			return errValidate
		}

		requestCtx := controllerCtx
		if _, hasDeadline := requestCtx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			timeout := u.options.ServerRunOptions.CtxTimeout
			if timeout == 0 {
				timeout = 30 * time.Second
			}
			requestCtx, cancel = context.WithTimeout(requestCtx, timeout)
			defer cancel()
		}

		userList, err := u.srv.Users().List(requestCtx, opts, u.options)
		if err != nil {
			errWrap := errors.WrapC(err, code.ErrInternal, "%s", errors.GetMessage(err))
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errWrap))
			if controllerCode == "0" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errWrap)
			outcomeHTTP = errors.GetHTTPStatus(errWrap)
			core.WriteResponse(ctx, errWrap, nil)
			auditLog("fail", errWrap, map[string]any{"stage": "service"})
			return errWrap
		}

		var publicUsers []*v1.PublicUser
		if len(userList.Items) > 0 {
			for _, user := range userList.Items {
				publicUser := v1.ConvertToPublicUser(user)
				publicUsers = append(publicUsers, publicUser)
			}
		}

		controllerDetails["returned_count"] = len(publicUsers)
		core.WriteResponse(ctx, nil, publicUsers)

		meta := map[string]any{
			"returned_count": len(publicUsers),
		}
		if opts.Limit != nil {
			meta["limit"] = *opts.Limit
		}
		if opts.Offset != nil {
			meta["offset"] = *opts.Offset
		}
		if opts.LabelSelector != "" {
			meta["label_selector"] = opts.LabelSelector
		}
		if opts.FieldSelector != "" {
			meta["field_selector"] = opts.FieldSelector
		}
		auditLog("success", nil, meta)
		return nil
	})

	if err != nil && outcomeStatus == "success" {
		outcomeStatus = "error"
		outcomeCode = strconv.Itoa(errors.GetCode(err))
		if outcomeCode == "0" {
			outcomeCode = strconv.Itoa(code.ErrUnknown)
		}
		outcomeMessage = errors.GetMessage(err)
		outcomeHTTP = errors.GetHTTPStatus(err)
		controllerStatus = "error"
		controllerCode = outcomeCode
	}

	trace.RecordOutcome(controllerCtx, outcomeCode, outcomeMessage, outcomeStatus, outcomeHTTP)
}

var (
	extendKeySegmentPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	supportedTimeLayouts    = []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	}
)

func populateUserListFilters(opts *metav1.ListOptions, values url.Values) error {
	if opts == nil {
		return fmt.Errorf("ListOptions 不能为空")
	}
	if values == nil {
		return nil
	}

	for key, rawVals := range values {
		if len(rawVals) == 0 {
			continue
		}

		switch key {
		case "id":
			raw := strings.TrimSpace(rawVals[len(rawVals)-1])
			if raw == "" {
				continue
			}
			id, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				return fmt.Errorf("id 必须为数字")
			}
			idCopy := id
			opts.Identifiers.ID = &idCopy

		case "instanceID":
			if val := strings.TrimSpace(rawVals[len(rawVals)-1]); val != "" {
				opts.Identifiers.InstanceID = val
			}

		case "name":
			if val := strings.TrimSpace(rawVals[len(rawVals)-1]); val != "" {
				opts.Identifiers.Name = val
			}

		case "status", "status[]":
			statuses, err := parseStatusValues(rawVals)
			if err != nil {
				return err
			}
			if len(statuses) > 0 {
				opts.Status.Statuses = statuses
			}

		case "isAdmin":
			raw := strings.TrimSpace(rawVals[len(rawVals)-1])
			if raw == "" {
				continue
			}
			boolVal, err := parseFlexibleBool(raw)
			if err != nil {
				return fmt.Errorf("isAdmin 解析失败: %w", err)
			}
			opts.Status.IsAdmin = &boolVal

		case "email":
			if val := strings.TrimSpace(rawVals[len(rawVals)-1]); val != "" {
				opts.Contact.Email = val
			}

		case "email[like]":
			if val := strings.TrimSpace(rawVals[len(rawVals)-1]); val != "" {
				opts.Contact.EmailLike = buildLikePattern(val)
			}

		case "phone":
			if val := strings.TrimSpace(rawVals[len(rawVals)-1]); val != "" {
				opts.Contact.Phone = val
			}

		case "phone[like]":
			if val := strings.TrimSpace(rawVals[len(rawVals)-1]); val != "" {
				opts.Contact.PhoneLike = buildLikePattern(val)
			}

		case "createdAt[gte]", "createdAt[lte]", "updatedAt[gt]", "updatedAt[lt]", "loginedAt[lt]":
			raw := strings.TrimSpace(rawVals[len(rawVals)-1])
			if raw == "" {
				continue
			}
			parsed, err := parseTimeValue(raw)
			if err != nil {
				return fmt.Errorf("%s 解析失败: %w", key, err)
			}
			assignTimeFilter(opts, key, parsed)

		default:
			if strings.HasPrefix(key, "extend.") {
				if err := parseExtendFilter(opts, key, rawVals); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func parseStatusValues(rawVals []string) ([]int, error) {
	if len(rawVals) == 0 {
		return nil, nil
	}
	seen := make(map[int]struct{})
	result := make([]int, 0, len(rawVals))
	for _, raw := range rawVals {
		parts := strings.Split(raw, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			value, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("status 参数值非法: %s", part)
			}
			if _, exists := seen[value]; exists {
				continue
			}
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	return result, nil
}

func parseFlexibleBool(raw string) (bool, error) {
	switch strings.ToLower(raw) {
	case "1":
		return true, nil
	case "0":
		return false, nil
	default:
		return strconv.ParseBool(raw)
	}
}

func buildLikePattern(raw string) string {
	pattern := strings.TrimSpace(raw)
	if pattern == "" {
		return ""
	}
	if strings.Contains(pattern, "%") {
		return pattern
	}
	escaped := escapeLikeLiteral(pattern)
	return escaped + "%"
}

func escapeLikeLiteral(input string) string {
	if input == "" {
		return ""
	}
	var builder strings.Builder
	builder.Grow(len(input) * 2)
	for _, r := range input {
		switch r {
		case '\\', '%', '_':
			builder.WriteByte('\\')
		}
		builder.WriteRune(r)
	}
	return builder.String()
}

func parseTimeValue(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("时间不能为空")
	}
	for _, layout := range supportedTimeLayouts {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("时间格式不支持: %s", raw)
}

func assignTimeFilter(opts *metav1.ListOptions, key string, value time.Time) {
	v := value
	switch key {
	case "createdAt[gte]":
		opts.Time.CreatedAtGTE = &v
	case "createdAt[lte]":
		opts.Time.CreatedAtLTE = &v
	case "updatedAt[gt]":
		opts.Time.UpdatedAtGT = &v
	case "updatedAt[lt]":
		opts.Time.UpdatedAtLT = &v
	case "loginedAt[lt]":
		opts.Time.LoginedAtLT = &v
	}
}

func parseExtendFilter(opts *metav1.ListOptions, key string, rawVals []string) error {
	if len(rawVals) == 0 {
		return nil
	}
	value := strings.TrimSpace(rawVals[len(rawVals)-1])
	if value == "" {
		return nil
	}
	fieldExpr := strings.TrimPrefix(key, "extend.")
	field, operator := splitExtendOperator(fieldExpr)
	normalizedField, err := normalizeExtendField(field)
	if err != nil {
		return err
	}

	switch operator {
	case "contains":
		if opts.Extend.Contains == nil {
			opts.Extend.Contains = make(map[string]string)
		}
		opts.Extend.Contains[normalizedField] = value
	case "in":
		values := splitCSV(value)
		if len(values) == 0 {
			return nil
		}
		if opts.Extend.In == nil {
			opts.Extend.In = make(map[string][]string)
		}
		opts.Extend.In[normalizedField] = values
	default:
		if opts.Extend.Equals == nil {
			opts.Extend.Equals = make(map[string]string)
		}
		opts.Extend.Equals[normalizedField] = value
	}
	return nil
}

func splitExtendOperator(expr string) (string, string) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return "", ""
	}
	if idx := strings.Index(trimmed, "["); idx != -1 && strings.HasSuffix(trimmed, "]") && idx < len(trimmed)-1 {
		field := strings.TrimSpace(trimmed[:idx])
		operator := strings.TrimSpace(trimmed[idx+1 : len(trimmed)-1])
		if operator == "" {
			operator = "equals"
		}
		return field, operator
	}
	return trimmed, "equals"
}

func normalizeExtendField(field string) (string, error) {
	trimmed := strings.TrimSpace(field)
	if trimmed == "" {
		return "", fmt.Errorf("extend 字段名不能为空")
	}
	segments := strings.Split(trimmed, ".")
	cleaned := make([]string, 0, len(segments))
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return "", fmt.Errorf("extend 字段名 %q 包含空片段", field)
		}
		if !extendKeySegmentPattern.MatchString(segment) {
			return "", fmt.Errorf("extend 字段名 %q 含非法字符", field)
		}
		cleaned = append(cleaned, segment)
	}
	return strings.Join(cleaned, "."), nil
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
