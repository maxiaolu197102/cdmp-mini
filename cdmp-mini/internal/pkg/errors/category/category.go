package category

import (
	goerrors "errors"
	"strings"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
)

var businessErrorCodeCategory = map[int]string{
	code.ErrSuccess:             "success",
	code.ErrUnknown:             "unknown_error",
	code.ErrInternal:            "unknown_error",
	code.ErrInternalServer:      "unknown_error",
	code.ErrServerBusy:          "unknown_error",
	code.ErrValidation:          "validation_error",
	code.ErrInvalidParameter:    "validation_error",
	code.ErrBind:                "validation_error",
	code.ErrInvalidJSON:         "validation_error",
	code.ErrInvalidYaml:         "validation_error",
	code.ErrInvalidBasicPayload: "validation_error",
	code.ErrBase64DecodeFail:    "validation_error",
	code.ErrEncodingJSON:        "serialization_error",
	code.ErrDecodingJSON:        "serialization_error",
	code.ErrEncodingYaml:        "serialization_error",
	code.ErrDecodingYaml:        "serialization_error",
	code.ErrEncodingFailed:      "serialization_error",
	code.ErrDecodingFailed:      "serialization_error",
	code.ErrEncrypt:             "serialization_error",
	code.ErrDatabase:            "database_error",
	code.ErrDatabaseDeadlock:    "database_error",
	code.ErrDatabaseTimeout:     "timeout",
	code.ErrKafkaFailed:         "network_error",
	code.ErrRedisFailed:         "network_error",
	code.ErrUnauthorized:        "permission_error",
	code.ErrPermissionDenied:    "permission_error",
	code.ErrNotAdministrator:    "permission_error",
	code.ErrTokenInvalid:        "permission_error",
	code.ErrTokenMismatch:       "permission_error",
	code.ErrRespCodeRTRevoked:   "permission_error",
	code.ErrPasswordIncorrect:   "permission_error",
	code.ErrExpired:             "permission_error",
	code.ErrMissingHeader:       "permission_error",
	code.ErrInvalidAuthHeader:   "permission_error",
	code.ErrSignatureInvalid:    "permission_error",
	code.ErrAccountLocked:       "permission_error",
	code.ErrUserAlreadyExist:    "duplicate",
	code.ErrResourceConflict:    "duplicate",
	code.ErrUserNotFound:        "not_found",
	code.ErrPageNotFound:        "not_found",
	code.ErrUserDisabled:        "permission_error",
	code.ErrRateLimitExceeded:   "timeout",
}

// ExtractCode 尝试从错误链中提取业务错误码。
func ExtractCode(err error) (int, bool) {
	type coder interface{ Code() int }
	type causer interface{ Cause() error }

	visited := map[error]struct{}{}
	for err != nil {
		if _, seen := visited[err]; seen {
			break
		}
		visited[err] = struct{}{}

		if c, ok := err.(coder); ok {
			return c.Code(), true
		}

		if unwrapped := goerrors.Unwrap(err); unwrapped != nil {
			err = unwrapped
			continue
		}

		if c, ok := err.(causer); ok {
			next := c.Cause()
			if next == nil {
				break
			}
			err = next
			continue
		}

		break
	}

	return 0, false
}

// CategoryFromError 返回业务错误类别标签，用于统一的监控维度。
func CategoryFromError(err error) string {
	if err == nil {
		return "success"
	}

	codeValue, hasCode := ExtractCode(err)
	if hasCode {
		if category, ok := businessErrorCodeCategory[codeValue]; ok {
			return category
		}
	}

	errMsg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(errMsg, "validation"), strings.Contains(errMsg, "validate"):
		return "validation_error"
	case strings.Contains(errMsg, "timeout"):
		return "timeout"
	case strings.Contains(errMsg, "database"):
		return "database_error"
	case strings.Contains(errMsg, "network"):
		return "network_error"
	case strings.Contains(errMsg, "permission"), strings.Contains(errMsg, "unauthorized"):
		return "permission_error"
	case strings.Contains(errMsg, "marshal"), strings.Contains(errMsg, "unmarshal"):
		return "serialization_error"
	case strings.Contains(errMsg, "not found"), strings.Contains(errMsg, "不存在"):
		return "not_found"
	case strings.Contains(errMsg, "already exists"), strings.Contains(errMsg, "已存在"):
		return "duplicate"
	default:
		if hasCode {
			return "business_error"
		}
		return "unknown_error"
	}
}
