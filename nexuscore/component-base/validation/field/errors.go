/*
这是一个用于字段级别验证的错误处理包，适合在业务场景中进行数据验证和错误管理。以下是详细分析：

包摘要
核心功能
字段级别验证错误：提供结构化的字段验证错误信息

多种错误类型：支持11种不同的验证错误类型

错误聚合处理：支持将多个错误聚合处理

错误过滤：支持按类型过滤错误

主要组件
1. Error 结构体
go

	type Error struct {
	    Type     ErrorType   // 错误类型
	    Field    string      // 字段路径
	    BadValue interface{} // 错误值
	    Detail   string      // 详细描述
	}

2. 错误类型 (ErrorType)
go
const (

	ErrorTypeNotFound      // 值未找到
	ErrorTypeRequired      // 必填字段为空
	ErrorTypeDuplicate     // 值重复
	ErrorTypeInvalid       // 值无效
	ErrorTypeNotSupported  // 值不支持
	ErrorTypeForbidden     // 值被禁止
	ErrorTypeTooLong       // 值过长
	ErrorTypeTooMany       // 数量过多
	ErrorTypeInternal      // 内部错误

)
3. 错误列表 (ErrorList)
go
type ErrorList []*Error
函数使用方式
基本错误创建
go
// 必填字段验证
err := field.Required(field.NewPath("username"), "用户名不能为空")

// 无效值验证
err := field.Invalid(field.NewPath("age"), -1, "年龄不能为负数")

// 重复值验证
err := field.Duplicate(field.NewPath("email"), "test@example.com")

// 不支持的值验证
err := field.NotSupported(

	field.NewPath("status"),
	"pending",
	[]string{"active", "inactive"}

)

// 值过长验证
err := field.TooLong(field.NewPath("description"), longText, 100)

// 数量过多验证
err := field.TooMany(field.NewPath("items"), 15, 10)
错误处理示例
go

	func ValidateUser(user *User) field.ErrorList {
	    var allErrors field.ErrorList

	    // 验证用户名
	    if user.Username == "" {
	        allErrors = append(allErrors, field.Required(
	            field.NewPath("username"),
	            "用户名必须填写"
	        ))
	    } else if len(user.Username) > 20 {
	        allErrors = append(allErrors, field.TooLong(
	            field.NewPath("username"),
	            user.Username,
	            20
	        ))
	    }

	    // 验证邮箱格式
	    if !isValidEmail(user.Email) {
	        allErrors = append(allErrors, field.Invalid(
	            field.NewPath("email"),
	            user.Email,
	            "邮箱格式不正确"
	        ))
	    }

	    // 验证年龄范围
	    if user.Age < 0 || user.Age > 150 {
	        allErrors = append(allErrors, field.Invalid(
	            field.NewPath("age"),
	            user.Age,
	            "年龄必须在0-150之间"
	        ))
	    }

	    return allErrors
	}

// 使用错误列表

	func CreateUser(user *User) error {
	    if errs := ValidateUser(user); len(errs) > 0 {
	        // 转换为聚合错误
	        return errs.ToAggregate()
	    }
	    // 创建用户逻辑...
	    return nil
	}

错误过滤
go
// 创建错误类型匹配器
requiredMatcher := field.NewErrorTypeMatcher(field.ErrorTypeRequired)
invalidMatcher := field.NewErrorTypeMatcher(field.ErrorTypeInvalid)

// 过滤掉特定类型的错误
filteredErrors := allErrors.Filter(requiredMatcher, invalidMatcher)

// 只保留必填错误

	requiredErrors := allErrors.Filter(func(err error) bool {
	    if fieldErr, ok := err.(*field.Error); ok {
	        return fieldErr.Type == field.ErrorTypeRequired
	    }
	    return false
	})

适合的业务场景
1. API 请求验证
go

	func ValidateCreateRequest(req *CreateRequest) field.ErrorList {
	    var errs field.ErrorList

	    if req.Name == "" {
	        errs = append(errs, field.Required(
	            field.NewPath("name"),
	            "名称不能为空"
	        ))
	    }

	    if req.Price <= 0 {
	        errs = append(errs, field.Invalid(
	            field.NewPath("price"),
	            req.Price,
	            "价格必须大于0"
	        ))
	    }

	    return errs
	}

2. 配置验证
go

	func ValidateConfig(config *Config) field.ErrorList {
	    var errs field.ErrorList

	    if config.Timeout < 0 {
	        errs = append(errs, field.Invalid(
	            field.NewPath("timeout"),
	            config.Timeout,
	            "超时时间不能为负数"
	        ))
	    }

	    if !isValidLogLevel(config.LogLevel) {
	        errs = append(errs, field.NotSupported(
	            field.NewPath("logLevel"),
	            config.LogLevel,
	            []string{"debug", "info", "warn", "error"}
	        ))
	    }

	    return errs
	}

3. 表单数据验证
go

	func ValidateRegistrationForm(form *RegistrationForm) field.ErrorList {
	    var errs field.ErrorList

	    // 必填字段验证
	    if form.Username == "" {
	        errs = append(errs, field.Required(
	            field.NewPath("username"),
	            "用户名必须填写"
	        ))
	    }

	    if form.Password == "" {
	        errs = append(errs, field.Required(
	            field.NewPath("password"),
	            "密码必须填写"
	        ))
	    }

	    // 格式验证
	    if !isValidEmail(form.Email) {
	        errs = append(errs, field.Invalid(
	            field.NewPath("email"),
	            form.Email,
	            "邮箱格式不正确"
	        ))
	    }

	    // 长度验证
	    if len(form.Description) > 500 {
	        errs = append(errs, field.TooLong(
	            field.NewPath("description"),
	            form.Description,
	            500
	        ))
	    }

	    return errs
	}

4. 批量操作验证
go

	func ValidateBatchOperation(operations []Operation) field.ErrorList {
	    var errs field.ErrorList

	    // 数量限制验证
	    if len(operations) > 100 {
	        errs = append(errs, field.TooMany(
	            field.NewPath("operations"),
	            len(operations),
	            100
	        ))
	    }

	    // 逐个验证操作
	    for i, op := range operations {
	        path := field.NewPath("operations").Index(i)
	        if op.Type == "" {
	            errs = append(errs, field.Required(
	                path.Child("type"),
	                "操作类型必须指定"
	            ))
	        }
	    }

	    return errs
	}

最佳实践
结构化错误信息：使用字段路径明确指示错误位置

详细的错误描述：提供具体的错误原因和建议

错误聚合：使用 ToAggregate() 将多个错误合并返回

错误过滤：根据需要过滤特定类型的错误

国际化支持：错误消息可以考虑支持多语言

这个包特别适合需要精细字段级别验证的业务场景，如API请求验证、配置验证、表单验证等。
*/
package field

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	utilerrors "github.com/maxiaolu1981/cretem/nexuscore/errors"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/sets"
)

// Error is an implementation of the 'error' interface, which represents a
// field-level validation error.
type Error struct {
	Type     ErrorType
	Field    string
	BadValue interface{}
	Detail   string
}

var _ error = &Error{}

// Error implements the error interface.
func (v *Error) Error() string {
	return fmt.Sprintf("%s: %s", v.Field, v.ErrorBody())
}

// ErrorBody returns the error message without the field name.  This is useful
// for building nice-looking higher-level error reporting.
func (v *Error) ErrorBody() string {
	var s string
	switch v.Type {
	//nolint: exhaustive
	case ErrorTypeRequired, ErrorTypeForbidden, ErrorTypeTooLong, ErrorTypeInternal:
		s = v.Type.String()
	default:
		value := v.BadValue
		valueType := reflect.TypeOf(value)
		if value == nil || valueType == nil {
			value = "null"
		} else if valueType.Kind() == reflect.Ptr {
			if reflectValue := reflect.ValueOf(value); reflectValue.IsNil() {
				value = "null"
			} else {
				value = reflectValue.Elem().Interface()
			}
		}
		switch t := value.(type) {
		case int64, int32, float64, float32, bool:
			// use simple printer for simple types
			s = fmt.Sprintf("%s: %v", v.Type, value)
		case string:
			s = fmt.Sprintf("%s: %q", v.Type, t)
		case fmt.Stringer:
			// anything that defines String() is better than raw struct
			s = fmt.Sprintf("%s: %s", v.Type, t.String())
		default:
			// fallback to raw struct
	
			// accidental use of internal types in external serialized form.  For now, use
			// %#v, although it would be better to show a more expressive output in the future
			s = fmt.Sprintf("%s: %#v", v.Type, value)
		}
	}
	if len(v.Detail) != 0 {
		s += fmt.Sprintf(": %s", v.Detail)
	}
	return s
}

// ErrorType is a machine readable value providing more detail about why
// a field is invalid.  These values are expected to match 1-1 with
// CauseType in api/types.go.
type ErrorType string


const (
	// ErrorTypeNotFound is used to report failure to find a requested value
	// (e.g. looking up an ID).  See NotFound().
	ErrorTypeNotFound ErrorType = "FieldValueNotFound"
	// ErrorTypeRequired is used to report required values that are not
	// provided (e.g. empty strings, null values, or empty arrays).  See
	// Required().
	ErrorTypeRequired ErrorType = "FieldValueRequired"
	// ErrorTypeDuplicate is used to report collisions of values that must be
	// unique (e.g. unique IDs).  See Duplicate().
	ErrorTypeDuplicate ErrorType = "FieldValueDuplicate"
	// ErrorTypeInvalid is used to report malformed values (e.g. failed regex
	// match, too long, out of bounds).  See Invalid().
	ErrorTypeInvalid ErrorType = "FieldValueInvalid"
	// ErrorTypeNotSupported is used to report unknown values for enumerated
	// fields (e.g. a list of valid values).  See NotSupported().
	ErrorTypeNotSupported ErrorType = "FieldValueNotSupported"
	// ErrorTypeForbidden is used to report valid (as per formatting rules)
	// values which would be accepted under some conditions, but which are not
	// permitted by the current conditions (such as security policy).  See
	// Forbidden().
	ErrorTypeForbidden ErrorType = "FieldValueForbidden"
	// ErrorTypeTooLong is used to report that the given value is too long.
	// This is similar to ErrorTypeInvalid, but the error will not include the
	// too-long value.  See TooLong().
	ErrorTypeTooLong ErrorType = "FieldValueTooLong"
	// ErrorTypeTooMany is used to report "too many". This is used to
	// report that a given list has too many items. This is similar to FieldValueTooLong,
	// but the error indicates quantity instead of length.
	ErrorTypeTooMany ErrorType = "FieldValueTooMany"
	// ErrorTypeInternal is used to report other errors that are not related
	// to user input.  See InternalError().
	ErrorTypeInternal ErrorType = "InternalError"
)

// String converts a ErrorType into its corresponding canonical error message.
func (t ErrorType) String() string {
	switch t {
	case ErrorTypeNotFound:
		return "Not found"
	case ErrorTypeRequired:
		return "Required value"
	case ErrorTypeDuplicate:
		return "Duplicate value"
	case ErrorTypeInvalid:
		return "Invalid value"
	case ErrorTypeNotSupported:
		return "Unsupported value"
	case ErrorTypeForbidden:
		return "Forbidden"
	case ErrorTypeTooLong:
		return "Too long"
	case ErrorTypeTooMany:
		return "Too many"
	case ErrorTypeInternal:
		return "Internal error"
	default:
		panic(fmt.Sprintf("unrecognized validation error: %q", string(t)))
	}
}

// NotFound returns a *Error indicating "value not found".  This is
// used to report failure to find a requested value (e.g. looking up an ID).
func NotFound(field *Path, value interface{}) *Error {
	return &Error{ErrorTypeNotFound, field.String(), value, ""}
}

// Required returns a *Error indicating "value required".  This is used
// to report required values that are not provided (e.g. empty strings, null
// values, or empty arrays).
func Required(field *Path, detail string) *Error {
	return &Error{ErrorTypeRequired, field.String(), "", detail}
}

// Duplicate returns a *Error indicating "duplicate value".  This is
// used to report collisions of values that must be unique (e.g. names or IDs).
func Duplicate(field *Path, value interface{}) *Error {
	return &Error{ErrorTypeDuplicate, field.String(), value, ""}
}

// Invalid returns a *Error indicating "invalid value".  This is used
// to report malformed values (e.g. failed regex match, too long, out of bounds).
func Invalid(field *Path, value interface{}, detail string) *Error {
	return &Error{ErrorTypeInvalid, field.String(), value, detail}
}

// NotSupported returns a *Error indicating "unsupported value".
// This is used to report unknown values for enumerated fields (e.g. a list of
// valid values).
func NotSupported(field *Path, value interface{}, validValues []string) *Error {
	detail := ""
	if len(validValues) > 0 {
		quotedValues := make([]string, len(validValues))
		for i, v := range validValues {
			quotedValues[i] = strconv.Quote(v)
		}
		detail = "supported values: " + strings.Join(quotedValues, ", ")
	}
	return &Error{ErrorTypeNotSupported, field.String(), value, detail}
}

// Forbidden returns a *Error indicating "forbidden".  This is used to
// report valid (as per formatting rules) values which would be accepted under
// some conditions, but which are not permitted by current conditions (e.g.
// security policy).
func Forbidden(field *Path, detail string) *Error {
	return &Error{ErrorTypeForbidden, field.String(), "", detail}
}

// TooLong returns a *Error indicating "too long".  This is used to
// report that the given value is too long.  This is similar to
// Invalid, but the returned error will not include the too-long
// value.
func TooLong(field *Path, value interface{}, maxLength int) *Error {
	return &Error{ErrorTypeTooLong, field.String(), value, fmt.Sprintf("must have at most %d bytes", maxLength)}
}

// TooMany returns a *Error indicating "too many". This is used to
// report that a given list has too many items. This is similar to TooLong,
// but the returned error indicates quantity instead of length.
func TooMany(field *Path, actualQuantity, maxQuantity int) *Error {
	return &Error{
		ErrorTypeTooMany,
		field.String(),
		actualQuantity,
		fmt.Sprintf("must have at most %d items", maxQuantity),
	}
}

// InternalError returns a *Error indicating "internal error".  This is used
// to signal that an error was found that was not directly related to user
// input.  The err argument must be non-nil.
func InternalError(field *Path, err error) *Error {
	return &Error{ErrorTypeInternal, field.String(), nil, err.Error()}
}

// ErrorList holds a set of Errors.  It is plausible that we might one day have
// non-field errors in this same umbrella package, but for now we don't, so
// we can keep it simple and leave ErrorList here.
type ErrorList []*Error

// NewErrorTypeMatcher returns an errors.Matcher that returns true
// if the provided error is a Error and has the provided ErrorType.
func NewErrorTypeMatcher(t ErrorType) utilerrors.Matcher {
	return func(err error) bool {
		if e, ok := err.(*Error); ok {
			return e.Type == t
		}
		return false
	}
}

// ToAggregate converts the ErrorList into an errors.Aggregate.
func (list ErrorList) ToAggregate() utilerrors.Aggregate {
	errs := make([]error, 0, len(list))
	errorMsgs := sets.NewString()
	for _, err := range list {
		msg := fmt.Sprintf("%v", err)
		if errorMsgs.Has(msg) {
			continue
		}
		errorMsgs.Insert(msg)
		errs = append(errs, err)
	}
	return utilerrors.NewAggregate(errs)
}

func fromAggregate(agg utilerrors.Aggregate) ErrorList {
	errs := agg.Errors()
	list := make(ErrorList, len(errs))
	for i := range errs {
		list[i] = errs[i].(*Error)
	}
	return list
}

// Filter removes items from the ErrorList that match the provided fns.
func (list ErrorList) Filter(fns ...utilerrors.Matcher) ErrorList {
	err := utilerrors.FilterOut(list.ToAggregate(), fns...)
	if err == nil {
		return nil
	}
	// FilterOut takes an Aggregate and returns an Aggregate
	return fromAggregate(err.(utilerrors.Aggregate))
}
