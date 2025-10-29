// 提供增强型错误处理功能，支持错误消息包装、堆栈跟踪记录和错误码关联
//
// 该包在标准库 error 接口的基础上扩展了以下核心能力：
// 1. 自动记录错误发生时的堆栈跟踪，便于问题定位
// 2. 支持多层错误包装（嵌套错误），保留完整的错误链
// 3. 允许为错误关联业务错误码，满足业务场景下的错误分类需求
// 4. 提供丰富的格式化输出（如 %+v 展示完整堆栈），提升调试效率
//
// 核心概念：
// - 基础错误（fundamental）：最底层错误，包含消息和初始堆栈
// - 错误包装（withStack/withMessage）：对原始错误添加堆栈或消息注解
// - 带码错误（withCode）：关联业务错误码的特殊错误类型，支持错误链传递
// - 错误根因（Cause）：通过 Cause() 方法可获取错误链的最原始错误
//
// 核心流程概览：
// 1. 错误创建：通过 New/Errorf 创建基础错误，自动记录堆栈
// 2. 错误包装：通过 Wrap/WithStack 等方法为错误添加额外上下文（消息/堆栈）
// 3. 错误码关联：通过 WithCode/WrapC 为错误绑定业务码，支持链式传递
// 4. 错误解析：通过 Cause() 获取根错误，通过格式化输出（%+v）查看完整信息
//
// 使用示例：
//
//  1. 创建基础错误：
//     err := errors.New("文件不存在")
//
//  2. 包装错误并添加消息：
//     err = errors.Wrap(err, "读取配置失败")
//
//  3. 关联业务错误码：
//     err = errors.WrapC(err, 1001, "配置加载异常")
//
//  4. 打印完整错误信息（含堆栈）：
//     fmt.Printf("错误详情: %+v\n", err)
//
//  5. 获取原始错误：
//     rootErr := errors.Cause(err)
package errors

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// fundamental 基础错误类型，包含错误消息和堆栈跟踪，无调用者信息
type fundamental struct {
	msg string
	*stack
}

// New 返回一个包含指定消息的错误，同时记录调用时的堆栈跟踪
// 核心流程：
// 1. 接收字符串消息作为参数
// 2. 调用 callers() 获取当前堆栈跟踪
// 3. 实例化 fundamental 结构体并返回其指针
func New(message string) error {
	return &fundamental{
		msg:   message,
		stack: callers(),
	}
}

// Error 实现 error 接口，返回错误消息
func (f *fundamental) Error() string { return f.msg }

// Format 实现 fmt.Formatter 接口，支持错误的格式化输出
// 核心流程：
// 1. 根据格式动词（%s/%v/%+v/%q）判断输出方式
// 2. %+v 时额外打印堆栈跟踪
// 3. %q 时将消息转为带双引号的转义字符串
// 4. %s 打印自定义错误信息
func (f *fundamental) Format(st fmt.State, verb rune) {
	switch verb {
	case 'v':
		if st.Flag('+') {
			io.WriteString(st, f.msg)
			f.stack.Format(st, verb)
			return
		}
		fallthrough
	case 's':
		io.WriteString(st, f.msg)
	case 'q':
		fmt.Fprintf(st, "%q", f.msg)
	}
}

// Errorf 根据格式说明符格式化错误消息，返回满足 error 接口的值，同时记录调用时的堆栈跟踪
// 核心流程：
// 1. 使用 fmt.Sprintf 格式化输入的格式字符串和参数
// 2. 调用 callers() 获取当前堆栈跟踪
// 3. 实例化 fundamental 结构体并返回其指针
func Errorf(format string, args ...interface{}) error {
	return &fundamental{
		msg:   fmt.Sprintf(format, args...),
		stack: callers(),
	}
}

// withStack 包含原始错误和堆栈跟踪的错误包装器
// 嵌入了一个 error 类型的字段（匿名嵌入）。在 Go 语言中，当一个结构体嵌入了另一个类型时，该结构体将 “继承” 被嵌入类型的所有方法。
type withStack struct {
	error
	*stack
}

// WithStack 为错误添加调用时的堆栈跟踪注解，若 err 为 nil 则返回 nil
// 核心流程：
// 1. 检查错误是否为 nil，是则返回 nil
// 2. 若错误是 withCode 类型，包装为新的 withCode 并添加当前堆栈
// 3. 否则包装为 withStack 类型并添加当前堆栈
func WithStack(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*withCode); ok {
		return &withCode{
			err:   e.err,
			code:  e.code,
			cause: err,
			stack: callers(),
		}
	}
	return &withStack{
		err,
		callers(),
	}
}

// Cause 返回原始错误
func (w *withStack) Cause() error {
	return w.error
}

// Format 实现 fmt.Formatter 接口，支持带堆栈的错误格式化输出
// 核心流程：
// 1. 根据格式动词（%s/%v/%+v/%q）判断输出方式
// 2. %+v 时先递归打印根错误的详细信息，再打印当前堆栈
// 3. 其他格式直接使用内部错误的对应格式输出
func (w *withStack) Format(st fmt.State, verb rune) {
	switch verb {
	case 'v':
		if st.Flag('+') {
			fmt.Fprintf(st, "%+v", w.Cause())
			w.stack.Format(st, verb)
			return
		}
		fallthrough
	case 's':
		io.WriteString(st, w.Error())
	case 'q':
		fmt.Fprintf(st, "%q", w.Error())
	}
}

// Wrap 为错误添加消息和堆栈跟踪注解，若 err 为 nil 则返回 nil
// 核心流程：
// 1. 检查原始错误是否为 nil，是则返回 nil
// 2. 若错误是 withCode 类型，包装为新的 withCode 并添加消息和当前堆栈
// 3. 否则先通过 withMessage 添加消息
// 4.  withStack 添加当前堆栈
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*withCode); ok {
		return &withCode{
			err:   fmt.Errorf("%s", message),
			code:  e.code,
			cause: err,
			stack: callers(),
		}
	}
	err = &withMessage{
		cause: err,
		msg:   message,
	}
	return &withStack{
		err,
		callers(),
	}
}

// Wrapf 为错误添加格式化消息和堆栈跟踪注解，若 err 为 nil 则返回 nil
// 核心流程：
// 1. 检查错误是否为 nil，是则返回 nil
// 2. 若错误是 withCode 类型，包装为新的 withCode 并添加格式化消息和当前堆栈
// 3. 否则先通过 withMessagef 添加格式化消息，再通过 withStack 添加当前堆栈
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*withCode); ok {
		return &withCode{
			err:   fmt.Errorf(format, args...),
			code:  e.code,
			cause: err,
			stack: callers(),
		}
	}
	err = &withMessage{
		cause: err,
		msg:   fmt.Sprintf(format, args...),
	}
	return &withStack{
		err,
		callers(),
	}

}

// withMessage 仅包含消息和根因的错误包装器
type withMessage struct {
	cause error
	msg   string
}

// WithMessage 为错误添加消息注解，若 err 为 nil 则返回 nil
// 核心流程：
// 1. 检查错误是否为 nil，是则返回 nil
// 2. 实例化 withMessage 结构体，包装原始错误和消息并返回
func WithMessage(err error, message string) error {
	if err == nil {
		return nil
	}
	return &withMessage{
		cause: err,
		msg:   message,
	}
}

// WithMessagef 为错误添加格式化消息注解，若 err 为 nil 则返回 nil
// 核心流程：
// 1. 检查错误是否为 nil，是则返回 nil
// 2. 使用 fmt.Sprintf 格式化消息
// 3. 实例化 withMessage 结构体，包装原始错误和格式化消息并返回
func WithMessagef(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return &withMessage{
		cause: err,
		msg:   fmt.Sprintf(format, args...),
	}
}

// Error 实现 error 接口，返回错误消息
func (w *withMessage) Error() string {
	return w.msg
}

// Cause 返回原始错误
func (w *withMessage) Cause() error {
	return w.cause
}

// Format 实现 fmt.Formatter 接口，支持带消息的错误格式化输出
// 核心流程：
// 1. 根据格式动词（%s/%v/%+v/%q）判断输出方式
// 2. %+v 时先递归打印根错误的详细信息，再打印当前消息
// 3. 其他格式直接输出当前消息
func (w *withMessage) Format(st fmt.State, verb rune) {
	switch verb {
	case 'v':
		if st.Flag('+') {
			fmt.Fprintf(st, "%+v\n", w.Cause())
			io.WriteString(st, w.msg)
			return
		}
		fallthrough
	case 's', 'q':
		io.WriteString(st, w.Error())
	}
}

type withCode struct {
	err    error //保留新的错误
	code   int   //业务码
	cause  error //上一级错误
	*stack       //添加新的堆栈记录
}

func (e *withCode) Code() int {
	return e.code
}

func (w *withCode) Format(st fmt.State, verb rune) {
	switch verb {
	case 'v':
		// 处理 %#v：JSON 格式输出（优化版）
		if st.Flag('#') {
			// 对当前错误（无论层级）调用 toJSON()
			jsonData := w.toJSON()
			// 序列化并写入
			b, err := json.MarshalIndent(jsonData, "", "    ")
			if err != nil {
				st.Write([]byte(fmt.Sprintf("格式化错误: %v", err)))
				return
			}
			st.Write(b)
			return
		}
		if st.Flag('+') {
			// 递归打印上一级错误（添加缩进区分层级）
			if w.cause != nil {
				// 打印上一级错误时，通过前缀 "  ↳" 标识层级
				fmt.Fprintf(st, "  ↳ %+v\n", w.cause)
			}
			// 打印当前层级的信息（错误码 + 消息）
			fmt.Fprintf(st, "[code: %d][http:%d] %s", w.code, ParseCoderByCode(w.code).HTTPStatus(), w.err.Error())
			// 打印当前层级的堆栈
			if w.stack != nil {
				w.stack.Format(st, verb)
			}
			return
		}
		if st.Flag('-') {
			// 打印当前层级的信息（错误码 + 消息）
			fmt.Fprintf(st, "[code: %d] %s", w.code, w.err.Error())
			// 打印当前层级的堆栈
			if w.stack != nil {
				w.stack.Format(st, verb)
			}
			return
		}
		fmt.Fprintf(st, "[code: %d] %s", w.code, w.err.Error())
	case 's', 'q':
		io.WriteString(st, w.Error())
	}
}

// 用于创建全新的带码错误（根错误），没有上一级错误需要关联
// WithCode 创建一个带错误码的新错误
// 核心流程：
// 1. 使用 fmt.Sprintf 格式化消息
// 2. 调用 callers() 获取当前堆栈跟踪
// 3. 实例化 withCode 结构体，包含错误码、消息和堆栈并返回
func WithCode(code int, format string, args ...interface{}) error {
	return &withCode{
		err:   fmt.Errorf(format, args...),
		code:  code,
		stack: callers(),
	}
}

// WrapC 为现有错误添加错误码和新消息注解
// 核心流程：
// 1. 检查错误是否为 nil，是则返回 nil
// 2. 使用 fmt.Sprintf 格式化新消息
// 3. 调用 callers() 获取当前堆栈跟踪
// 4. 实例化 withCode 结构体，包含错误码、新消息、原始错误和堆栈并返回
func WrapC(err error, code int, format string, args ...interface{}) error {
	// 生成错误消息
	msg := fmt.Errorf(format, args...)
	// 无论原始err是否为nil，都创建带码错误
	errWithCode := &withCode{
		err:   msg,
		code:  code,
		stack: callers(),
	}
	// 如果有原始错误，记录cause
	if err != nil {
		errWithCode.cause = err
	}
	return errWithCode
}

// Error 实现 error 接口，返回外部安全的错误消息
func (w *withCode) Error() string {
	return fmt.Sprintf("%v", w)
}

// Cause 返回错误的根因
func (w *withCode) Cause() error {
	return w.cause
}

// Cause 返回错误的底层根因（如果可能）
// 核心流程：
// 1. 定义内部 causer 接口（包含 Cause() 方法）
// 2. 循环解包错误：若错误实现 causer 接口，则递归获取其 Cause()
// 3. 直到获取到不实现 causer 接口的错误，返回该错误作为根因
func Cause(err error) error {
	type causer interface {
		Cause() error
	}
	for err != nil {
		cause, ok := err.(causer)
		if !ok {
			break
		}
		if cause.Cause() == nil {
			break
		}
		err = cause.Cause()
	}
	return err
}

// 用于 JSON 序列化的结构
// withCodeJSON 用于 JSON 序列化的结构（优化版）
type withCodeJSON struct {
	Code    int         `json:"code"`            // 错误码（必显）
	Message string      `json:"message"`         // 错误消息（必显）
	Cause   interface{} `json:"cause,omitempty"` // 嵌套错误（空值时省略）
	Stack   []string    `json:"stack,omitempty"` // 堆栈数组（空值时省略）
	Http    int         `json:"httpStatus"`      //http状态
	Ref     string      `json:"reference,omitempty"`
}

// toJSON 递归转换为 withCodeJSON 结构，用于嵌套序列化
func (w *withCode) toJSON() withCodeJSON {
	var causeJSON interface{}
	if w.cause != nil {
		if c, ok := w.cause.(*withCode); ok {
			// 递归处理嵌套的 withCode 错误
			causeJSON = c.toJSON()
		} else {
			// 非 withCode 错误，仅保留消息
			causeJSON = map[string]string{"message": w.cause.Error()}
		}
	}
	return withCodeJSON{
		Code:    w.code,
		Message: w.err.Error(),
		Http:    ParseCoderByCode(w.code).HTTPStatus(),
		Ref:     ParseCoderByCode(w.code).Reference(),
		Cause:   causeJSON,
		Stack:   w.stack.ToSlice(),
	}
}

func IsWithCode(err error) bool {
	_, ok := err.(*withCode)
	return ok
}

// GetCode 从 withCode 错误中提取业务码（包内直接访问未导出字段 code）
// 若不是 withCode 类型，返回 -1（无效码）
func GetCode(err error) int {
	if e, ok := err.(*withCode); ok {
		return e.code
	}
	return -1
}

// GetMessage 从 withCode 错误中提取错误消息（包内直接访问未导出字段 err）
// 若不是 withCode 类型，返回原始错误消息
func GetMessage(err error) string {
	if e, ok := err.(*withCode); ok {
		return e.err.Error()
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

// GetHTTPStatus 从 withCode 错误中获取映射的 HTTP 状态码（整合 ParseCoderByCode）
// 若不是 withCode 类型，返回默认 500
func GetHTTPStatus(err error) int {
	if e, ok := err.(*withCode); ok {
		return ParseCoderByCode(e.code).HTTPStatus()
	}
	return http.StatusInternalServerError
}

// HTTPStatusMessageFunc 符合框架要求的签名：仅返回错误消息字符串
// 功能：从错误中提取用户可见的消息，依赖你的errors库处理
func HTTPStatusMessageFunc(e error, c *gin.Context) string {
	// 1. 处理空错误（返回默认成功消息）
	if e == nil {
		return "操作成功"
	}

	// 2. 调用你库的GetMessage直接获取错误消息
	// - 若e是withCode类型：返回withCode.err的消息（如"用户名格式错误"）
	// - 若不是：返回e.Error()（普通错误消息）
	errMsg := GetMessage(e)

	// 3. 优化：对未注册的错误码（返回_unknownCode）添加提示（可选）
	if IsWithCode(e) { // 判断是否为带码错误
		code := GetCode(e)
		// 检查该错误码是否注册（_unknownCode的code是1）
		if ParseCoderByCode(code).Code() == 1 {
			errMsg = fmt.Sprintf("[错误码未注册:%d] %s", code, errMsg)
		}
	}

	return errMsg
}

// isUnrecoverableError 判断是否为不可恢复的错误（与 shouldRetry 保持一致）
func isUnrecoverableError(errStr string) bool {
	unrecoverableErrors := []string{
		"Duplicate entry", "1062", "23000", "duplicate key value", "23505",
		"用户已存在", "UserAlreadyExist",
		"UNMARSHAL_ERROR", "invalid json", "unknown operation", "poison message",
		"definer", "DEFINER", "1449", "permission denied",
		"does not exist", "not found", "record not found", "ErrRecordNotFound",
		"constraint", "foreign key", "1451", "1452", "syntax error",
		"Data too long for column", "1406",
		"ErrInvalidData", "ErrInvalidTransaction", "ErrNotImplemented", "ErrMissingWhereClause", "ErrPrimaryKeyRequired", "ErrModelValueRequired", "ErrUnsupportedRelation", "ErrRegistered", "ErrInvalidField", "ErrEmptySlice", "ErrDryRunModeUnsupported",
		"invalid format", "validation failed",
	}
	for _, unrecoverableErr := range unrecoverableErrors {
		if strings.Contains(errStr, unrecoverableErr) {
			return true
		}
	}
	return false
}

// isRecoverableError 判断是否为可恢复的错误（与 shouldRetry 保持一致）
func isRecoverableError(errStr string) bool {
	recoverableErrors := []string{
		"timeout", "deadline exceeded", "connection refused", "network error",
		"connection reset", "broken pipe", "no route to host",
		"database is closed", "deadlock", "1213", "40001",
		"temporary", "busy", "lock", "try again",
		"resource temporarily unavailable", "too many connections",
		"ErrInvalidTransaction", "ErrDryRunModeUnsupported",
	}
	for _, recoverableErr := range recoverableErrors {
		if strings.Contains(errStr, recoverableErr) {
			return true
		}
	}
	return false
}

// Temporary 实现标准临时错误接口，便于统一重试和监控判断
// 结合 shouldRetry 逻辑，优先字符串判定，其次业务码区间
func (e *withCode) Temporary() bool {
	errStr := ""
	if e != nil && e.err != nil {
		errStr = e.err.Error()
	}
	if isUnrecoverableError(errStr) {
		return false
	}
	if isRecoverableError(errStr) {
		return true
	}
	// 兜底：5xx 业务码为临时错误
	return e.code >= 500 && e.code < 600
}
