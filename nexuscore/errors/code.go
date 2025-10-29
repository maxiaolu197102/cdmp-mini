/*
package errors
code.go
错误码管理包：提供标准化的错误码定义、注册、解析及查询功能。

设计思路：
- 结构体名称首字母小写（如 defaultCoder）：表示类型为包内细节，外部无需依赖具体实现。
- 结构体字段首字母大写（如 defaultCoder.C）：允许外部通过接口访问字段值，平衡封装与可用性。

HTTP 状态码约定：
- 200：请求成功执行
- 400：客户端请求错误（如参数无效）
- 401：认证失败（如令牌无效）
- 403：授权失败（如权限不足）
- 404：资源不存在（如 URL 或 RESTful 资源未找到）
- 500：服务器内部错误
*/
package errors

import (
	"fmt"
	"sort"
	"sync"
)

// ------------------------------ 全局变量与常量 ------------------------------
const (
	// _iseMsg 内部服务器错误的默认提示信息,用于defaultCoder的Ext
	_iseMsg = `发生了内部服务器错误,请参阅http://github.com/maxiaolu1981/errors/README.md`
)

var (
	// _codes 存储所有注册的错误码（key=错误编码，value=Coder实例）
	_codes = map[int]Coder{}
	// _codeMutex 互斥锁，保证并发环境下对 _codes 的操作安全
	_codeMutex = &sync.Mutex{}
	// _unknownCode[1,500,_iseMsg,"http://github.com/maxiaolu1981/errors/README.md"] 提供未知错误的默认编码（兜底方案）
	_unknownCode = defaultCoder{
		C:    1,
		Http: 500,
		Ext:  _iseMsg,
		Ref:  `http://github.com/maxiaolu1981/errors/README.md`,
	}
)

// ------------------------------ 核心接口定义

// Coder 定义错误码的标准行为，是错误码对外暴露的核心接口
type Coder interface {
	// 返回错误的唯一业务编码（如10001）
	Code() int
	// 返回错误对应的HTTP状态码（如400、500）
	HTTPStatus() int
	// 返回用户可见的错误描述（如"参数无效"）
	String() string
	// 返回错误的参考文档地址
	Reference() string
}

// ------------------------------ -----------------------接口实现结构体

// defaultCoder[C Http Ext Ref] 是 Coder 接口的默认实现，封装错误码的核心信息
type defaultCoder struct {
	C    int
	Http int
	Ext  string
	Ref  string
}

// ------------------------------ defaultCoder 方法实现
//------------------------------

// Code 返回业务错误编码
func (d defaultCoder) Code() int {
	return d.C
}

// HTTPStatus 返回对应的HTTP状态码，默认返回500
func (d defaultCoder) HTTPStatus() int {
	if d.Http == 0 {
		return 500
	}
	return d.Http
}

// String 返回用户可见的错误描述
func (d defaultCoder) String() string {
	return d.Ext
}

// Reference 返回错误的参考文档地址
func (d defaultCoder) Reference() string {
	return d.Ref
}

// ------------------------------ 错误码注册函数
//------------------------------

// Register 注册自定义错误码，若编码已存在则覆盖旧值
// 禁止注册编码=0（预留为未知错误标识），否则会panic
func Register(coder Coder) {
	if coder.Code() == 0 {
		panic(_iseMsg)
	}
	withLock(func() {
		_codes[coder.Code()] = coder
	})
}

// MustRegister 注册自定义错误码，若编码已存在则panic（严格模式）
// 禁止注册编码=0（预留为未知错误标识），否则会panic
func MustRegister(coder Coder) {
	if coder.Code() == 0 {
		panic(_iseMsg)
	}
	withLock(func() {

		if _, ok := _codes[coder.Code()]; ok {
			panic(fmt.Sprintf("%d编码已经存在,不允许重复录入", coder.Code()))
		}
		_codes[coder.Code()] = coder
	})
}

// ------------------------------ 错误码解析与检查函数 ------------------------------

// ParseCoderByErr 将输入的错误解析为对应的Coder实例
// 若err为nil，返回nil；若err不是*withCode类型或编码未注册，返回_unknownCode
func ParseCoderByErr(err error) Coder {
	if err == nil {
		return nil
	}

	if v, ok := err.(*withCode); ok {
		if coder, ok := _codes[v.code]; ok {
			return coder
		}
	}

	return _unknownCode
}

func ParseCoderByCode(code int) Coder {
	if coder, ok := _codes[code]; ok {
		return coder
	}
	return _unknownCode
}

// IsCode 检查错误链中是否包含指定的错误编码
// 支持嵌套错误（通过cause追溯底层错误）
func IsCode(err error, code int) bool {
	e, ok := err.(*withCode)
	if ok {
		if e.code == code {
			return true
		}
		if e.cause != nil {
			return IsCode(e.cause, code)
		}
	}
	return false
}

// ------------------------------ 错误码查询与辅助函数 ------------------------------

// 辅助函数withLock 减少重复代码
func withLock(fn func()) {
	_codeMutex.Lock()
	defer _codeMutex.Unlock()
	fn()
}

// ------------------------------ 包初始化 ------------------------------

// init 初始化包时注册默认的未知错误码
func init() {
	Register(_unknownCode)
}

// ------------------------------ 测试用 ------------------------------
func NewDefaultCoder(code int, http int, msg string, ref string) defaultCoder {
	return defaultCoder{
		C:    code,
		Http: http,
		Ext:  msg,
		Ref:  ref,
	}
}

// 错误码分组映射表（前缀 -> 分组名称）
var codeGroups = map[int]string{
	1000: "通用基本错误（1000xx）",
	1001: "通用数据库错误（1001xx）",
	1002: "通用授权认证错误（1002xx）",
	1003: "通用加解码错误（1003xx）",
	1100: "iam-apiserver 用户模块（1100xx）",
	1101: "iam-apiserver 密钥模块（1101xx）",
	1102: "iam-apiserver 策略模块（1102xx）",
}

// 错误码常量名映射表（需要手动维护与错误码的对应关系）
// 注意：新增错误码时需在此处补充映射
var codeConstNames = map[int]string{
	// 通用基本错误
	100001: "ErrSuccess",
	100002: "ErrUnknown",
	100003: "ErrBind",
	100004: "ErrValidation",
	100005: "ErrPageNotFound",
	100006: "ErrMethodNotAllowed",
	100007: "ErrUnsupportedMediaType",
	100008: "ErrContextCanceled",

	// 通用数据库错误
	100101: "ErrDatabase",
	100102: "ErrDatabaseTimeout",
	100103: "ErrDatabaseDeadlock",

	// 通用授权认证错误
	100201: "ErrEncrypt",
	100202: "ErrSignatureInvalid",
	100203: "ErrExpired",
	100204: "ErrInvalidAuthHeader",
	100205: "ErrMissingHeader",
	100206: "ErrPasswordIncorrect",
	100207: "ErrPermissionDenied",
	100208: "ErrTokenInvalid",
	100209: "ErrBase64DecodeFail",
	100210: "ErrInvalidBasicPayload",
	100211: "ErrRespCodeRTRevoked",
	100212: "ErrTokenMismatch",

	// 通用加解码错误
	100301: "ErrEncodingFailed",
	100302: "ErrDecodingFailed",
	100303: "ErrInvalidJSON",
	100304: "ErrEncodingJSON",
	100305: "ErrDecodingJSON",
	100306: "ErrInvalidYaml",
	100307: "ErrEncodingYaml",
	100308: "ErrDecodingYaml",

	//kafka（1004xx）：服务10 + 模块04 + 序号 404
	100401: "ErrKafkaSendFailed",
	100402: "ErrRedisFailed",

	// iam-apiserver 用户模块
	110001: "ErrUserNotFound",
	110002: "ErrUserAlreadyExist",
	110003: "ErrUnauthorized",
	110004: "ErrInvalidParameter",
	110005: "ErrInternal",
	110006: "ErrResourceConflict",
	110007: "ErrInternalServer",
	110008: "ErrNotAdministrator",
	110009: "ErrUserDisabled",

	// iam-apiserver 密钥模块
	110101: "ErrReachMaxCount",
	110102: "ErrSecretNotFound",

	// iam-apiserver 策略模块
	110201: "ErrPolicyNotFound",
}



// ListAllCodes 按归类分组打印所有已注册的错误码信息，包含编码、常量名、HTTP状态、描述
func ListAllCodes() {
	withLock(func() {
		if len(_codes) == 0 {
			fmt.Println("当前无注册的错误码")
			return
		}

		// 1. 按分组归类错误码
		groupedCodes := make(map[int][]int) // 分组前缀 -> 错误码列表
		for code := range _codes {
			prefix := code / 100 // 提取前4位作为分组前缀（如100001 -> 1000）
			groupedCodes[prefix] = append(groupedCodes[prefix], code)
		}

		// 2. 定义分组打印顺序（按前缀升序）
		var groupOrder []int
		for prefix := range groupedCodes {
			groupOrder = append(groupOrder, prefix)
		}
		sort.Ints(groupOrder)

		// 3. 遍历分组打印
		for _, prefix := range groupOrder {
			codes := groupedCodes[prefix]
			sort.Ints(codes) // 组内按错误码升序排列

			// 打印分组标题
			fmt.Printf("\n===== %s =====\n", codeGroups[prefix])

			// 打印表头
			fmt.Printf(
				"%-8s %-20s %-10s %s\n",
				"错误码", "常量名", "HTTP状态", "描述信息",
			)
			fmt.Println("----------------------------------------------------------------------")

			// 打印组内错误码
			for _, code := range codes {
				coder := _codes[code]
				constName := codeConstNames[code]
				if constName == "" {
					constName = "未知常量名" // 未在映射表中定义时的默认值
				}

				fmt.Printf(
					"%-8d %-20s %-10d %s\n",
					code,
					constName,
					coder.HTTPStatus(),
					truncateString(coder.String(), 50), // 描述最多显示50字符
				)
			}
		}
	})
}

// 辅助函数：截断过长字符串
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

