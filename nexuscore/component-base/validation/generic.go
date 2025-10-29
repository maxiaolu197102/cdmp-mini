/*
包摘要
该包提供了一系列数据验证工具函数，用于校验各类输入数据的合法性，包括用户名、标签值、DNS格式、端口号、IP地址、百分比格式及密码等。验证逻辑基于正则表达式和规则判断，返回具体的错误信息，适用于需要严格数据校验的场景。

核心功能
1. 名称和标签验证
合格名称验证：支持带DNS前缀的名称格式
标签值验证：Kubernetes风格的标签验证
DNS标签验证：DNS-1123标准标签验证

2. 网络相关验证
IP地址验证：IPv4和IPv6地址验证
端口号验证：1-65535范围验证
DNS子域名验证：完整的域名验证

3. 格式验证
百分比格式验证：数字+%格式验证
密码强度验证：复杂度要求验证

4. 辅助功能
错误信息生成：标准化的错误提示生成
范围验证：数值范围验证
函数使用方法
1. 基本名称验证
go
// 合格名称验证（支持前缀）
errs := validation.IsQualifiedName("example.com/my-app")
if len(errs) > 0 {
    fmt.Println("验证失败:", strings.Join(errs, "; "))
}

// 标签值验证
errs := validation.IsValidLabelValue("my_label-value123")
if len(errs) > 0 {
    fmt.Println("标签值无效:", errs)
}
2. DNS相关验证
go
// DNS标签验证
errs := validation.IsDNS1123Label("my-service")
if len(errs) > 0 {
    fmt.Println("DNS标签无效:", errs)
}

// DNS子域名验证
errs := validation.IsDNS1123Subdomain("test.example.com")
if len(errs) > 0 {
    fmt.Println("子域名无效:", errs)
}
3. 网络配置验证
go
// 端口号验证
errs := validation.IsValidPortNum(8080)
if len(errs) > 0 {
    fmt.Println("端口号无效:", errs)
}

// IP地址验证
errs := validation.IsValidIP("192.168.1.1")
if len(errs) > 0 {
    fmt.Println("IP地址无效:", errs)
}

// 带字段路径的IP验证（返回field.ErrorList）
fldPath := field.NewPath("network", "ip")
errs := validation.IsValidIPv4Address(fldPath, "192.168.1.1")
if len(errs) > 0 {
    fmt.Println("IPv4地址无效:", errs.ToAggregate())
}
4. 格式验证
go
// 百分比验证
errs := validation.IsValidPercent("50%")
if len(errs) > 0 {
    fmt.Println("百分比格式无效:", errs)
}

// 密码强度验证
err := validation.IsValidPassword("StrongPass123!")
if err != nil {
    fmt.Println("密码不符合要求:", err)
}

// 数值范围验证
errs := validation.IsInRange(25, 1, 100)
if len(errs) > 0 {
    fmt.Println("数值超出范围:", errs)
}
5. 错误信息生成
go
// 生成标准错误信息
maxLenErr := validation.MaxLenError(63)
fmt.Println(maxLenErr) // "长度不能超过63个字符"

rangeErr := validation.InclusiveRangeError(1, 100)
fmt.Println(rangeErr) // "必须在1到100之间（包含边界）"

regexErr := validation.RegexError(
    "必须由字母数字组成",
    "[A-Za-z0-9]+",
    "example",
    "test123"
)
fmt.Println(regexErr)
使用的业务场景
1. Kubernetes资源验证
go
func ValidatePodSpec(spec *PodSpec) field.ErrorList {
    var allErrors field.ErrorList
    fldPath := field.NewPath("spec")

    // 验证容器名称
    for i, container := range spec.Containers {
        containerPath := fldPath.Child("containers").Index(i).Child("name")
        if errs := validation.IsQualifiedName(container.Name); len(errs) > 0 {
            for _, err := range errs {
                allErrors = append(allErrors, field.Invalid(containerPath, container.Name, err))
            }
        }
    }

    return allErrors
}
2. API请求参数验证
go
func ValidateCreateRequest(req *CreateRequest) error {
    var errors []string

    // 验证名称格式
    errors = append(errors, validation.IsQualifiedName(req.Name)...)

    // 验证标签
    for key, value := range req.Labels {
        if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
            errors = append(errors, fmt.Sprintf("标签%s的值无效: %s", key, strings.Join(errs, ", ")))
        }
    }

    // 验证端口
    if errs := validation.IsValidPortNum(req.Port); len(errs) > 0 {
        errors = append(errors, errs...)
    }

    if len(errors) > 0 {
        return fmt.Errorf("请求参数验证失败: %s", strings.Join(errors, "; "))
    }

    return nil
}
3. 配置文件中验证
go
func ValidateAppConfig(config *Config) field.ErrorList {
    var allErrors field.ErrorList
    basePath := field.NewPath("config")

    // 验证域名配置
    if errs := validation.IsDNS1123Subdomain(config.Domain); len(errs) > 0 {
        for _, err := range errs {
            allErrors = append(allErrors, field.Invalid(
                basePath.Child("domain"),
                config.Domain,
                err
            ))
        }
    }

    // 验证端口范围
    if errs := validation.IsInRange(config.Timeout, 1, 300); len(errs) > 0 {
        for _, err := range errs {
            allErrors = append(allErrors, field.Invalid(
                basePath.Child("timeout"),
                config.Timeout,
                err
            ))
        }
    }

    return allErrors
}
4. 用户注册验证
go
func ValidateUserRegistration(user *User) []string {
    var errors []string

    // 验证用户名
    errors = append(errors, validation.IsQualifiedName(user.Username)...)

    // 验证密码强度
    if err := validation.IsValidPassword(user.Password); err != nil {
        errors = append(errors, "密码强度不足: "+err.Error())
    }

    // 验证邮箱格式（使用DNS验证）
    if parts := strings.Split(user.Email, "@"); len(parts) == 2 {
        if errs := validation.IsDNS1123Subdomain(parts[1]); len(errs) > 0 {
            errors = append(errors, "邮箱域名无效: "+strings.Join(errs, ", "))
        }
    }

    return errors
}
5. 网络配置验证
go
func ValidateNetworkConfig(netConfig *NetworkConfig) field.ErrorList {
    var allErrors field.ErrorList
    fldPath := field.NewPath("network")

    // 验证IP地址
    if netConfig.IP != "" {
        ipErrors := validation.IsValidIPv4Address(fldPath.Child("ip"), netConfig.IP)
        allErrors = append(allErrors, ipErrors...)
    }

    // 验证子网掩码格式
    if netConfig.Subnet != "" {
        if errs := validation.IsValidIP(netConfig.Subnet); len(errs) > 0 {
            for _, err := range errs {
                allErrors = append(allErrors, field.Invalid(
                    fldPath.Child("subnet"),
                    netConfig.Subnet,
                    err
                ))
            }
        }
    }

    return allErrors
}
6. 监控配置验证
go
func ValidateMonitoringConfig(monConfig *MonitoringConfig) []string {
    var errors []string

    // 验证百分比格式的阈值
    if errs := validation.IsValidPercent(monConfig.CPUThreshold); len(errs) > 0 {
        errors = append(errors, "CPU阈值格式错误: "+strings.Join(errs, ", "))
    }

    if errs := validation.IsValidPercent(monConfig.MemoryThreshold); len(errs) > 0 {
        errors = append(errors, "内存阈值格式错误: "+strings.Join(errs, ", "))
    }

    return errors
}
优势特点
标准化验证：遵循Kubernetes和DNS标准

详细错误信息：提供具体的错误原因和建议

灵活的输出格式：支持字符串数组和field.ErrorList

全面的覆盖：覆盖常见的验证场景

易于集成：可以轻松集成到各种验证框架中

这个包特别适合需要严格数据验证的云原生应用、API服务和配置管理系统。
IsQualifiedName:验证字符串是否为合法的"合格名称"格式，支持可选DNS前缀 适用于用户名、资源名称等需要命名规范的场景
IsValidLabelValue:验证字符串是否为合法的标签值，允许为空或符合特定格式 适用于Kubernetes标签、配置标签等键值对场景
IsDNS1123Label:验证字符串是否符合DNS-1123标签格式（小写字母、数字、连字符） 适用于域名标签、主机名等DNS相关命名场景
IsDNS1123Subdomain:验证字符串是否符合DNS-1123子域名格式 适用于完整域名、子域名验证场景
IsValidPortNum:验证端口号是否合法（0-65535范围） 适用于网络服务端口配置验证场景
IsInRange:验证数值是否在指定范围内 适用于各种数值范围限制验证场景
IsValidIP:验证字符串是否为合法IP地址（IPv4或IPv6） 适用于IP地址配置验证场景
IsValidIPv4Address:验证字符串是否为合法IPv4地址 适用于需要严格IPv4地址验证的场景
IsValidIPv6Address:验证字符串是否为合法IPv6地址 适用于需要严格IPv6地址验证的场景
IsValidPercent:验证字符串是否为合法的百分比格式 适用于百分比配置、进度表示等场景
MaxLenError:生成"长度超限"的错误提示信息 适用于验证错误信息生成场景
RegexError:生成正则匹配失败的错误提示，包含示例 适用于正则验证失败时的友好错误提示场景
EmptyError:生成"不能为空"的错误提示 适用于必填字段验证场景
prefixEach:给错误信息列表中的每个信息添加前缀 适用于错误信息批量处理场景
InclusiveRangeError:生成"数值范围"的错误提示（包含边界值） 适用于范围验证错误提示场景
IsValidPassword:验证密码是否合法，检查长度、字符类型等复杂度要求 适用于用户密码强度验证场景

*/

// Package validation 提供各类数据合法性校验工具，用于验证用户名、密码、DNS格式等输入数据。

package validation

import (
	"fmt"
	"net"
	"regexp"
	"strings"
	"unicode"

	v1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/stringutil"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation/field"
)

// 用户名相关校验规则
const (
	qnameCharFmt     string = "[A-Za-z0-9]"                                               // 用户名允许的基础字符（字母、数字）
	qnameExtCharFmt  string = "[-A-Za-z0-9_.]"                                            // 用户名允许的扩展字符（含-、_、.）
	qualifiedNameFmt string = "(" + qnameCharFmt + qnameExtCharFmt + "*)?" + qnameCharFmt // 用户名完整格式
)

var (
	GlobalMaxTimeout     int64 = 60
	GlobalMaxOffset      int64 = 100
	GlobalMaxLimit       int64 = 10000
	GlobalMaxSelectorLen int   = 1024
)

const (
	qualifiedNameErrMsg    string = "必须由字母、数字、'-'、'_'或'.'组成，且首尾必须为字母或数字"
	qualifiedNameMaxLength int    = 63 // 用户名最大长度
)

// 用户名校验正则表达式（匹配qualifiedNameFmt）
var qualifiedNameRegexp = regexp.MustCompile("^" + qualifiedNameFmt + "$")

// IsQualifiedName 验证字符串是否为合法的"合格名称"（支持可选DNS前缀，如"example.com/MyName"）
// 返回错误信息列表（空列表表示验证通过）

// 标签值相关校验规则
const labelValueFmt string = "(" + qualifiedNameFmt + ")?" // 标签值格式（允许为空或符合用户名规则）

const labelValueErrMsg string = "标签值必须为空或由字母、数字、'-'、'_'或'.'组成，且首尾必须为字母或数字"

// LabelValueMaxLength 标签值最大长度
const LabelValueMaxLength int = 63

// 标签值校验正则表达式
var labelValueRegexp = regexp.MustCompile("^" + labelValueFmt + "$")

// IsValidLabelValue 验证字符串是否为合法的标签值
func IsValidLabelValue(value string) []string {
	var errs []string
	if len(value) > LabelValueMaxLength {
		errs = append(errs, MaxLenError(LabelValueMaxLength)) // 长度超限
	}
	if !labelValueRegexp.MatchString(value) {
		errs = append(errs, RegexError(labelValueErrMsg, labelValueFmt, "MyValue", "my_value", "12345"))
	}
	return errs
}

// DNS 标签（如域名中的一段）校验规则
const dns1123LabelFmt string = "[a-z0-9]([-a-z0-9]*[a-z0-9])?" // 小写字母、数字、-，首尾必须为字母/数字

const dns1123LabelErrMsg string = "DNS-1123标签必须由小写字母、数字或'-'组成，且首尾必须为字母或数字"

// DNS1123LabelMaxLength DNS标签最大长度
const DNS1123LabelMaxLength int = 63

// DNS标签校验正则表达式
var dns1123LabelRegexp = regexp.MustCompile("^" + dns1123LabelFmt + "$")

// IsDNS1123Label 验证字符串是否符合DNS-1123标签格式（如域名中的单个标签）
func IsDNS1123Label(value string) []string {
	var errs []string
	if len(value) > DNS1123LabelMaxLength {
		errs = append(errs, MaxLenError(DNS1123LabelMaxLength))
	}
	if !dns1123LabelRegexp.MatchString(value) {
		errs = append(errs, RegexError(dns1123LabelErrMsg, dns1123LabelFmt, "my-name", "123-abc"))
	}
	return errs
}

// DNS子域名校验规则
const dns1123SubdomainFmt string = dns1123LabelFmt + "(\\." + dns1123LabelFmt + ")*" // 多个DNS标签用.连接

const dns1123SubdomainErrorMsg string = "DNS-1123子域名必须由小写字母、数字、'-'或'.'组成，且首尾必须为字母或数字"

// DNS1123SubdomainMaxLength DNS子域名最大长度
const DNS1123SubdomainMaxLength int = 253

// DNS子域名校验正则表达式
var dns1123SubdomainRegexp = regexp.MustCompile("^" + dns1123SubdomainFmt + "$")

// IsDNS1123Subdomain 验证字符串是否符合DNS-1123子域名格式（如"example.com"）
func IsDNS1123Subdomain(value string) []string {
	var errs []string
	if len(value) > DNS1123SubdomainMaxLength {
		errs = append(errs, MaxLenError(DNS1123SubdomainMaxLength))
	}
	if !dns1123SubdomainRegexp.MatchString(value) {
		errs = append(errs, RegexError(dns1123SubdomainErrorMsg, dns1123SubdomainFmt, "example.com"))
	}
	return errs
}

// IsValidPortNum 验证端口号是否合法（1-65535之间）
func IsValidPortNum(port int) []string {
	if port == 0 || (1 <= port && port <= 65535) {
		return nil
	}
	return []string{InclusiveRangeError(1, 65535)}
}

// IsInRange 验证数值是否在[min, max]范围内
func IsInRange(value int, min int, max int) []string {
	if value >= min && value <= max {
		return nil
	}
	return []string{InclusiveRangeError(min, max)}
}

// IsValidIP 验证字符串是否为合法IP地址（IPv4或IPv6）
func IsValidIP(value string) []string {
	if net.ParseIP(value) == nil {
		return []string{"必须是有效的IP地址（如10.9.8.7）"}
	}
	return nil
}

// IsValidIPv4Address 验证字符串是否为合法IPv4地址
func IsValidIPv4Address(fldPath *field.Path, value string) field.ErrorList {
	var allErrors field.ErrorList
	ip := net.ParseIP(value)
	if ip == nil || ip.To4() == nil { // To4()返回nil表示非IPv4
		allErrors = append(allErrors, field.Invalid(fldPath, value, "必须是有效的IPv4地址"))
	}
	return allErrors
}

// IsValidIPv6Address 验证字符串是否为合法IPv6地址
func IsValidIPv6Address(fldPath *field.Path, value string) field.ErrorList {
	var allErrors field.ErrorList
	ip := net.ParseIP(value)
	if ip == nil || ip.To4() != nil { // To4()非nil表示是IPv4
		allErrors = append(allErrors, field.Invalid(fldPath, value, "必须是有效的IPv6地址"))
	}
	return allErrors
}

// 百分比格式校验规则
const (
	percentFmt    string = "[0-9]+%" // 数字+%格式（如10%）
	percentErrMsg string = "有效的百分比格式必须是数字后跟'%'（如1%、93%）"
)

// 百分比校验正则表达式
var percentRegexp = regexp.MustCompile("^" + percentFmt + "$")

// IsValidPercent 验证字符串是否为合法的百分比格式（如"50%"）
func IsValidPercent(percent string) []string {
	if !percentRegexp.MatchString(percent) {
		return []string{RegexError(percentErrMsg, percentFmt, "1%", "93%")}
	}
	return nil
}

// MaxLenError 生成"长度超限"的错误提示
func MaxLenError(length int) string {
	return fmt.Sprintf("长度不能超过%d个字符", length)
}

// RegexError 生成正则匹配失败的错误提示，包含示例和正则规则
func RegexError(msg string, fmt string, examples ...string) string {
	if len(examples) == 0 {
		return msg + "（用于验证的正则表达式：'" + fmt + "'）"
	}
	msg += "（例如："
	for i := range examples {
		if i > 0 {
			msg += "或"
		}
		msg += "'" + examples[i] + "'，"
	}
	msg += "用于验证的正则表达式：'" + fmt + "'）"
	return msg
}

// EmptyError 生成"不能为空"的错误提示
func EmptyError() string {
	return "不能为空"
}

// prefixEach 给错误信息列表中的每个信息添加前缀
func prefixEach(msgs []string, prefix string) []string {
	for i := range msgs {
		msgs[i] = prefix + msgs[i]
	}
	return msgs
}

// InclusiveRangeError 生成"数值范围"的错误提示（包含min和max）
func InclusiveRangeError(lo, hi int) string {
	return fmt.Sprintf("必须在%d到%d之间（包含边界）", lo, hi)
}

// 检查特殊字符比例
func hasExcessiveSpecialChars(input string) bool {
	specialCount := 0
	totalCount := len(input)

	for _, ch := range input {
		if unicode.IsPunct(ch) || unicode.IsSymbol(ch) {
			specialCount++
			// 如果特殊字符超过30%或绝对数量超过8个，认为是攻击载荷
			if specialCount > 8 || (float64(specialCount)/float64(totalCount)) > 0.3 {
				return true
			}
		}
	}
	return false
}

// ValidateListOptionsBase 验证 ListOptions 基础参数（函数选项模式）
func ValidateListOptionsBase(opts *v1.ListOptions) field.ErrorList {
	if opts == nil {
		return field.ErrorList{field.Required(field.NewPath("ListOptions"), "ListOptions 不能为空")}
	}

	var allErrors field.ErrorList
	basePath := field.NewPath("ListOptions")

	// 1. 校验 LabelSelector
	if opts.LabelSelector != "" {
		labelPath := basePath.Child("LabelSelector")
		errors := validateLabelSelector(opts.LabelSelector, GlobalMaxSelectorLen)
		for _, err := range errors {
			allErrors = append(allErrors, field.Invalid(labelPath, opts.LabelSelector, err))
		}
	}

	// 2. 校验 FieldSelector
	if opts.FieldSelector != "" {
		fieldPath := basePath.Child("FieldSelector")
		errors := validateFieldSelector(opts.FieldSelector, GlobalMaxSelectorLen)
		for _, err := range errors {
			allErrors = append(allErrors, field.Invalid(fieldPath, opts.FieldSelector, err))
		}
	}

	// 3. 校验 TimeoutSeconds 范围
	if opts.TimeoutSeconds != nil {
		timeoutPath := basePath.Child("TimeoutSeconds")
		timeout := *opts.TimeoutSeconds
		if timeout < 0 || timeout > GlobalMaxTimeout {
			allErrors = append(allErrors, field.Invalid(
				timeoutPath,
				timeout,
				InclusiveRangeError(0, int(GlobalMaxTimeout)),
			))
		}
	}

	// 4. 校验 Offset 范围
	if opts.Offset != nil {
		offsetPath := basePath.Child("Offset")
		offset := *opts.Offset
		if offset < 0 || offset > GlobalMaxOffset {
			allErrors = append(allErrors, field.Invalid(
				offsetPath,
				offset,
				InclusiveRangeError(0, int(GlobalMaxOffset)),
			))
		}
	}

	// 5. 校验 Limit 范围
	if opts.Limit != nil {
		limitPath := basePath.Child("Limit")
		limit := *opts.Limit
		if limit < 0 || limit > GlobalMaxLimit {
			allErrors = append(allErrors, field.Invalid(
				limitPath,
				limit,
				InclusiveRangeError(0, int(GlobalMaxLimit)),
			))
		}
	}

	// if opts.APIVersion != "" {
	// 	labelPath := basePath.Child("APIVersion")
	// 	if !stringutil.StringIn(opts.APIVersion, []string{"v1", "v2"}) {
	// 		allErrors = append(allErrors, field.Invalid(labelPath, opts.APIVersion, "取值范围在:v1,v2范围内"))
	// 	}
	// }

	return allErrors
}

func ValidateGetOptionsBase(opts *v1.GetOptions) field.ErrorList {
	if opts == nil {
		return field.ErrorList{field.Required(field.NewPath("GetOptions"), "GetOptions 不能为空")}
	}
	var allErrors field.ErrorList
	basePath := field.NewPath("GetOptions")
	if opts.APIVersion != "" {
		labelPath := basePath.Child("APIVersion")
		if !stringutil.StringIn(opts.APIVersion, []string{"v1", "v2"}) {
			allErrors = append(allErrors, field.Invalid(labelPath, opts.APIVersion, "取值范围在:v1,v2范围内"))
		}
	}
	return allErrors
}

func ValidateCreateOptionsBase(opts *v1.CreateOptions) field.ErrorList {
	if opts == nil {
		return field.ErrorList{field.Required(field.NewPath("CreateOptions"), "CreateOptions 不能为空")}
	}
	var allErrors field.ErrorList
	basePath := field.NewPath("CreateOptions")
	if opts.APIVersion != "" {
		labelPath := basePath.Child("APIVersion")
		if !stringutil.StringIn(opts.APIVersion, []string{"v1", "v2"}) {
			allErrors = append(allErrors, field.Invalid(labelPath, opts.APIVersion, "取值范围在:v1,v2范围内"))
		}
	}
	return allErrors
}

func ValidateDeleteOptionsBase(opts *v1.DeleteOptions) field.ErrorList {
	if opts == nil {
		return field.ErrorList{field.Required(field.NewPath("DeleteOptions"), "DeleteOptions 不能为空")}
	}
	var allErrors field.ErrorList
	basePath := field.NewPath("DeleteOptions")
	if opts.APIVersion != "" {
		labelPath := basePath.Child("APIVersion")
		if !stringutil.StringIn(opts.APIVersion, []string{"v1", "v2"}) {
			allErrors = append(allErrors, field.Invalid(labelPath, opts.APIVersion, "取值范围在:v1,v2范围内"))
		}
	}
	return allErrors
}

// validateLabelSelector 校验标签选择器的完整语法和内容（带长度配置）
func validateLabelSelector(selector string, maxSelectorLen int) []string {
	var errs []string

	// 1. 长度校验
	if len(selector) > maxSelectorLen {
		errs = append(errs, MaxLenError(maxSelectorLen))
	}

	// 2. 整体语法结构校验
	labelPattern := `^[\w-]+(=|!=| in | notin )([\w-]+|\([\w-, ]+\))(,[\w-]+(=|!=| in | notin )([\w-]+|\([\w-, ]+\)))*$`
	if !regexp.MustCompile(labelPattern).MatchString(selector) {
		errs = append(errs, RegexError(
			"标签选择器语法错误，支持 =, !=, in, notin",
			labelPattern,
			"env=prod",
			"app in (api,web),env!=test",
		))
		return errs // 整体语法错误，无需继续解析
	}

	// 3. 拆分多条件并校验每个条件
	conditions := strings.Split(selector, ",")
	for _, cond := range conditions {
		key, value := parseLabelCondition(cond)

		// 3.1 校验标签键
		if keyErrs := IsQualifiedName(key); len(keyErrs) > 0 {
			errs = append(errs, prefixEach(keyErrs, "标签键 '"+key+"' 不合法：")...)
		}

		// 3.2 校验标签值（复用 IsValidLabelValue）
		values := strings.Split(strings.Trim(value, "()"), ",") // 处理 in/notin 的值列表
		for _, v := range values {
			v = strings.TrimSpace(v)
			if valueErrs := IsValidLabelValue(v); len(valueErrs) > 0 {
				errs = append(errs, prefixEach(valueErrs, "标签值 '"+v+"' 不合法：")...)
			}
		}
	}

	return errs
}

// validateFieldSelector 校验字段选择器的完整语法（带长度配置）
func validateFieldSelector(selector string, maxSelectorLen int) []string {
	var errs []string

	// 1. 长度校验
	if len(selector) > maxSelectorLen {
		errs = append(errs, MaxLenError(maxSelectorLen))
	}

	// 2. 语法结构校验
	fieldPattern := `^[\w.]+(=|!=|>|<)([\w-]+|\d{4}-\d{2}-\d{2})(,[\w.]+(=|!=|>|<)([\w-]+|\d{4}-\d{2}-\d{2}))*$`
	if !regexp.MustCompile(fieldPattern).MatchString(selector) {
		errs = append(errs, RegexError(
			"字段选择器语法错误，支持 =, !=, >, <",
			fieldPattern,
			"status=active",
			"age>18,createTime<2024-01-01",
		))
	}

	return errs
}

// parseLabelCondition 解析标签选择器条件，提取键、运算符和值
func parseLabelCondition(cond string) (key, value string) {
	cond = strings.TrimSpace(cond)

	switch {
	case strings.Contains(cond, " notin "):
		parts := strings.SplitN(cond, " notin ", 2)
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	case strings.Contains(cond, " in "):
		parts := strings.SplitN(cond, " in ", 2)
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	case strings.Contains(cond, "!="):
		parts := strings.SplitN(cond, "!=", 2)
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	case strings.Contains(cond, "="):
		parts := strings.SplitN(cond, "=", 2)
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	default:
		return "", "" // 不应出现，已被正则校验拦截
	}
}
