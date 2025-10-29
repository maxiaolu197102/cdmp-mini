// validation/security.go
package validation

import (
	"errors"
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"unicode"
)

// 密码校验规则
const (
	minPassLength = 8   // 密码最小长度
	maxPassLength = 200 // 密码最大长度
)

// 安全检测配置
const (
	maxInputLength      = 256 // 最大输入长度
	maxSpecialCharRate  = 0.3 // 特殊字符最大比例
	maxSpecialCharCount = 8   // 特殊字符最大数量
)

// 公用恶意模式检测
func ContainsMaliciousPatterns(input string) bool {
	// 快速长度检查
	if len(input) > maxInputLength {
		return true
	}

	// 关键攻击模式检测
	maliciousPatterns := []string{
		// SQL 注入
		" OR ", " AND ", " UNION ", " SELECT ", " INSERT ", " UPDATE ", " DELETE ", " DROP ",
		"--", "/*", "*/", ";", "'", "\"", "`",
		// XSS 攻击
		"<script", "</script", "javascript:", "onload=", "onerror=", "onclick=",
		"alert(", "eval(", "document.cookie", "window.location",
		// 路径遍历
		"../", "..\\", "/etc/", "\\windows\\", "/bin/", "/usr/",
		// 命令注入
		"${", "$(", "|", "&", ">", "<", "\n", "\r",
	}

	inputLower := strings.ToLower(input)
	for _, pattern := range maliciousPatterns {
		if strings.Contains(inputLower, strings.ToLower(pattern)) {
			return true
		}
	}

	return false
}

// 检查特殊字符比例
func HasExcessiveSpecialChars(input string) bool {
	specialCount := 0
	totalCount := len(input)

	if totalCount == 0 {
		return false
	}

	for _, ch := range input {
		if unicode.IsPunct(ch) || unicode.IsSymbol(ch) {
			specialCount++
			if specialCount > maxSpecialCharCount ||
				(float64(specialCount)/float64(totalCount)) > maxSpecialCharRate {
				return true
			}
		}
	}
	return false
}

// 移除不可见和控制字符
func RemoveInvisibleChars(input string) string {
	var result strings.Builder
	for _, ch := range input {
		// 保留可见字符和常用空白字符
		if ch >= 32 && ch != 127 || ch == '\t' || ch == '\n' || ch == '\r' {
			result.WriteRune(ch)
		}
	}
	return result.String()
}

// IsValidPhone 生产级电话验证
func IsValidPhone(phone string) error {
	if phone == "" {
		return nil // 空值通过（可选字段）
	}

	// 🔒 前置安全检测
	if ContainsMaliciousPatterns(phone) {
		return fmt.Errorf("电话包含不安全内容")
	}

	// 清理输入
	cleaned := strings.TrimSpace(phone)
	cleaned = RemoveInvisibleChars(cleaned)

	// 长度检查
	if len(cleaned) < 11 || len(cleaned) > 20 {
		return fmt.Errorf("电话长度必须在11到20个字符之间")
	}

	// 🔒 检查不可见字符比例
	if hasExcessiveNonPhoneChars(cleaned) {
		return fmt.Errorf("电话格式不正确")
	}

	// 格式验证
	if !isValidPhoneFormat(cleaned) {
		return fmt.Errorf("电话格式不正确")
	}

	return nil
}

// 检查非电话字符比例
func hasExcessiveNonPhoneChars(input string) bool {
	nonPhoneCount := 0
	totalCount := len(input)

	for _, ch := range input {
		if !unicode.IsDigit(ch) && ch != '+' && ch != '-' && ch != '(' && ch != ')' && ch != ' ' {
			nonPhoneCount++
			if (float64(nonPhoneCount) / float64(totalCount)) > 0.2 {
				return true
			}
		}
	}
	return false
}

// isValidPhoneFormat 检查电话格式
func isValidPhoneFormat(phone string) bool {
	// 国际格式: +8613812345678
	if strings.HasPrefix(phone, "+") {
		return regexp.MustCompile(`^\+\d{10,19}$`).MatchString(phone)
	}

	// 中国手机号: 1开头，11位数字
	if regexp.MustCompile(`^1[3-9]\d{9}$`).MatchString(phone) {
		return true
	}

	// 带区号的固定电话
	if regexp.MustCompile(`^0\d{2,3}-?\d{7,8}$`).MatchString(phone) {
		return true
	}

	// 纯数字（国际号码不带+）
	return regexp.MustCompile(`^\d{10,20}$`).MatchString(phone)
}

// IsValidEmail 生产级邮箱验证
func IsValidEmail(email string) error {
	if email == "" {
		return errors.New("邮箱不能为空")
	}

	// 🔒 前置安全检测
	if ContainsMaliciousPatterns(email) {
		return fmt.Errorf("邮箱包含不安全内容")
	}

	// 清理输入
	email = strings.TrimSpace(email)
	email = RemoveInvisibleChars(email)

	// 长度检查
	if len(email) < 3 || len(email) > 254 {
		return errors.New("邮箱长度必须在3到254个字符之间")
	}

	// 基础格式检查
	pattern := `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
	matched, _ := regexp.MatchString(pattern, email)
	if !matched {
		return errors.New("邮箱格式不正确")
	}

	// 使用标准库进一步验证
	_, err := mail.ParseAddress(email)
	if err != nil {
		return errors.New("邮箱格式不正确")
	}

	// 禁止某些特殊字符组合
	if strings.Contains(email, "..") || strings.Contains(email, ".@") {
		return errors.New("邮箱格式不正确")
	}

	return nil
}

// IsValidPassword 生产级密码验证
func IsValidPassword(password string) error {
	var hasUpper bool      // 是否包含大写字母
	var hasLower bool      // 是否包含小写字母
	var hasNumber bool     // 是否包含数字
	var hasSpecial bool    // 是否包含特殊字符（标点或符号）
	var passLen int        // 密码长度
	var errorString string // 错误信息汇总

	// 🔒 前置安全检测
	if ContainsMaliciousPatterns(password) {
		return fmt.Errorf("密码包含不安全内容")
	}

	// 遍历密码字符，检查各类字符是否存在
	for _, ch := range password {
		switch {
		case unicode.IsNumber(ch):
			hasNumber = true
			passLen++
		case unicode.IsUpper(ch):
			hasUpper = true
			passLen++
		case unicode.IsLower(ch):
			hasLower = true
			passLen++
		case unicode.IsPunct(ch) || unicode.IsSymbol(ch):
			hasSpecial = true
			passLen++
		case ch == ' ': // 允许空格
			passLen++
		default:
			// 非法字符直接返回，避免继续处理
			return fmt.Errorf("密码包含非法字符")
		}
	}

	// 拼接错误信息
	appendError := func(err string) {
		if len(strings.TrimSpace(errorString)) != 0 {
			errorString += "，" + err
		} else {
			errorString = err
		}
	}

	if !hasLower {
		appendError("缺少小写字母")
	}
	if !hasUpper {
		appendError("缺少大写字母")
	}
	if !hasNumber {
		appendError("至少需要一个数字")
	}
	if !hasSpecial {
		appendError("缺少特殊字符")
	}
	if !(minPassLength <= passLen && passLen <= maxPassLength) {
		appendError(
			fmt.Sprintf("密码长度必须在%d到%d个字符之间", minPassLength, maxPassLength),
		)
	}

	if len(errorString) != 0 {
		return fmt.Errorf("%s", errorString)
	}

	// 🔒 后置安全检测 - 特殊字符比例
	if HasExcessiveSpecialChars(password) {
		return fmt.Errorf("密码包含过多特殊字符")
	}

	return nil
}

// IsQualifiedName 生产级名称验证（保持您原有逻辑）
func IsQualifiedName(value string) []string {
	var errs []string
	parts := strings.Split(value, "/") // 按"/"分割前缀和名称部分
	var name string

	switch len(parts) {
	case 1:
		name = parts[0] // 无前缀，直接使用整个值作为名称
	case 2:
		var prefix string
		prefix, name = parts[0], parts[1] // 提取前缀和名称
		if len(prefix) == 0 {
			errs = append(errs, "前缀部分"+EmptyError()) // 前缀不能为空
		} else if msgs := IsDNS1123Subdomain(prefix); len(msgs) != 0 {
			errs = append(errs, prefixEach(msgs, "前缀部分")...) // 前缀需符合DNS子域名规则
		}
	default:
		// 分割后超过2部分，不符合格式
		return append(
			errs,
			"合格名称"+RegexError(
				qualifiedNameErrMsg,
				qualifiedNameFmt,
				"MyName",
				"my.name",
				"123-abc",
			)+"，可包含可选DNS前缀（如'example.com/MyName'）",
		)
	}

	// 验证名称部分
	if len(name) == 0 {
		errs = append(errs, "名称部分"+EmptyError()) // 名称不能为空
	} else if len(name) > qualifiedNameMaxLength {
		errs = append(errs, "名称部分"+MaxLenError(qualifiedNameMaxLength)) // 名称长度超限
	}
	if !qualifiedNameRegexp.MatchString(name) {
		errs = append(
			errs,
			"名称部分"+RegexError(qualifiedNameErrMsg, qualifiedNameFmt, "MyName", "my.name", "123-abc"),
		)
	}
	return errs
}
