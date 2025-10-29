package validation

import "fmt"

// ValidateUserFields 校验用户相关字段长度，按数据库约束
func ValidateUserFields(name, nickname, password, email, phone string) error {
	if len(name) > 45 {
		return fmt.Errorf("NAME_TOO_LONG: 用户名长度超出数据库限制(45): %s", name)
	}
	if len(nickname) > 30 {
		return fmt.Errorf("NICKNAME_TOO_LONG: 昵称长度超出数据库限制(30): %s", nickname)
	}
	if len(password) > 255 {
		return fmt.Errorf("PASSWORD_TOO_LONG: 密码长度超出数据库限制(255)")
	}
	if len(email) > 256 {
		return fmt.Errorf("EMAIL_TOO_LONG: 邮箱长度超出数据库限制(256): %s", email)
	}
	if len(phone) > 20 {
		return fmt.Errorf("PHONE_TOO_LONG: 手机号长度超出数据库限制(20): %s", phone)
	}
	return nil
}
