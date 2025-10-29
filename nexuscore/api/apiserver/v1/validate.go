package v1

import (
	"fmt"
	"strings"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation/field"
)

// Validate validates that a user object is valid.
func (u *User) Validate() field.ErrorList {
	val := validation.NewValidator(u)
	allErrs := val.Validate()

	// 补充用户名校验
	namePath := field.NewPath("name")
	if errs := validation.IsQualifiedName(u.Name); len(errs) > 0 {
		errsMsg := strings.Join(errs, ":")
		allErrs = append(allErrs, field.Invalid(
			namePath,
			u.Name,
			fmt.Sprintf("用户名不合法: %s", errsMsg),
		))
	}

	// 补充密码强度校验
	passwordPath := field.NewPath("password")
	if err := validation.IsValidPassword(u.Password); err != nil {
		allErrs = append(allErrs, field.Invalid(
			passwordPath,
			"******", // 密码脱敏
			err.Error(),
		))
	}

	// 补充电话格式校验 - 从密码校验中移出来
	phonePath := field.NewPath("phone")
	if u.Phone != "" {
		if err := validation.IsValidPhone(u.Phone); err != nil {
			allErrs = append(allErrs, field.Invalid(
				phonePath,
				u.Phone,
				err.Error(),
			))
		}
	}

	// 新增邮箱格式校验
	emailPath := field.NewPath("email")
	if u.Email != "" {
		if err := validation.IsValidEmail(u.Email); err != nil {
			allErrs = append(allErrs, field.Invalid(
				emailPath,
				u.Email,
				err.Error(),
			))
		}
	}

	return allErrs
}

// ValidateUpdate validates that a user object is valid when update.
// Like User.Validate but not validate password.
func (u *User) ValidateUpdate() field.ErrorList {
	val := validation.NewValidator(u)
	allErrs := val.Validate()
	// 过滤密码的必填校验（更新时可不用提供密码）
	allErrs = allErrs.Filter(func(err error) bool {
		if e, ok := err.(*field.Error); ok {
			return e.Field == "password" && e.Type == field.ErrorTypeRequired
		}
		return false
	})
	return allErrs
}

// Validate validates that a secret object is valid.
func (s *Secret) Validate() field.ErrorList {
	val := validation.NewValidator(s)

	return val.Validate()
}

// Validate validates that a policy object is valid.
func (p *Policy) Validate() field.ErrorList {
	val := validation.NewValidator(p)

	return val.Validate()
}
