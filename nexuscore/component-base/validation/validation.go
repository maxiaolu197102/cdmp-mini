/*
这个代码实现了一个基于 go-playground/validator 的定制化验证器，用于对配置数据进行结构化验证。以下是对该包及其功能的详细分析：

包摘要
包名: validation
作用: 提供自定义的结构体验证功能，用于验证配置数据的合法性。
核心功能:

注册自定义验证规则（目录存在性、文件存在性、描述长度、名称格式）

提供多语言错误消息支持

生成结构化的验证错误列表

主要函数使用方式
1. 创建验证器
go
// 创建针对特定数据结构的验证器
validator := NewValidator(configData)
2. 执行验证
go
// 验证数据并获取错误列表
errors := validator.Validate()
if len(errors) > 0 {
    // 处理验证错误
    for _, err := range errors {
        fmt.Printf("Field: %s, Error: %s\n", err.Field, err.Detail)
    }
}
3. 自定义验证规则使用
在结构体标签中使用自定义验证规则：

go
type Config struct {
    ConfigDir    string `validate:"dir"`        // 必须存在目录
    ConfigFile   string `validate:"file"`       // 必须存在文件
    ServiceName  string `validate:"name"`       // 必须符合命名规范
    Description  string `validate:"description"`// 描述长度限制
}
业务场景
1. 配置文件验证
场景: 应用程序启动时验证配置文件各项参数的正确性

go
type ServerConfig struct {
    ListenAddress string `validate:"required"`
    DataDir       string `validate:"dir"`
    LogFile       string `validate:"file"`
    ServiceName   string `validate:"name"`
}

func LoadConfig(path string) (*ServerConfig, error) {
    config := &ServerConfig{}
    // 加载配置...

    validator := validation.NewValidator(config)
    if errs := validator.Validate(); len(errs) > 0 {
        return nil, fmt.Errorf("config validation failed: %v", errs)
    }

    return config, nil
}
2. API 请求参数验证
场景: REST API 接口的请求参数验证

go
type CreateUserRequest struct {
    Username    string `validate:"name" json:"username"`
    DisplayName string `validate:"description" json:"displayName"`
    AvatarPath  string `validate:"file" json:"avatarPath"`
}

func (h *UserHandler) CreateUser(c *gin.Context) {
    var req CreateUserRequest
    if err := c.BindJSON(&req); err != nil {
        c.JSON(400, gin.H{"error": "invalid request"})
        return
    }

    validator := validation.NewValidator(&req)
    if errs := validator.Validate(); len(errs) > 0 {
        c.JSON(400, gin.H{"errors": errs})
        return
    }

    // 处理创建用户逻辑
}
3. 系统配置验证
场景: 管理系统中的配置项验证

go
type SystemSettings struct {
    BackupDir      string `validate:"dir"`
    Certificate    string `validate:"file"`
    SystemName     string `validate:"name"`
    SystemDesc     string `validate:"description"`
    MaxConnections int    `validate:"min=1,max=1000"`
}

func ValidateSettings(settings *SystemSettings) field.ErrorList {
    validator := validation.NewValidator(settings)
    return validator.Validate()
}
4. 命令行参数验证
场景: CLI 工具的参数验证

go
func validateCLIOptions(options *CLIOptions) error {
    validator := validation.NewValidator(options)
    if errs := validator.Validate(); len(errs) > 0 {
        return fmt.Errorf("invalid options: %v", errs)
    }
    return nil
}
优势特点
结构化错误信息: 返回 field.ErrorList，便于程序化处理

多语言支持: 内置英文错误消息，可扩展其他语言

可扩展性: 易于添加新的自定义验证规则

标准兼容: 基于广泛使用的 validator/v10 包

类型安全: 通过结构体标签进行验证，减少运行时错误

这个验证框架特别适合需要严格验证配置数据正确性的企业级应用和系统工具。

就比如这个结构type InsecureServingOptions struct {
	BindAddress string `json:"bind-address" mapstructure:"bind-address"`
	BindPort    int    `json:"bind-port"    mapstructure:"bind-port"`
}使用上述校验器如何使用？
对于您的 InsecureServingOptions 结构体，使用上述校验器的方式如下：

1. 首先为结构体添加验证标签
go
type InsecureServingOptions struct {
	BindAddress string `json:"bind-address" mapstructure:"bind-address" validate:"required,ip"`
	BindPort    int    `json:"bind-port"    mapstructure:"bind-port"    validate:"required,min=1,max=65535"`
}
2. 创建验证器并执行验证
基本使用方式：
go
func ValidateInsecureServing(options *InsecureServingOptions) field.ErrorList {
	validator := NewValidator(options)
	return validator.Validate()
}

// 使用示例
func main() {
	options := &InsecureServingOptions{
		BindAddress: "192.168.1.100",
		BindPort:    8080,
	}

	errs := ValidateInsecureServing(options)
	if len(errs) > 0 {
		fmt.Printf("Validation errors: %v\n", errs)
		// 处理错误
	} else {
		fmt.Println("Validation passed!")
	}
}
更完整的示例：
go
package main

import (
	"fmt"
	"log"

	"github.com/your-project/validation"
)

type InsecureServingOptions struct {
	BindAddress string `json:"bind-address" mapstructure:"bind-address" validate:"required,ip"`
	BindPort    int    `json:"bind-port"    mapstructure:"bind-port"    validate:"required,min=1,max=65535"`
}

func main() {
	// 测试用例1：有效配置
	validOptions := &InsecureServingOptions{
		BindAddress: "0.0.0.0",
		BindPort:    8080,
	}

	fmt.Println("Testing valid configuration:")
	errs := validation.NewValidator(validOptions).Validate()
	if len(errs) > 0 {
		log.Printf("Unexpected errors: %v", errs)
	} else {
		fmt.Println("✓ Validation passed")
	}

	// 测试用例2：无效IP地址
	invalidIPOptions := &InsecureServingOptions{
		BindAddress: "invalid-ip",
		BindPort:    8080,
	}

	fmt.Println("\nTesting invalid IP address:")
	errs = validation.NewValidator(invalidIPOptions).Validate()
	for _, err := range errs {
		fmt.Printf("✗ %s: %s\n", err.Field, err.Detail)
	}

	// 测试用例3：端口超出范围
	invalidPortOptions := &InsecureServingOptions{
		BindAddress: "127.0.0.1",
		BindPort:    70000, // 无效端口
	}

	fmt.Println("\nTesting invalid port:")
	errs = validation.NewValidator(invalidPortOptions).Validate()
	for _, err := range errs {
		fmt.Printf("✗ %s: %s\n", err.Field, err.Detail)
	}
}
3. 集成到配置加载流程中
go
func LoadConfig() (*InsecureServingOptions, error) {
	// 从配置文件或环境变量加载配置
	options := &InsecureServingOptions{
		BindAddress: viper.GetString("bind-address"),
		BindPort:    viper.GetInt("bind-port"),
	}

	// 验证配置
	validator := validation.NewValidator(options)
	if errs := validator.Validate(); len(errs) > 0 {
		return nil, fmt.Errorf("invalid insecure serving configuration: %v", errs)
	}

	return options, nil
}
4. 常用的验证标签
对于网络相关的结构体，可以考虑使用这些验证标签：

go
type InsecureServingOptions struct {
	BindAddress string `validate:"required,ip"`                    // 必须且为有效IP
	BindPort    int    `validate:"required,min=1,max=65535"`       // 必须且在端口范围内
}

// 或者更严格的验证
type StrictServingOptions struct {
	BindAddress string `validate:"required,ipv4"`                  // 必须为IPv4
	BindPort    int    `validate:"required,min=1024,max=49151"`    // 必须为用户端口范围
}
5. 自定义错误消息处理
go
func ValidateWithCustomMessages(options *InsecureServingOptions) error {
	errs := validation.NewValidator(options).Validate()
	if len(errs) == 0 {
		return nil
	}

	// 自定义错误消息格式
	var errorMessages []string
	for _, err := range errs {
		switch err.Field {
		case "BindAddress":
			errorMessages = append(errorMessages, fmt.Sprintf("绑定地址无效: %s", err.Detail))
		case "BindPort":
			errorMessages = append(errorMessages, fmt.Sprintf("端口号无效: %s", err.Detail))
		default:
			errorMessages = append(errorMessages, err.Detail)
		}
	}

	return fmt.Errorf(strings.Join(errorMessages, "; "))
}
这样使用验证器可以确保您的 InsecureServingOptions 配置在程序启动前就得到充分的验证，避免运行时出现网络绑定失败等问题。


*/

package validation

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"

	english "github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	"github.com/go-playground/validator/v10/translations/en"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation/field"
)

const (
	maxDescriptionLength = 255
)

// Validator is a custom validator for configs.
type Validator struct {
	val   *validator.Validate
	data  interface{}
	trans ut.Translator
}

var (
	initOnce        sync.Once
	defaultValidate *validator.Validate
	defaultTrans    ut.Translator
)

// ensureDefaultValidator lazily builds the shared validator and translator instance.
func ensureDefaultValidator() {
	initOnce.Do(func() {
		result := validator.New()

		// independent validators
		_ = result.RegisterValidation("dir", validateDir)
		_ = result.RegisterValidation("file", validateFile)
		_ = result.RegisterValidation("description", validateDescription)
		_ = result.RegisterValidation("name", validateName)

		eng := english.New()
		uni := ut.New(eng, eng)
		trans, found := uni.GetTranslator("en")
		if !found {
			panic("validator translation 'en' not found")
		}

		if err := en.RegisterDefaultTranslations(result, trans); err != nil {
			panic(err)
		}

		translations := []struct {
			tag         string
			translation string
		}{
			{
				tag:         "dir",
				translation: "{0} must point to an existing directory, but found '{1}'",
			},
			{
				tag:         "file",
				translation: "{0} must point to an existing file, but found '{1}'",
			},
			{
				tag:         "description",
				translation: fmt.Sprintf("must be less than %d", maxDescriptionLength),
			},
			{
				tag:         "name",
				translation: "is not a invalid name",
			},
		}
		for _, t := range translations {
			if err := result.RegisterTranslation(t.tag, trans, registrationFunc(t.tag, t.translation), translateFunc); err != nil {
				panic(err)
			}
		}

		defaultValidate = result
		defaultTrans = trans
	})
}

// NewValidator creates a new Validator.
func NewValidator(data interface{}) *Validator {
	ensureDefaultValidator()

	return &Validator{
		val:   defaultValidate,
		data:  data,
		trans: defaultTrans,
	}
}

func registrationFunc(tag string, translation string) validator.RegisterTranslationsFunc {
	return func(ut ut.Translator) (err error) {
		if err = ut.Add(tag, translation, true); err != nil {
			return
		}

		return
	}
}

func translateFunc(ut ut.Translator, fe validator.FieldError) string {
	t, err := ut.T(fe.Tag(), fe.Field(), reflect.ValueOf(fe.Value()).String())
	if err != nil {
		return fe.(error).Error()
	}

	return t
}

// Validate validates config for errors and returns an error (it can be casted to
// ValidationErrors, containing a list of errors inside). When error is printed as string, it will
// automatically contains the full list of validation errors.
func (v *Validator) Validate() field.ErrorList {
	// validate policy
	err := v.val.Struct(v.data)
	if err == nil {
		return nil
	}

	// this check is only needed when your code could produce
	// an invalid value for validation such as interface with nil
	// value most including myself do not usually have code like this.
	if _, ok := err.(*validator.InvalidValidationError); ok {
		return field.ErrorList{field.Invalid(field.NewPath(""), err.Error(), "")}
	}

	allErrs := field.ErrorList{}

	vErrors, _ := err.(validator.ValidationErrors)
	for _, vErr := range vErrors {
		// 1. 移除结构体名称（如 "User."）
		namespace := strings.ReplaceAll(vErr.Namespace(), "User.", "")
		// 2. 转换为 JSON 字段格式（如 "Nickname" → "nickname"）
		jsonField := strings.ToLower(namespace)
		// 3. 处理嵌套字段（如 "metadata.Name" → "metadata.name"）
		jsonField = strings.ReplaceAll(jsonField, ".name", ".name") // 保持小写（示例）

		allErrs = append(allErrs, field.Invalid(
			field.NewPath(jsonField), // JSON 字段路径
			vErr.Value(),             // 错误值
			vErr.Translate(v.trans),  // 错误详情（如“必填项”）
		))
	}

	return allErrs

}

// validateDir checks if a given string is an existing directory.
func validateDir(fl validator.FieldLevel) bool {
	path := fl.Field().String()
	if stat, err := os.Stat(path); err == nil && stat.IsDir() {
		return true
	}

	return false
}

// validateFile checks if a given string is an existing file.
func validateFile(fl validator.FieldLevel) bool {
	path := fl.Field().String()
	if stat, err := os.Stat(path); err == nil && !stat.IsDir() {
		return true
	}

	return false
}

// validateDescription checks if a given description is illegal.
func validateDescription(fl validator.FieldLevel) bool {
	description := fl.Field().String()

	return len(description) <= maxDescriptionLength
}

// validateName checks if a given name is illegal.
func validateName(fl validator.FieldLevel) bool {
	name := fl.Field().String()
	if errs := IsQualifiedName(name); len(errs) > 0 {
		return false
	}

	return true
}
