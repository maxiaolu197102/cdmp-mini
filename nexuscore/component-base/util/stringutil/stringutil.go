/*
这是一个字符串处理工具包，提供了多种常用的字符串操作函数。以下是详细分析：

包摘要
stringutil 包提供了一系列字符串处理工具函数，包括集合操作、格式转换、查找判断和字符串反转等功能，基于 govalidator 库扩展。

函数使用方式和业务场景
1. Diff - 计算字符串切片差集
使用方式：

go
base := []string{"a", "b", "c", "d"}
exclude := []string{"b", "d", "e"}
result := stringutil.Diff(base, exclude) // ["a", "c"]
业务场景：

权限过滤：从用户所有权限中排除已被禁用的权限

内容过滤：从文章列表中过滤掉用户已读的文章

黑名单过滤：从用户列表中移除黑名单用户

go
// 用户拥有的所有权限
allPermissions := []string{"read", "write", "delete", "admin"}
// 被禁用的权限
disabledPermissions := []string{"delete", "admin"}
// 计算有效权限
effectivePermissions := stringutil.Diff(allPermissions, disabledPermissions)
2. Unique - 字符串切片去重
使用方式：

go
duplicates := []string{"a", "b", "a", "c", "b", "d"}
unique := stringutil.Unique(duplicates) // ["a", "b", "c", "d"]（顺序可能不同）
业务场景：

标签去重：处理用户输入的重复标签

日志分析：统计唯一的用户ID或IP地址

数据清洗：去除重复的数据记录

go
// 用户输入的标签（可能有重复）
userTags := []string{"golang", "python", "golang", "java", "python"}
// 去重处理
uniqueTags := stringutil.Unique(userTags)
3. CamelCaseToUnderscore - 驼峰转下划线
使用方式：

go
camelCase := "UserName"
underscore := stringutil.CamelCaseToUnderscore(camelCase) // "user_name"
业务场景：

数据库字段映射：将Go结构体字段名映射为数据库列名

API字段转换：RESTful API 的JSON字段命名转换

配置文件键名转换

go
// Go结构体到数据库字段的映射
type User struct {
    UserID      int    `json:"userId"`
    UserName    string `json:"userName"`
    CreatedAt   time.Time
}

// 自动生成SQL语句
fieldName := "UserName"
dbColumn := stringutil.CamelCaseToUnderscore(fieldName) // "user_name"
sql := fmt.Sprintf("SELECT %s FROM users", dbColumn)
4. UnderscoreToCamelCase - 下划线转驼峰
使用方式：

go
underscore := "user_name"
camelCase := stringutil.UnderscoreToCamelCase(underscore) // "UserName"
业务场景：

数据库到Go的映射：将数据库列名转换为Go结构体字段名

API响应格式化：将数据库字段转换为JSON字段名

数据转换处理

go
// 数据库查询结果映射
dbColumns := []string{"user_id", "user_name", "created_at"}
for _, column := range dbColumns {
    fieldName := stringutil.UnderscoreToCamelCase(column)
    // 使用反射设置结构体字段值
}
5. FindString - 查找字符串在切片中的位置
使用方式：

go
array := []string{"apple", "banana", "orange"}
index := stringutil.FindString(array, "banana") // 1
index = stringutil.FindString(array, "grape")   // -1
业务场景：

配置验证：检查输入值是否在允许的选项列表中

权限检查：验证用户角色是否在授权角色列表中

数据验证：检查枚举值是否有效

go
// 验证HTTP方法是否支持
supportedMethods := []string{"GET", "POST", "PUT", "DELETE"}
requestMethod := "PATCH"

if stringutil.FindString(supportedMethods, requestMethod) == -1 {
    return errors.New("不支持的HTTP方法")
}
6. StringIn - 判断字符串是否在切片中
使用方式：

go
array := []string{"admin", "user", "guest"}
exists := stringutil.StringIn("admin", array) // true
exists = stringutil.StringIn("visitor", array) // false
业务场景：

权限检查：快速检查用户是否有某个权限

类别验证：验证产品类别是否有效

状态检查：检查状态值是否在预期范围内

go
// 中间件中的权限检查
func AuthMiddleware(requiredRole string) {
    userRoles := getUserRoles()
    if !stringutil.StringIn(requiredRole, userRoles) {
        return errors.New("权限不足")
    }
}
7. Reverse - 字符串反转（支持UTF-8）
使用方式：

go
original := "Hello, 世界"
reversed := stringutil.Reverse(original) // "界世 ,olleH"
业务场景：

文本处理：字符串反转显示需求

密码学应用：简单的字符串变换

测试验证：验证字符串处理功能

国际化支持：处理多语言文本的反转

go
// 国际化字符串反转示例
func displayReversedText(text string) {
    // 正确处理中文、阿拉伯文等Unicode字符
    reversed := stringutil.Reverse(text)
    fmt.Println("反转结果:", reversed)
}

// 输入: "Hello, 你好！"
// 输出: "！好你 ,olleH"
综合业务场景示例
用户权限管理系统
go
func CheckUserAccess(userID int, requiredPermission string) bool {
    // 获取用户所有权限
    allPermissions := getUserPermissions(userID)

    // 获取系统黑名单权限
    blacklistedPermissions := getBlacklistedPermissions()

    // 计算有效权限（排除黑名单）
    effectivePermissions := stringutil.Diff(allPermissions, blacklistedPermissions)

    // 检查是否拥有所需权限
    return stringutil.StringIn(requiredPermission, effectivePermissions)
}

// 数据库字段映射
func MapStructToDB(model interface{}) map[string]interface{} {
    result := make(map[string]interface{})
    val := reflect.ValueOf(model)

    for i := 0; i < val.NumField(); i++ {
        field := val.Type().Field(i)
        fieldName := field.Name
        dbColumn := stringutil.CamelCaseToUnderscore(fieldName)
        result[dbColumn] = val.Field(i).Interface()
    }

    return result
}
性能特点
Diff 和 Unique：使用 map 实现 O(n) 时间复杂度

FindString 和 StringIn：线性查找 O(n)，适合小规模数据

Reverse：正确处理UTF-8字符，支持多语言

格式转换：基于成熟的 govalidator 库

适用场景总结
Web开发：权限检查、数据验证、字段映射
数据处理：数据清洗、去重、过滤
API开发：请求验证、响应格式化
数据库操作：字段名转换、数据映射
国际化应用：多语言文本处理
这个工具包提供了字符串处理的常见需求，特别适合Web开发和数据处理场景。

Diff:计算两个字符串切片的差集，返回在第一个切片中但不在第二个切片中的元素
Unique:去除字符串切片中的重复元素，返回唯一值集合
CamelCaseToUnderscore:将驼峰命名法转换为下划线命名法（如：UserName → user_name）
UnderscoreToCamelCase:将下划线命名法转换为驼峰命名法（如：user_name → UserName）
FindString:在字符串切片中查找指定字符串，返回其索引位置（未找到返回-1）
StringIn:检查字符串是否存在于给定的字符串切片中（基于FindString的布尔包装）
Reverse:反转字符串，正确处理UTF-8编码的多字节字符

*/

package stringutil

import (
	"unicode/utf8"

	"github.com/asaskevich/govalidator"
)

// Creates an slice of slice values not included in the other given slice.
func Diff(base, exclude []string) (result []string) {
	excludeMap := make(map[string]bool)
	for _, s := range exclude {
		excludeMap[s] = true
	}
	for _, s := range base {
		if !excludeMap[s] {
			result = append(result, s)
		}
	}
	return result
}

func Unique(ss []string) (result []string) {
	smap := make(map[string]bool)
	for _, s := range ss {
		smap[s] = true
	}
	for s := range smap {
		result = append(result, s)
	}
	return result
}

func CamelCaseToUnderscore(str string) string {
	return govalidator.CamelCaseToUnderscore(str)
}

func UnderscoreToCamelCase(str string) string {
	return govalidator.UnderscoreToCamelCase(str)
}

func FindString(array []string, str string) int {
	for index, s := range array {
		if str == s {
			return index
		}
	}
	return -1
}

func StringIn(str string, array []string) bool {
	return FindString(array, str) > -1
}

func Reverse(s string) string {
	size := len(s)
	buf := make([]byte, size)
	for start := 0; start < size; {
		r, n := utf8.DecodeRuneInString(s[start:])
		start += n
		utf8.EncodeRune(buf[size-start:], r)
	}
	return string(buf)
}
