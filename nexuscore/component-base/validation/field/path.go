/*
Path 包分析（修正版）
包摘要
这是一个专门用于构建和表示字段路径的实用包。它提供了结构化的方式来构建从根到特定字段的路径，主要用于在数据验证、错误报告等场景中精确定位字段位置。

核心组件
Path 结构体
go

	type Path struct {
	    name   string // 字段名称
	    index  string // 索引或键（如果是数组或map）
	    parent *Path  // 父路径
	}

函数使用方式
创建和构建路径
go
// 1. 创建根路径
rootPath := field.NewPath("user")
// user

// 2. 创建多级路径
deepPath := field.NewPath("data", "user", "profile")
// data.user.profile

// 3. 添加子字段
usernamePath := rootPath.Child("username")
// user.username

// 4. 数组索引
usersPath := field.NewPath("users")
firstUser := usersPath.Index(0)
// users[0]

// 5. Map键值
configPath := field.NewPath("config")
dbConfig := configPath.Key("database")
// config[database]

// 6. 混合使用
complexPath := field.NewPath("api").

	Child("endpoints").
	Index(1).
	Child("methods").
	Key("GET")

// api.endpoints[1].methods[GET]
获取路径信息
go
path := field.NewPath("users").Index(1).Child("emails").Index(0)

// 获取路径字符串
pathStr := path.String() // "users[1].emails[0]"

// 获取根路径
root := path.Root() // 返回 "users" 的路径
使用的业务场景
1. 验证框架中的错误定位
go
// 假设有一个独立的验证包

	func validateUser(user User) []ValidationError {
	    var errors []ValidationError
	    path := field.NewPath("user")

	    if user.Name == "" {
	        errors = append(errors, ValidationError{
	            Path:    path.Child("name"),
	            Message: "姓名不能为空",
	        })
	    }

	    if user.Age < 0 {
	        errors = append(errors, ValidationError{
	            Path:    path.Child("age"),
	            Message: "年龄不能为负数",
	            Value:   user.Age,
	        })
	    }

	    return errors
	}

2. 配置解析和验证
go

	func validateConfig(config Config) []ConfigError {
	    var errors []ConfigError
	    basePath := field.NewPath("config")

	    if config.Database.Host == "" {
	        errors = append(errors, ConfigError{
	            Field:   basePath.Child("database").Child("host"),
	            Message: "数据库主机必须配置",
	        })
	    }

	    for i, endpoint := range config.API.Endpoints {
	        if endpoint.Path == "" {
	            errors = append(errors, ConfigError{
	                Field:   basePath.Child("api").Child("endpoints").Index(i).Child("path"),
	                Message: "API端点路径必须配置",
	            })
	        }
	    }

	    return errors
	}

3. 表单数据处理
go

	func processFormData(form FormData) map[string]string {
	    errors := make(map[string]string)
	    path := field.NewPath("form")

	    if form.Email == "" {
	        errors[path.Child("email").String()] = "邮箱不能为空"
	    }

	    if len(form.Tags) > 10 {
	        errors[path.Child("tags").String()] = "标签数量不能超过10个"
	    }

	    return errors
	}

4. API 响应构建
go

	func buildErrorResponse(errors []ValidationError) APIResponse {
	    var fieldErrors []FieldError

	    for _, err := range errors {
	        fieldErrors = append(fieldErrors, FieldError{
	            Field:   err.Path.String(), // 使用Path的字符串表示
	            Message: err.Message,
	            Value:   err.Value,
	        })
	    }

	    return APIResponse{
	        Success: false,
	        Errors:  fieldErrors,
	    }
	}

5. 日志记录和调试
go

	func logDataIssues(data interface{}) {
	    // 记录数据中特定字段的问题
	    path := field.NewPath("response", "items")

	    if items, ok := data.(map[string]interface{})["items"].([]interface{}); ok {
	        for i, item := range items {
	            if itemMap, ok := item.(map[string]interface{}); ok {
	                if itemMap["id"] == nil {
	                    log.Printf("缺少ID字段在: %s", path.Index(i).Child("id").String())
	                }
	            }
	        }
	    }
	}

6. 数据转换和映射
go

	func mapFields(source, target map[string]interface{}, mapping map[string]*Path) {
	    for srcField, targetPath := range mapping {
	        if value, exists := source[srcField]; exists {
	            // 使用Path来构建目标数据结构
	            current := target
	            path := targetPath

	            // 遍历路径并创建嵌套结构
	            for path.parent != nil {
	                // 实现具体的映射逻辑...
	            }
	        }
	    }
	}

设计优势
类型安全：通过方法链式调用避免字符串拼接错误

可读性强：清晰的API设计，意图明确

灵活性强：支持数组、map、嵌套结构等各种场景

易于扩展：可以轻松添加新的路径构建方法

这个 field 包是一个基础工具包，其他包（如验证包、错误处理包）可以依赖它来提供字段路径功能，从而实现关注点分离。

path := field.NewPath("users").Index(1).Child("emails").Index(0) 结果是多少？
path.String() 的结果是：users[1].emails[0]

路径构建过程分解：
go
// 步骤分解：
step1 := field.NewPath("users")        // "users"
step2 := step1.Index(1)                // "users[1]"
step3 := step2.Child("emails")         // "users[1].emails"
step4 := step3.Index(0)                // "users[1].emails[0]"

path := step4
fmt.Println(path.String()) // 输出: users[1].emails[0]
路径结构表示：
text
Path{index: "0", parent:

	  ⇩
	  Path{name: "emails", parent:
	    ⇩
	    Path{index: "1", parent:
	      ⇩
	      Path{name: "users", parent: nil}
	    }
	  }
	}

其他示例：
go
// 更多示例：
path1 := field.NewPath("config").Key("database").Child("host")
// config[database].host

path2 := field.NewPath("items").Index(0).Child("metadata").Key("type")
// items[0].metadata[type]

path3 := field.NewPath("response", "data", "users")
// response.data.users

path4 := field.NewPath("nested").Child("array").Index(2).Child("object").Key("id")
// nested.array[2].object[id]
这个路径表示法非常适合在错误消息中精确定位数据结构中的具体位置。
*/
package field

import (
	"bytes"
	"fmt"
	"strconv"
)

// Path represents the path from some root to a particular field.
type Path struct {
	name   string // the name of this field or "" if this is an index
	index  string // if name == "", this is a subscript (index or map key) of the previous element
	parent *Path  // nil if this is the root element
}

// NewPath creates a root Path object.
func NewPath(name string, moreNames ...string) *Path {
	r := &Path{name: name, parent: nil}
	for _, anotherName := range moreNames {
		r = &Path{name: anotherName, parent: r}
	}
	return r
}

// Root returns the root element of this Path.
func (p *Path) Root() *Path {
	for ; p.parent != nil; p = p.parent {
		// Do nothing.
	}
	return p
}

// Child creates a new Path that is a child of the method receiver.
func (p *Path) Child(name string, moreNames ...string) *Path {
	r := NewPath(name, moreNames...)
	r.Root().parent = p
	return r
}

// Index indicates that the previous Path is to be subscripted by an int.
// This sets the same underlying value as Key.
func (p *Path) Index(index int) *Path {
	return &Path{index: strconv.Itoa(index), parent: p}
}

// Key indicates that the previous Path is to be subscripted by a string.
// This sets the same underlying value as Index.
func (p *Path) Key(key string) *Path {
	return &Path{index: key, parent: p}
}

// String produces a string representation of the Path.
func (p *Path) String() string {
	// make a slice to iterate
	elems := []*Path{}
	for ; p != nil; p = p.parent {
		elems = append(elems, p)
	}

	// iterate, but it has to be backwards
	buf := bytes.NewBuffer(nil)
	for i := range elems {
		p := elems[len(elems)-1-i]
		if p.parent != nil && len(p.name) > 0 {
			// This is either the root or it is a subscript.
			buf.WriteString(".")
		}
		if len(p.name) > 0 {
			buf.WriteString(p.name)
		} else {
			fmt.Fprintf(buf, "[%s]", p.index)
		}
	}
	return buf.String()
}
