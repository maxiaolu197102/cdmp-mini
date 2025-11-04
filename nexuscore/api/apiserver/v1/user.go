/*
v1 包定义了用户（User）的数据模型，包含用户的基本信息（如用户名、密码、邮箱等）及相关方法。该模型既作为 RESTful API 的数据交换格式，也作为 GORM 的数据库映射模型，实现了 “API 层与存储层数据结构统一”。同时提供了密码验证、数据库操作钩子等辅助功能，支撑用户相关的核心业务逻辑。
核心流程
该包的核心是 User 结构体的定义及配套方法，主要流程如下：
数据模型定义：User 结构体通过字段和标签（json、gorm、validate）定义了用户的属性（如 Nickname、Email）、JSON 序列化规则、数据库表映射关系及参数校验规则。
辅助方法实现：
TableName() 指定数据库表名（user），实现 GORM 模型的表映射；
Compare() 用于验证输入密码与存储的加密密码是否一致；
AfterCreate() 作为 GORM 钩子函数，在用户记录创建后自动生成并更新实例 ID（InstanceID）。
列表模型支持：UserList 结构体用于封装用户列表数据，包含分页元信息（ListMeta）和用户项列表（Items），适配 API 分页查询场景。
*/

package v1

import (
	"fmt"
	"time"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/auth"           // 自定义库：密码加密与验证工具
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1" // 自定义库：元数据（如对象元信息、列表元信息）
	// 自定义库：实例ID生成工具
	// 第三方库：GORM ORM 框架
)

// User 表示用户资源，同时作为 GORM 数据库模型（映射到数据库表）。
type User struct {
	// 未来可能添加 TypeMeta（类型元信息）
	// metav1.TypeMeta `json:",inline"`

	// 标准对象的元数据（如 ID、创建时间、更新时间等）
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Status int `json:"status" gorm:"column:status" validate:"omitempty"` // 用户状态（如启用/禁用）

	Nickname string `json:"nickname" gorm:"column:nickname" validate:"omitempty,min=1,max=30"` // 用户昵称（1-30字符）

	// Required: true（必填项）
	Password string `json:"password,omitempty" gorm:"column:password" validate:"required"` // 加密存储的密码（JSON 序列化时忽略）

	// Required: true（必填项）
	Email string `json:"email" gorm:"column:email" validate:"required,email,min=1,max=100"` // 邮箱（需符合邮箱格式，1-100字符）

	Phone string `json:"phone" gorm:"column:phone" validate:"omitempty"` // 手机号（可选）

	IsAdmin int `json:"isAdmin,omitempty" gorm:"column:isAdmin" validate:"omitempty"` // 是否为管理员（0：否，1：是）

	TotalPolicy int64  `json:"totalPolicy" gorm:"-" validate:"omitempty"` // 关联的策略总数（不映射到数据库字段）
	Role        string `json:"role" gorm:"-"`                             // 关联的策略总数（不映射到数据库字段）

	LoginedAt time.Time `json:"loginedAt,omitempty" gorm:"column:loginedAt"` // 最后登录时间

	Command         UserUpdateCommand `json:"command,omitempty" gorm:"-"`
	ExpectedVersion *uint64           `json:"version,omitempty" gorm:"-"`
	Patch           *UserPatchSpec    `json:"updates,omitempty" gorm:"-"`
	Conditions      UserConditions    `json:"conditions,omitempty" gorm:"-"`
}

// UserList 表示存储中所有用户的列表，用于 API 分页返回。
type UserList struct {
	// 未来可能添加 TypeMeta（类型元信息）
	// metav1.TypeMeta `json:",inline"`

	// 标准列表元数据（如总条数、分页信息等）
	// +optional（可选）
	metav1.ListMeta `json:",inline"`

	Items []*User `json:"items"` // 用户列表项
}

// TableName 指定当前模型映射到 MySQL 数据库的表名。
func (u *User) TableName() string {
	return "user" // 映射到 "user" 表
}

// Compare 比较输入的明文密码与当前用户存储的加密密码是否一致。
// 若一致返回 nil，否则返回错误。
func (u *User) Compare(pwd string) error {
	if err := auth.Compare(u.Password, pwd); err != nil {
		return fmt.Errorf("密码验证失败: %w", err)
	}

	return nil
}

// AfterCreate 是 GORM 的钩子函数，在用户记录创建后执行。
// 作用：生成并更新用户的实例 ID（InstanceID）。
// func (u *User) AfterCreate(tx *gorm.DB) error {
// 	// 基于用户 ID 生成实例 ID（格式如 "user-xxx"）
// 	u.InstanceID = idutil.GetInstanceID(u.ID, "user-")

// 	// 保存更新后的实例 ID 到数据库
// 	return tx.Save(u).Error
// }

// 辅助函数：过滤敏感字段，返回前端可展示的用户信息
func ConvertToPublicUser(rawUser *User) *PublicUser {
	return &PublicUser{
		Metadata:  rawUser.ObjectMeta,
		ID:        rawUser.ID,
		Username:  rawUser.Name,
		Nickname:  rawUser.Nickname,
		Email:     rawUser.Email,
		Phone:     rawUser.Phone,
		IsAdmin:   rawUser.IsAdmin,
		CreatedAt: rawUser.CreatedAt,
		UpdatedAt: rawUser.UpdatedAt,
		// 刻意排除敏感字段：PasswordHash、Token、Secret等
	}
}

type PublicUser struct {
	Metadata  metav1.ObjectMeta `json:"metadata"`
	ID        uint64            `json:"id"`
	Username  string            `json:"username"`
	Nickname  string            `json:"nickname"`
	Email     string            `json:"email"`
	Phone     string            `json:"phone"`
	IsAdmin   int               `json:"isAdmin"`
	CreatedAt time.Time         `json:"createdAt"`
	UpdatedAt time.Time         `json:"updatedAt"`
	Role      string            `json:"role"`
}
