/*
包摘要
v1 包定义了通用的元数据结构（如对象元信息、列表元信息）、扩展字段类型（Extend）以及 API 操作选项（如创建、更新、删除选项）。这些结构是整个系统中资源（如用户、策略等）的基础框架，统一了数据的元信息格式、扩展字段处理方式和 API 交互的参数规范，确保各模块之间的数据交互一致性。
核心流程
该包的核心是提供通用的数据结构和处理逻辑，支撑上层资源的定义和 API 交互，主要流程如下：
扩展字段处理：通过 Extend 类型（基于 map[string]interface{}）支持资源的动态扩展字段，提供序列化、合并（从数据库影子字段恢复）等方法，解决固定字段无法满足灵活业务需求的问题。
元数据定义：
TypeMeta：标识资源的类型和 API 版本，用于序列化和反序列化时的类型识别；
ObjectMeta：所有持久化资源的基础元信息（如 ID、实例 ID、创建 / 更新时间等），并通过 GORM 钩子函数（BeforeCreate、BeforeUpdate 等）自动处理扩展字段的数据库存储（序列化到 ExtendShadow 字段）；
ListMeta：列表数据的元信息（如总条数），支撑分页查询场景。
API 操作选项：定义 CreateOptions、UpdateOptions 等结构，规范 API 操作的参数（如是否 dry run、强制更新等），统一各资源的操作行为。
中文注释代码（注释汉化）
*/

package v1

import (
	"time"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/json" // 自定义库：JSON 序列化工具
	"gorm.io/gorm"                                                 // 第三方库：GORM ORM 框架
)

// Extend 定义了一种新类型，用于存储扩展字段（动态字段）。
type Extend map[string]interface{}

// String 返回 Extend 的字符串格式（JSON 序列化结果）。
func (ext Extend) String() string {
	data, _ := json.Marshal(ext)
	return string(data)
}

// Merge 从 extendShadow（数据库中存储的扩展字段影子字符串）合并扩展字段。
// 优先保留数据库中的影子字段，若当前扩展字段中无对应键，则从影子字段中补充。
func (ext Extend) Merge(extendShadow string) Extend {
	var extend Extend

	// 始终信任数据库中的 extendShadow
	_ = json.Unmarshal([]byte(extendShadow), &extend)
	for k, v := range extend {
		if _, ok := ext[k]; !ok { // 若当前扩展字段中无此键，则从影子字段添加
			ext[k] = v
		}
	}

	return ext
}

// TypeMeta 描述 API 请求或响应中的单个对象，包含表示对象类型和 API 模式版本的字符串。
// 带版本或需要持久化的结构应嵌入 TypeMeta。
type TypeMeta struct {
	// Kind 是表示此对象所代表的 REST 资源的字符串值。
	// 服务器可从客户端提交请求的端点推断此值。
	// 不可更新。
	// 采用驼峰命名法。
	// required: false（非必填）
	Kind string `json:"kind,omitempty"`

	// APIVersion 定义此对象表示的版本化模式。
	// 服务器应将识别的模式转换为最新的内部值，并可能拒绝未识别的值。
	APIVersion string `json:"apiVersion,omitempty"`
}

// ListMeta 描述合成资源（包括列表和各种状态对象）必须具有的元数据。
// 一个资源只能包含 {ObjectMeta, ListMeta} 中的一个。
type ListMeta struct {
	TotalCount int64 `json:"totalCount,omitempty"` // 总记录数（用于分页）

}

// IdentifierFilter 描述用于唯一定位资源的标识过滤器。
type IdentifierFilter struct {
	ID         *uint64 `json:"id,omitempty"`
	InstanceID string  `json:"instanceID,omitempty"`
	Name       string  `json:"name,omitempty"`
}

// StatusFilter 描述与资源状态相关的筛选条件。
type StatusFilter struct {
	Statuses []int `json:"statuses,omitempty"`
	IsAdmin  *bool `json:"isAdmin,omitempty"`
}

// ContactFilter 描述与联系信息相关的筛选条件。
type ContactFilter struct {
	Email     string `json:"email,omitempty"`
	EmailLike string `json:"emailLike,omitempty"`
	Phone     string `json:"phone,omitempty"`
	PhoneLike string `json:"phoneLike,omitempty"`
}

// TimeRangeFilter 描述资源的时间范围筛选条件。
type TimeRangeFilter struct {
	CreatedAtGTE *time.Time `json:"createdAtGte,omitempty"`
	CreatedAtLTE *time.Time `json:"createdAtLte,omitempty"`
	UpdatedAtGT  *time.Time `json:"updatedAtGt,omitempty"`
	UpdatedAtLT  *time.Time `json:"updatedAtLt,omitempty"`
	LoginedAtLT  *time.Time `json:"loginedAtLt,omitempty"`
}

// ExtendFilter 描述扩展字段（JSON）相关的筛选条件。
type ExtendFilter struct {
	Equals   map[string]string   `json:"equals,omitempty"`
	Contains map[string]string   `json:"contains,omitempty"`
	In       map[string][]string `json:"in,omitempty"`
}

// ObjectMeta 是所有持久化资源必须具有的元数据，包含所有对象的通用属性。
// 同时被 GORM 用作数据库模型的基础元信息。
type ObjectMeta struct {
	// ID 是此对象在时间和空间上的唯一值。通常由存储在资源创建成功时生成，
	// 且在 PUT 操作中不允许更改。
	//
	// 由系统生成。
	// 只读。
	ID uint64 `json:"id,omitempty" gorm:"primary_key;AUTO_INCREMENT;column:id"` // 数据库主键（自增）

	// InstanceID 定义字符串类型的资源标识符，使用前缀区分资源类型，便于记忆和 URL 友好。
	InstanceID string `json:"instanceID,omitempty" gorm:"unique;column:instanceID;type:varchar(32);not null"` // 唯一实例 ID

	// Name 在每个命名空间内必须唯一。
	// 并非所有对象都需要限定到用户名 - 这些对象的此字段值将为空。
	//
	// 必须符合 DNS_LABEL 格式。
	// 不可更新。
	// （注：原注释中的 Username 字段已注释，此处保留说明）

	// Required: true（必填）
	// Name 必须唯一。创建资源时必填。
	// Name 主要用于创建幂等性和配置定义。
	// 仅当未指定 Name 时，才会自动生成。
	// 不可更新。
	Name string `json:"name,omitempty" gorm:"column:name;type:varchar(64);not null" validate:"name"` // 资源名称（唯一）

	// Extend 存储需要添加但不想新增表列的字段，不会存储在数据库中。
	Extend Extend `json:"extend,omitempty" gorm:"-" validate:"omitempty"` // 扩展字段（内存中）

	// ExtendShadow 是 Extend 的影子字段。请勿直接修改。
	// 用于将 Extend 序列化后存储到数据库（GORM 映射字段）。
	ExtendShadow string `json:"-" gorm:"column:extendShadow" validate:"omitempty"` // 扩展字段的数据库存储（JSON 字符串）

	// CreatedAt 是表示此对象在服务器上创建时的时间戳。
	// 不保证在不同操作之间按 happens-before 顺序设置。
	// 客户端不得设置此值。以 RFC3339 格式表示，且位于 UTC 时区。
	//
	// 由系统生成。
	// 只读。
	// 列表中为 null。
	CreatedAt time.Time `json:"createdAt,omitempty" gorm:"column:createdAt"` // 创建时间

	// UpdatedAt 是表示此对象在服务器上更新时的时间戳。
	// 客户端不得设置此值。以 RFC3339 格式表示，且位于 UTC 时区。
	//
	// 由系统生成。
	// 只读。
	// 列表中为 null。
	UpdatedAt time.Time `json:"updatedAt,omitempty" gorm:"column:updatedAt"` // 更新时间

	// Version 是资源的乐观锁版本号，每次成功更新后递增。
	// 客户端在执行需要并发控制的操作时必须携带当前版本号。
	Version uint64 `json:"version,omitempty" gorm:"column:version;type:bigint unsigned;not null;default:1"` // 乐观锁版本

	// DeletedAt 是此资源将被删除的 RFC 3339 日期和时间。
	// 此字段由服务器在用户请求优雅删除时设置，客户端不能直接设置。
	//
	// 当请求优雅删除时由系统生成。
	// 只读。
	// （注：原字段已注释，此处保留说明）
	// DeletedAt gorm.DeletedAt `json:"-" gorm:"column:deletedAt;index:idx_deletedAt"`
}

// BeforeCreate 是 GORM 的钩子函数，在数据库记录创建前执行。
// 作用：将 Extend 序列化为字符串，存储到 ExtendShadow 字段（用于数据库存储）。
func (obj *ObjectMeta) BeforeCreate(tx *gorm.DB) error {
	if obj.Version == 0 {
		obj.Version = 1
	}
	obj.ExtendShadow = obj.Extend.String()
	return nil
}

// BeforeUpdate 是 GORM 的钩子函数，在数据库记录更新前执行。
// 作用：同 BeforeCreate，更新 ExtendShadow 以同步 Extend 的最新状态。
func (obj *ObjectMeta) BeforeUpdate(tx *gorm.DB) error {
	obj.ExtendShadow = obj.Extend.String()
	return nil
}

// AfterFind 是 GORM 的钩子函数，在查询数据库记录后执行。
// 作用：将数据库中的 ExtendShadow 反序列化为 Extend（内存中可用的扩展字段）。
func (obj *ObjectMeta) AfterFind(tx *gorm.DB) error {
	if err := json.Unmarshal([]byte(obj.ExtendShadow), &obj.Extend); err != nil {
		return err
	}
	return nil
}

// ListOptions 是标准 REST 列表查询的选项参数。
/*
ListOptions 是一个用于描述 “标准 REST 接口列表查询时可附加的选项参数” 的结构体，主要作用是对列表查询结果进行过滤、分页、设置超时等控制，让调用方能够按需获取数据。具体含义如下：
1. 整体作用
当通过 REST API 接口查询资源列表（如查询用户列表、订单列表等）时，调用方可以传递 ListOptions 结构体作为参数，用来精确控制返回的结果范围、数量、过滤条件等。它相当于给列表查询提供了一套 “筛选和分页工具”。
2. 各字段解析
TypeMeta 嵌入字段
通过 json:",inline" 嵌入 TypeMeta 结构体（包含 Kind 和 APIVersion），用于标识当前查询选项对应的资源类型和 API 版本。例如：
Kind: "User" 表示该选项用于查询 “用户” 资源列表；
APIVersion: "v1" 表示遵循 v1 版本的 API 规范。
这确保了查询选项与目标资源的匹配，避免参数用错场景。
LabelSelector 字段
作用：通过 “标签选择器” 过滤符合条件的资源。
说明：许多资源会被打上标签（如 env=prod、app=payment），LabelSelector 允许通过标签规则筛选结果。例如：
LabelSelector: "env=prod,app=payment" 表示只返回标签为 “环境 = 生产” 且 “应用 = 支付” 的资源。
格式：通常支持等式（key=value）、不等（key!=value）或集合（key in (v1,v2)）等规则（具体取决于 API 实现）。
FieldSelector 字段
作用：通过 “字段值” 过滤符合条件的资源。
说明：与 LabelSelector 基于标签不同，FieldSelector 直接根据资源自身的字段进行筛选。例如：
对于用户资源，FieldSelector: "status=active,age>18" 可能表示只返回 “状态为活跃” 且 “年龄大于 18” 的用户（具体字段规则取决于 API 定义）。
默认：若不指定，默认返回所有符合条件的资源（无字段过滤）。
TimeoutSeconds 字段
作用：指定查询请求的超时时间（单位：秒）。
说明：是一个指针类型（*int64），允许为 nil（不设置超时）。若设置，当查询耗时超过该值时，API 可能会终止请求并返回超时错误，避免长时间阻塞。
示例：TimeoutSeconds: 30 表示查询最多等待 30 秒，超时则失败。
Offset 字段
作用：指定分页查询时的 “偏移量”（即跳过前 N 条记录）。
说明：配合 Limit 实现分页。例如，Offset: 10 表示跳过前 10 条记录，从第 11 条开始返回。
用途：常用于 “上一页 / 下一页” 场景，通过调整 Offset 切换分页位置。
Limit 字段
作用：指定分页查询时 “每页返回的最大记录数”。
说明：控制单次查询返回的结果数量，避免数据量过大导致响应缓慢。例如，Limit: 20 表示每页最多返回 20 条记录。
配合 Offset 使用：Offset: 20, Limit: 10 表示返回第 21-30 条记录（跳过前 20 条，取接下来的 10 条）。
3. 使用场景举例
假设查询 “用户列表” 时传递以下 ListOptions：
go
ListOptions{
    LabelSelector:  "department=tech",  // 只看“部门=技术部”的标签
    FieldSelector:  "role=developer",   // 只看“角色=开发者”的字段
    Offset:         20,                 // 跳过前 20 条
    Limit:          10,                 // 每页返回 10 条
    TimeoutSeconds: 10,                 // 超时 10 秒
}

则 API 会返回：技术部标签 + 开发者角色的用户中，第 21-30 条记录，且查询最多等待 10 秒。
总结
ListOptions 是 REST 列表查询的 “控制中心”，通过标签筛选、字段筛选、分页（偏移量 + 每页条数）、超时设置等，让调用方能够高效、精确地获取所需的资源列表。

*/
type ListOptions struct {
	TypeMeta `json:",inline"` // 嵌入类型元信息

	// LabelSelector 用于查找匹配的 REST 资源。
	LabelSelector string `json:"labelSelector,omitempty" form:"labelSelector"`

	// FieldSelector 按字段限制返回的对象列表。默认为返回所有。
	FieldSelector string `json:"fieldSelector,omitempty" form:"fieldSelector"`

	// TimeoutSeconds 指定 ClientIP 类型会话的粘性时间（秒）。
	TimeoutSeconds *int64 `json:"timeoutSeconds,omitempty"`

	// Offset 指定开始返回记录前要跳过的记录数（分页偏移量）。
	Offset *int64 `json:"offset,omitempty" form:"offset"`

	// Limit 指定要检索的记录数（每页条数）。
	Limit *int64 `json:"limit,omitempty" form:"limit"`

	// Identifiers 用于唯一标识资源的精确匹配过滤条件。
	Identifiers IdentifierFilter `json:"identifiers,omitempty" form:"-"`

	// Status 用于描述资源状态维度的过滤条件。
	Status StatusFilter `json:"statusFilter,omitempty" form:"-"`

	// Contact 用于描述资源联系信息的过滤条件。
	Contact ContactFilter `json:"contactFilter,omitempty" form:"-"`

	// Time 用于描述时间范围的过滤条件。
	Time TimeRangeFilter `json:"timeFilter,omitempty" form:"-"`

	// Extend 用于描述扩展字段（JSON）的过滤条件。
	Extend ExtendFilter `json:"extendFilter,omitempty" form:"-"`
}

// ExportOptions 是标准 REST 获取调用的查询选项。
// 已废弃。计划在 1.18 版本中移除。
type ExportOptions struct {
	TypeMeta `json:",inline"` // 嵌入类型元信息

	// 此值是否应导出。导出会剥离用户无法指定的字段。
	// 已废弃。计划在 1.18 版本中移除。
	Export bool `json:"export"`
	// 导出是否应精确。精确导出保留集群特定字段（如 'Namespace'）。
	// 已废弃。计划在 1.18 版本中移除。
	Exact bool `json:"exact"`
}

// GetOptions 是标准 REST 获取调用的查询选项。
type GetOptions struct {
	TypeMeta `json:",inline"` // 嵌入类型元信息
}

// DeleteOptions 可在删除 API 对象时提供。
/*
1. 整体作用
当通过 API 接口删除资源（如用户、配置项等）时，调用方可以传递 DeleteOptions 结构体作为参数，用来调整删除操作的执行逻辑。它相当于给删除操作提供了一个 “行为开关”，允许根据需求改变删除的范围或方式。
2. 核心字段解析
TypeMeta 嵌入字段
通过 json:",inline" 嵌入 TypeMeta 结构体（包含 Kind 和 APIVersion），用于标识当前选项对应的资源类型和 API 版本。例如：
Kind: "User" 表示该选项用于删除 “用户” 资源；
APIVersion: "v1" 表示遵循 v1 版本的 API 规范。
这确保了选项与目标资源的匹配，避免参数用错场景。
Unscoped 字段（核心功能）
Unscoped 是一个布尔值，用于控制删除操作是否 “忽略默认作用域”，具体逻辑与系统的 “数据过滤机制” 相关：
默认情况（Unscoped: false）：删除操作会遵循系统的默认作用域规则。例如，若系统对资源启用了 “软删除”（即删除时不直接物理删除，而是标记为 “已删除” 状态），默认作用域可能会自动过滤掉已标记的软删除记录，此时删除操作仅针对 “未被软删除” 的记录生效。
开启 Unscoped: true：删除操作会忽略默认作用域，直接对所有符合条件的记录生效（包括已被软删除的记录）。这可能意味着执行 “物理删除” 或对原本被过滤的记录执行操作。
简单来说，Unscoped 决定了删除操作是否 “突破默认的数据过滤规则”，直接操作所有匹配的资源（包括通常被隐藏的记录）。
3. 使用场景举例
假设系统中 “用户” 资源支持软删除（删除后记录保留但标记为 deleted: true），默认删除操作仅处理未被标记的用户：
传递 Unscoped: false（或不传递）：删除操作只针对 “未被软删除” 的用户；
传递 Unscoped: true：删除操作会忽略 “软删除标记”，直接处理所有匹配的用户（可能包括物理删除已软删除的记录）。
*/
type DeleteOptions struct {
	TypeMeta `json:",inline"` // 嵌入类型元信息

	// +optional（可选）
	Unscoped bool `json:"unscoped"` // 是否不使用默认作用域（如不过滤软删除记录）
}

// CreateOptions 可在创建 API 对象时提供。
/*
TypeMeta 嵌入字段
通过 json:",inline" 嵌入 TypeMeta 结构体（包含 Kind 和 APIVersion），用于标识当前选项对应的资源类型和 API 版本。例如：
Kind: "User" 表示该选项用于创建 “用户” 资源；
APIVersion: "v1" 表示遵循 v1 版本的 API 规范。
这确保了选项与目标资源的匹配，避免参数用错场景。
DryRun 字段（核心功能）
DryRun 是一个字符串切片，用于开启 “干跑模式”（也叫 “模拟操作”），核心逻辑是：
正常模式：不传递 DryRun 时，创建操作会真实执行（如写入数据库）；
干跑模式：传递 DryRun: ["All"] 时，系统会验证操作的合法性（如参数是否合规、权限是否足够），但不会实际创建资源（不写入数据库），仅返回验证结果。
这类似于 “预览操作”，用于在实际执行前检查操作是否可行，避免误操作。
有效值：目前仅支持 "All"，表示执行所有干跑阶段的验证；
若传递无效值（如 "Test"），系统会返回错误，终止操作。
3. 使用场景举例
假设通过 API 创建用户时：
正常创建：不传递 DryRun，请求会真实创建用户并写入数据库；
验证创建可行性：传递 DryRun: ["All"]，系统会检查 “用户名是否重复”“邮箱格式是否正确” 等，但不会创建用户，仅返回 “操作是否可执行” 的结果。
*/
type CreateOptions struct {
	TypeMeta `json:",inline"` // 嵌入类型元信息

	// 当存在时，表示修改不应被持久化。无效或未识别的 dryRun 指令将
	// 导致错误响应，且请求不再进一步处理。有效值为：
	// - All：处理所有 dry run 阶段
	// +optional（可选）
	DryRun []string `json:"dryRun,omitempty"` // 干跑模式（不实际执行操作）
}

// PatchOptions 可在补丁更新 API 对象时提供。
// PatchOptions 旨在作为 UpdateOptions 的超集。
type PatchOptions struct {
	TypeMeta `json:",inline"` // 嵌入类型元信息

	// 当存在时，表示修改不应被持久化。无效或未识别的 dryRun 指令将
	// 导致错误响应，且请求不再进一步处理。有效值为：
	// - All：处理所有 dry run 阶段
	// +optional（可选）
	DryRun []string `json:"dryRun,omitempty"` // 干跑模式

	// Force 用于 "force" Apply 请求。表示用户将重新获取
	// 其他人拥有的冲突字段。非 apply 补丁请求必须 unset 此标志。
	// +optional（可选）
	Force bool `json:"force,omitempty"` // 强制更新（忽略冲突）
}

// UpdateOptions 可在更新 API 对象时提供。
// UpdateOptions 中的所有字段也应存在于 PatchOptions 中。
type UpdateOptions struct {
	TypeMeta `json:",inline"` // 嵌入类型元信息

	// 当存在时，表示修改不应被持久化。无效或未识别的 dryRun 指令将
	// 导致错误响应，且请求不再进一步处理。有效值为：
	// -空数组 [] 或 nil：关闭 DryRun 模式（实际执行操作）
	//非空数组 ["All"]：开启 DryRun 模式（只验证不执行
	DryRun []string `json:"dryRun,omitempty"` // 干跑模式
}

// AuthorizeOptions 可在授权 API 对象时提供。
type AuthorizeOptions struct {
	TypeMeta `json:",inline"` // 嵌入类型元信息
}

// TableOptions 用于调用者请求 Table 时。
type TableOptions struct {
	TypeMeta `json:",inline"` // 嵌入类型元信息

	// NoHeaders 仅对内部调用者开放。未包含在 OpenAPI 定义中，
	// 可能在未来版本中作为字段移除。
	NoHeaders bool `json:"-"` // 是否不返回表头
}
