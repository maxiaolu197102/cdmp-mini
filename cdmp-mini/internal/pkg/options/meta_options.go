package options

import (
	v1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
	"github.com/spf13/pflag"
)

type MetaOptions struct {
	ListOptions   *v1.ListOptions
	UpdateOptions *v1.UpdateOptions
	GetOptions    *v1.GetOptions
	DeleteOptions *v1.DeleteOptions
	CreateOptions *v1.CreateOptions
}

func NewMetaOptions() *MetaOptions {
	timeout := int64(60)
	offset := int64(100)
	limit := int64(10000)
	return &MetaOptions{
		ListOptions: &v1.ListOptions{
			TimeoutSeconds: &timeout,
			Offset:         &offset,
			Limit:          &limit,
			TypeMeta: v1.TypeMeta{
				APIVersion: "v1",
			},
		},
		GetOptions: &v1.GetOptions{
			TypeMeta: v1.TypeMeta{
				APIVersion: "v1",
			},
		},
		UpdateOptions: &v1.UpdateOptions{
			TypeMeta: v1.TypeMeta{
				APIVersion: "v1",
			},
		},
		DeleteOptions: &v1.DeleteOptions{
			TypeMeta: v1.TypeMeta{
				APIVersion: "v1",
			},
		},
		CreateOptions: &v1.CreateOptions{
			TypeMeta: v1.TypeMeta{
				APIVersion: "v1",
			},
		},
	}
}

// Complete 填充默认值并验证选项，返回错误信息
func (o *MetaOptions) Complete() error {
	if o == nil {
		o = &MetaOptions{}
	}

	// 确保各选项对象不为nil
	if o.ListOptions == nil {
		o.ListOptions = &v1.ListOptions{}
	}
	if o.UpdateOptions == nil {
		o.UpdateOptions = &v1.UpdateOptions{}
	}
	if o.GetOptions == nil {
		o.GetOptions = &v1.GetOptions{}
	}
	if o.DeleteOptions == nil {
		o.DeleteOptions = &v1.DeleteOptions{}
	}
	if o.CreateOptions == nil {
		o.CreateOptions = &v1.CreateOptions{}
	}

	// 使用指定的默认值
	timeout := int64(60)
	offset := int64(100)
	limit := int64(10000)

	// 完成 ListOptions 配置
	if o.ListOptions.TimeoutSeconds == nil {
		o.ListOptions.TimeoutSeconds = &timeout
	}
	if o.ListOptions.Offset == nil {
		o.ListOptions.Offset = &offset
	}
	if o.ListOptions.Limit == nil {
		o.ListOptions.Limit = &limit
	}

	// 确保 TypeMeta 有合理值
	if o.ListOptions.APIVersion == "" {
		o.ListOptions.APIVersion = "v1"
	}

	if o.UpdateOptions.APIVersion == "" {
		o.UpdateOptions.APIVersion = "v1"
	}

	// 确保 GetOptions TypeMeta 有合理值
	if o.GetOptions.APIVersion == "" {
		o.GetOptions.APIVersion = "v1"
	}

	// 确保 DeleteOptions TypeMeta 有合理值
	if o.DeleteOptions.APIVersion == "" {
		o.DeleteOptions.APIVersion = "v1"
	}

	if o.CreateOptions.APIVersion == "" {
		o.CreateOptions.APIVersion = "v1"
	}

	return nil
}

func (o *MetaOptions) Validate() []error {
	if o == nil {
		return []error{errors.New("MetaOptions 不能为空")}
	}

	var errs []error
	// 验证 TimeoutSeconds
	if o.ListOptions.TimeoutSeconds != nil && *o.ListOptions.TimeoutSeconds < 0 {
		errs = append(errs, errors.New("超时时间不能为负数"))
	}

	// 验证 Offset
	if o.ListOptions.Offset != nil && *o.ListOptions.Offset < 0 {
		errs = append(errs, errors.New("分页偏移量不能为负数"))
	}

	// 验证 Limit
	if o.ListOptions.Limit != nil {
		if *o.ListOptions.Limit < 0 {
			errs = append(errs, errors.New("每页条数不能为负数"))
		}
		if *o.ListOptions.Limit > 10000 {
			errs = append(errs, errors.New("每页条数不能超过10000条"))
		}
	}

	// 验证 DryRun 参数
	if o.UpdateOptions.DryRun != nil {
		if len(o.UpdateOptions.DryRun) != 1 || o.UpdateOptions.DryRun[0] != "All" {
			errs = append(errs, errors.New("UpdateOptions.DryRun 必须为 nil 或精确的 [\"All\"]"))
		}
	}

	if o.CreateOptions.DryRun != nil {
		if len(o.CreateOptions.DryRun) != 1 || o.CreateOptions.DryRun[0] != "All" {
			errs = append(errs, errors.New("CreateOptions.DryRun 必须为 nil 或精确的 [\"All\"]"))
		}
	}

	return errs
}

// AddFlags 将 MetaOptions 的参数添加到命令行标志集中
func (o *MetaOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil || fs == nil {
		return
	}

	// 确保指针字段已经初始化
	if o.ListOptions.TimeoutSeconds == nil {
		timeout := int64(60)
		o.ListOptions.TimeoutSeconds = &timeout
	}
	if o.ListOptions.Offset == nil {
		offset := int64(100)
		o.ListOptions.Offset = &offset
	}
	if o.ListOptions.Limit == nil {
		limit := int64(10000)
		o.ListOptions.Limit = &limit
	}

	// ListOptions 相关标志
	fs.Int64Var(o.ListOptions.TimeoutSeconds, "meta.timeout", *o.ListOptions.TimeoutSeconds, "查询超时时间（秒），0表示不超时")
	fs.Int64Var(o.ListOptions.Offset, "meta.offset", *o.ListOptions.Offset, "分页偏移量，跳过前N条记录")
	fs.Int64Var(o.ListOptions.Limit, "meta.limit", *o.ListOptions.Limit, "每页条数限制，最大10000条")
	fs.StringVar(&o.ListOptions.LabelSelector, "meta.selector", "", "标签选择器，用于筛选资源")
	fs.StringVar(&o.ListOptions.FieldSelector, "meta.field-selector", "", "字段选择器，用于按字段筛选资源")

	// UpdateOptions 相关标志
	fs.StringSliceVar(&o.UpdateOptions.DryRun, "meta.update-dry-run", nil, "更新操作的试运行模式，值为 All 开启试运行")

	// CreateOptions 相关标志
	fs.StringSliceVar(&o.CreateOptions.DryRun, "meta.create-dry-run", nil, "创建操作的试运行模式，值为 All 开启试运行")

	// DeleteOptions 相关标志
	fs.BoolVar(&o.DeleteOptions.Unscoped, "meta.unscoped", false, "是否不使用默认作用域（如包含软删除的记录）")

	// TypeMeta 相关标志（可选）
	fs.StringVar(&o.ListOptions.Kind, "meta.kind", "", "资源类型，如 Deployment、Pod、Service 等")
	fs.StringVar(&o.ListOptions.APIVersion, "meta.api-version", "v1", "API版本")
}
