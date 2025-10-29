package policy

import (
	"context"

	gormutil "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/util"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/fields"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

func (p *Policy) List(ctx context.Context, username string, opts metav1.ListOptions) (*v1.PolicyList, error) {
	ret := &v1.PolicyList{}
	ol := gormutil.Unpointer(opts.Offset, opts.Limit)

	// 构建基础查询
	query := p.Db.Model(&v1.Policy{})

	if username != "" {
		query = query.Where("username = ?", username)
	}

	if opts.FieldSelector != "" {
		selector, err := fields.ParseSelector(opts.FieldSelector)
		if err != nil {
			return nil, err
		}
		if name, exists := selector.RequiresExactMatch("name"); exists {
			query = query.Where("name LIKE ?", "%"+name+"%")
		}
	}

	// 正确的 Count 查询
	if err := query.Count(&ret.TotalCount).Error; err != nil {
		return nil, err
	}
	// SQL: SELECT COUNT(*) FROM policies WHERE username = ? AND name LIKE ?

	// 正确的 Find 查询
	if err := query.Offset(ol.Offset).
		Limit(ol.Limit).
		Order("id desc").
		Find(&ret.Items).Error; err != nil {
		return nil, err
	}
	// SQL: SELECT * FROM policies WHERE username = ? AND name LIKE ? ORDER BY id DESC LIMIT ? OFFSET ?

	return ret, nil

}
