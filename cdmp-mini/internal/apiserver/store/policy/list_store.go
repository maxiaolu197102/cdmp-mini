package policy

import (
	"context"
	"strings"

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

func (p *Policy) CountByUsernames(ctx context.Context, usernames []string) (map[string]int64, error) {
	result := make(map[string]int64, len(usernames))
	if p == nil || p.Db == nil {
		return result, nil
	}
	unique := make([]string, 0, len(usernames))
	seen := make(map[string]struct{}, len(usernames))
	for _, name := range usernames {
		clean := strings.TrimSpace(name)
		if clean == "" {
			continue
		}
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		unique = append(unique, clean)
	}
	if len(unique) == 0 {
		return result, nil
	}
	type countRow struct {
		Username string
		Total    int64
	}
	rows := make([]countRow, 0, len(unique))
	query := p.Db.WithContext(ctx).
		Model(&v1.Policy{}).
		Select("username, COUNT(*) as total").
		Where("username IN ?", unique).
		Group("username")
	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}
	for _, row := range rows {
		result[row.Username] = row.Total
	}
	for name := range seen {
		if _, ok := result[name]; !ok {
			result[name] = 0
		}
	}
	return result, nil
}
