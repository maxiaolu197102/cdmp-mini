package policyaudit

import (
	"context"

	"gorm.io/gorm"
)

type Policy_audit struct {
	Db *gorm.DB
}

func (p *Policy_audit) ClearOutdated(ctx context.Context, maxReserveDays int) (int64, error) {
	return 0, nil
}

