package policy

import (
	"gorm.io/gorm"
)

type Policy struct {
	Db *gorm.DB
}
