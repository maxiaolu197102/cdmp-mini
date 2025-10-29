package secret

import (
	"gorm.io/gorm"
)

type Secret struct {
	Db *gorm.DB
}
