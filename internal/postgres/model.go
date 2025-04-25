package postgres

import (
	"time"

	"github.com/google/uuid"
)

// BaseModel is intended to be embedded in other models for common fields
// GORM annotations included for DB mapping
type BaseModel struct {
	Id        uuid.UUID `gorm:"type:uuid;default:uuid_generate_v4();primaryKey" json:"id"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
	DeletedAt time.Time `gorm:"autoDeleteTime" json:"deleted_at"`
}
