package postgres

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// BaseModel is intended to be embedded in other models for common fields
// GORM annotations included for DB mapping
type BaseModel struct {
	Id        uuid.UUID      `gorm:"type:uuid;default:uuid_generate_v4();primaryKey" json:"id"`
	CreatedAt time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deleted_at"` // Updated to use gorm.DeletedAt for soft deletes
}
