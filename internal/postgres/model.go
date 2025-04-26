package postgres

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// BaseModel is intended to be embedded in other models for common fields
// GORM annotations included for DB mapping
// Removed default:uuid_generate_v4(); will set UUID in BeforeCreate hook
type BaseModel struct {
	Id        uuid.UUID      `gorm:"type:uuid;primaryKey" json:"id"`
	CreatedAt time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deleted_at"` // Updated to use gorm.DeletedAt for soft deletes
}

// BeforeCreate GORM hook to set UUID if not set
func (base *BaseModel) BeforeCreate(tx *gorm.DB) (err error) {
	if base.Id == uuid.Nil {
		base.Id = uuid.New()
	}
	return nil
}
