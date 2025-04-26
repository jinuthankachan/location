package postgres

import (
	"errors"
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

// Common error definitions
var (
	ErrPrimaryNameExists   = errors.New("location already has a primary name")
	ErrInvalidHierarchy    = errors.New("parent rank must be lower than child rank")
	ErrDuplicateRelation   = errors.New("child already has a parent of this level")
	ErrLocationNotFound    = errors.New("location not found")
	ErrGeoLevelNotExist    = errors.New("geo level does not exist")
	ErrNameRequired        = errors.New("name is required")
	ErrNameAlreadyExists   = errors.New("name already exists for this location")
	ErrCannotDeletePrimary = errors.New("cannot delete primary name")
	ErrPrimaryNameNotFound = errors.New("primary name not found for location")
)
