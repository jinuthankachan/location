package postgres

import (
	"github.com/google/uuid"
)

// Location represents a geographical entity (country, state, etc.)
type Location struct {
	BaseModel
	GeoLevelID uuid.UUID `gorm:"type:uuid;not null" json:"geo_level_id"`
	GeoLevel   *GeoLevel `gorm:"foreignKey:GeoLevelID;references:Id" json:"geo_level"`
}
