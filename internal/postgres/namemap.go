package postgres

import (
	"github.com/google/uuid"
)

// NameMap represents names (including alternate names) for a location
type NameMap struct {
	BaseModel
	Name       string    `gorm:"type:varchar(128);not null" json:"name"`
	LocationID uuid.UUID `gorm:"type:uuid;not null;index" json:"location_id"`
	Primary    bool      `gorm:"not null;default:false" json:"primary"`
	Location   *Location `gorm:"foreignKey:LocationID;references:Id" json:"location"`
}
