package postgres

import (
	"github.com/google/uuid"
)

// Relation represents the hierarchical relationship between two locations
type Relation struct {
	BaseModel
	ParentLocationID uuid.UUID `gorm:"type:uuid;not null;index" json:"parent_location_id"`
	ChildLocationID  uuid.UUID `gorm:"type:uuid;not null;index" json:"child_location_id"`
	ParentLocation   *Location `gorm:"foreignKey:ParentLocationID;references:Id" json:"parent_location"`
	ChildLocation    *Location `gorm:"foreignKey:ChildLocationID;references:Id" json:"child_location"`
}
