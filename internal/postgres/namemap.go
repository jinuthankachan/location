package postgres

import (
	"context"
	"fmt"

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

// TableName returns the table name for the NameMap model
func (NameMap) TableName() string {
	return "name_maps"
}

// InsertNameMap inserts a new name map
func (s *Store) InsertNameMap(ctx context.Context, name string, locationID uuid.UUID, primary bool) (*NameMap, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// GetLocationNames returns names for a location
func (s *Store) GetLocationNames(ctx context.Context, locationID uuid.UUID) ([]NameMap, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// GetNamesByPattern returns names by matching the pattern of the name.
func (s *Store) GetNamesByPattern(ctx context.Context, name string) ([]NameMap, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// DeleteNameMap deletes a name map by its name
func (s *Store) DeleteNameMap(ctx context.Context, name string) error {
	// TODO: Implement
	return fmt.Errorf("not implemented")
}
