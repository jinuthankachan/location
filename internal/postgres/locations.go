package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// Location represents a geographical entity (country, state, etc.)
// Location.Id is also referenced to as geo_id.
type Location struct {
	BaseModel
	GeoLevelID uuid.UUID `gorm:"type:uuid;not null" json:"geo_level_id"`
	GeoLevel   *GeoLevel `gorm:"foreignKey:GeoLevelID;references:Id" json:"geo_level"`
}

// TableName returns the table name for the Location model
func (Location) TableName() string {
	return "locations"
}

// InsertLocation inserts a new location
// It also inserts the primary name of the location
func (s *Store) InsertLocation(ctx context.Context, geoLevelID uuid.UUID, name string) (*Location, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// GetLocation returns a location by its id
func (s *Store) GetLocation(ctx context.Context, id uuid.UUID) (*Location, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// GetLocationsByPattern returns a location by its name
// It searches the name map by the pattern and returns the matches
func (s *Store) GetLocationsByPattern(ctx context.Context, name string, geoLevelID *uuid.UUID) ([]Location, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// UpdateLocation updates a location
func (s *Store) UpdateLocation(ctx context.Context, location *Location) (*Location, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// DeleteLocation deletes a location by its id
func (s *Store) DeleteLocation(ctx context.Context, id uuid.UUID) error {
	// TODO: Implement
	return fmt.Errorf("not implemented")
}
