package postgres

import (
	"context"
	"fmt"
)

type GeoLevel struct {
	BaseModel
	Name string   `gorm:"type:varchar(64);unique;not null" json:"name"`
	Rank *float64 `gorm:"type:float" json:"rank"`
}

// TableName returns the table name for the GeoLevel model
func (GeoLevel) TableName() string {
	return "geo_levels"
}

// InsertGeoLevel inserts a new geo level
func (s *Store) InsertGeoLevel(ctx context.Context, name string, rank *float64) (*GeoLevel, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// GetGeoLevelByPattern returns a geo level by its name
func (s *Store) GetGeoLevelByPattern(ctx context.Context, name string) (*GeoLevel, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// UpdateGeoLevel updates a geo level by its name
func (s *Store) UpdateGeoLevel(ctx context.Context, name string, newName string, newRank *float64) (*GeoLevel, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// DeleteGeoLevel deletes a geo level by its name
func (s *Store) DeleteGeoLevel(ctx context.Context, name string) error {
	// TODO: Implement
	return fmt.Errorf("not implemented")
}
