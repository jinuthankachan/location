package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Location represents an identifiable unit of a location
type Location struct {
	BaseModel
	GeoLevelID uuid.UUID `gorm:"type:uuid;not null;index" json:"geo_level_id"`
	GeoLevel   GeoLevel  `gorm:"foreignKey:GeoLevelID;references:Id;constraint:OnDelete:RESTRICT" json:"geo_level"`
}

// TableName returns the table name for the Location model
func (Location) TableName() string {
	return "locations"
}

// LocationFilter are the fiters to be used to fetch location
type LocationFilter struct {
	Ids      *uuid.UUIDs
	GeoLevel *string
	Name     *string
}

// LocationWithNames represents a location with its names for API responses
type LocationWithNames struct {
	Id       uuid.UUID `json:"geo_id"`
	GeoLevel string    `json:"geo_level"`
	Name     string    `json:"name"`    // Primary name
	Aliases  []string  `json:"aliases"` // Other names (non-primary)
}

// InsertLocation inserts a new location with its primary name
// It accepts the geo level name (not ID) for usability
func (s *Store) InsertLocation(ctx context.Context, geoLevelName string, name string) (*Location, error) {
	if name == "" {
		return nil, ErrNameRequired
	}

	var location *Location
	err := s.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Get the geo level by name
		var geoLevel GeoLevel
		if err := tx.Where("name = ?", strings.ToUpper(geoLevelName)).First(&geoLevel).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrGeoLevelNotExist
			}
			return err
		}

		// Create the location
		location = &Location{
			GeoLevelID: geoLevel.Id,
		}
		if err := tx.Create(location).Error; err != nil {
			return fmt.Errorf("failed to create location: %w", err)
		}

		// Create the primary name for the location
		nameMap := &NameMap{
			LocationID: location.Id,
			Name:       name,
			IsPrimary:  true,
		}
		if err := tx.Create(nameMap).Error; err != nil {
			return fmt.Errorf("failed to create primary name: %w", err)
		}

		// Load the geo level relationship
		if err := tx.Model(location).Association("GeoLevel").Find(&location.GeoLevel); err != nil {
			return fmt.Errorf("failed to load geo level: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return location, nil
}

// GetLocation returns a location by its id with names
func (s *Store) GetLocation(ctx context.Context, id uuid.UUID) (*LocationWithNames, error) {
	var location Location
	err := s.DB.WithContext(ctx).
		Preload("GeoLevel").
		First(&location, id).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrLocationNotFound
		}
		return nil, fmt.Errorf("failed to get location: %w", err)
	}

	// Get all names for the location
	var names []NameMap
	if err := s.DB.Where("location_id = ?", id).Find(&names).Error; err != nil {
		return nil, fmt.Errorf("failed to get location names: %w", err)
	}

	result := &LocationWithNames{
		Id:       location.Id,
		GeoLevel: location.GeoLevel.Name,
		Aliases:  make([]string, 0),
	}

	// Process names, separating primary and aliases
	for _, name := range names {
		if name.IsPrimary {
			result.Name = name.Name
		} else {
			result.Aliases = append(result.Aliases, name.Name)
		}
	}

	return result, nil
}

// SearchLocationsByPattern returns locations by matching name pattern
func (s *Store) SearchLocationsByPattern(ctx context.Context, pattern string, geoLevelID *uuid.UUID) ([]*LocationWithNames, error) {
	if pattern == "" {
		return nil, ErrNameRequired
	}

	// Find name maps matching the pattern
	query := s.DB.WithContext(ctx).
		Preload("Location.GeoLevel").
		Where("name LIKE ?", fmt.Sprintf("%%%s%%", pattern))

	// Filter by geo level if provided
	if geoLevelID != nil {
		query = query.Joins("JOIN locations ON locations.id = name_maps.location_id").
			Where("locations.geo_level_id = ?", *geoLevelID)
	}

	var nameMatches []NameMap
	if err := query.Find(&nameMatches).Error; err != nil {
		return nil, fmt.Errorf("failed to search locations: %w", err)
	}

	// Group names by location
	locationMap := make(map[uuid.UUID]*LocationWithNames)
	for _, name := range nameMatches {
		loc, exists := locationMap[name.LocationID]
		if !exists {
			loc = &LocationWithNames{
				Id:       name.Location.Id,
				GeoLevel: name.Location.GeoLevel.Name,
				Aliases:  make([]string, 0),
			}
			locationMap[name.LocationID] = loc
		}

		if name.IsPrimary {
			loc.Name = name.Name
		} else {
			loc.Aliases = append(loc.Aliases, name.Name)
		}
	}

	// Convert map to slice
	results := make([]*LocationWithNames, 0, len(locationMap))
	for _, loc := range locationMap {
		results = append(results, loc)
	}

	return results, nil
}

// UpdateLocation updates a location
func (s *Store) UpdateLocation(ctx context.Context, id uuid.UUID, geoLevelName *string, name *string) (*Location, error) {
	// TODO: implement
	return nil, fmt.Errorf("not implemented")
}

// GetLocationByGeoLevelName returns a location by its geo level name
func (s *Store) GetLocationByGeoLevelName(ctx context.Context, geoLevelName string) (*Location, error) {
	// TODO: implement
	return nil, fmt.Errorf("not implemented")
}

// DeleteLocation deletes a location and cascades to its names and relations
func (s *Store) DeleteLocation(ctx context.Context, id uuid.UUID) error {
	return s.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Check if location exists
		var location Location
		if err := tx.First(&location, id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrLocationNotFound
			}
			return err
		}

		// Delete all names (will be handled by CASCADE constraints)
		if err := tx.Where("location_id = ?", id).Delete(&NameMap{}).Error; err != nil {
			return fmt.Errorf("failed to delete names: %w", err)
		}

		// Delete all relations where this location is parent or child
		// (will be handled by CASCADE constraints)
		if err := tx.Where("parent_id = ? OR child_id = ?", id, id).Delete(&Relation{}).Error; err != nil {
			return fmt.Errorf("failed to delete relations: %w", err)
		}

		// Delete the location itself
		if err := tx.Delete(&location).Error; err != nil {
			return fmt.Errorf("failed to delete location: %w", err)
		}

		return nil
	})
}

// ListLocations lists all locations with their names
func (s *Store) ListLocations(ctx context.Context) ([]*LocationWithNames, error) {
	var locations []Location
	err := s.DB.WithContext(ctx).
		Preload("GeoLevel").
		Find(&locations).Error
	if err != nil {
		return nil, fmt.Errorf("failed to list locations: %w", err)
	}

	results := make([]*LocationWithNames, 0, len(locations))
	for _, loc := range locations {
		var names []NameMap
		if err := s.DB.Where("location_id = ?", loc.Id).Find(&names).Error; err != nil {
			return nil, fmt.Errorf("failed to get names for location %s: %w", loc.Id, err)
		}

		result := &LocationWithNames{
			Id:       loc.Id,
			GeoLevel: loc.GeoLevel.Name,
			Aliases:  make([]string, 0),
		}

		for _, name := range names {
			if name.IsPrimary {
				result.Name = name.Name
			} else {
				result.Aliases = append(result.Aliases, name.Name)
			}
		}

		results = append(results, result)
	}

	return results, nil
}
