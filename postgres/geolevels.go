package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

type GeoLevel struct {
	BaseModel
	Name string   `gorm:"type:varchar(64);unique;not null;check:name = upper(name)" json:"name"`
	Rank *float64 `gorm:"type:float" json:"rank"`
}

// TableName returns the table name for the GeoLevel model
func (GeoLevel) TableName() string {
	return "geo_levels"
}

// InsertGeoLevel inserts a new geo level
func (s *Store) InsertGeoLevel(ctx context.Context, name string, rank *float64) (*GeoLevel, error) {
	// Validate input
	if name == "" {
		return nil, ErrGeoLevelNameRequired
	}

	// Convert name to uppercase
	name = strings.ToUpper(name)

	geoLevel := &GeoLevel{
		Name: name,
		Rank: rank,
	}

	err := s.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Check for existing geo level with same name
		var count int64
		if err := tx.Model(&GeoLevel{}).Where("name = ?", name).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return ErrGeoLevelAlreadyExists
		}

		// Create the geo level
		return tx.Create(geoLevel).Error
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create geo level: %w", err)
	}

	return geoLevel, nil
}

// GetGeoLevelsByPattern returns geo levels by matching its name with the given pattern
func (s *Store) GetGeoLevelsByPattern(ctx context.Context, name string) ([]GeoLevel, error) {
	if name == "" {
		return nil, ErrGeoLevelNameRequired
	}

	// Convert to uppercase for consistent searching
	name = strings.ToUpper(name)

	var geoLevels []GeoLevel
	err := s.DB.WithContext(ctx).
		Where("name LIKE ?", fmt.Sprintf("%%%s%%", name)).
		Find(&geoLevels).Error

	if err != nil {
		return nil, fmt.Errorf("failed to get geo levels: %w", err)
	}

	if len(geoLevels) == 0 {
		return nil, ErrGeoLevelNotFound
	}

	return geoLevels, nil
}

// GetGeoLevelByName returns a geo level by its exact name
func (s *Store) GetGeoLevelByName(ctx context.Context, name string) (*GeoLevel, error) {
	if name == "" {
		return nil, ErrGeoLevelNameRequired
	}

	name = strings.ToUpper(name)

	var geoLevel GeoLevel
	err := s.DB.WithContext(ctx).
		Where("name = ?", name).
		First(&geoLevel).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrGeoLevelNotFound
		}
		return nil, fmt.Errorf("failed to get geo level: %w", err)
	}

	return &geoLevel, nil
}

// UpdateGeoLevel updates a geo level by its name
func (s *Store) UpdateGeoLevel(ctx context.Context, name string, newName *string, newRank *float64) (*GeoLevel, error) {
	if name == "" {
		return nil, ErrGeoLevelNameRequired
	}

	// Convert names to uppercase for consistent operations
	name = strings.ToUpper(name)
	if newName != nil {
		if *newName == "" {
			return nil, ErrGeoLevelNameRequired
		}
		*newName = strings.ToUpper(*newName)
		// Check if newName is properly uppercase
		if *newName != strings.ToUpper(*newName) {
			return nil, ErrGeoLevelNameNotUpper
		}
	}

	var geoLevel GeoLevel
	err := s.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Get existing geo level
		if err := tx.Where("name = ?", name).First(&geoLevel).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrGeoLevelNotFound
			}
			return err
		}

		// If new name is provided and different, check it doesn't exist
		if newName != nil && *newName != name {
			var count int64
			if err := tx.Model(&GeoLevel{}).Where("name = ?", *newName).Count(&count).Error; err != nil {
				return err
			}
			if count > 0 {
				return ErrGeoLevelAlreadyExists
			}
			geoLevel.Name = *newName
		}

		if newRank != nil {
			geoLevel.Rank = newRank
		}

		return tx.Save(&geoLevel).Error
	})

	if err != nil {
		return nil, fmt.Errorf("failed to update geo level: %w", err)
	}

	return &geoLevel, nil
}

// DeleteGeoLevel deletes a geo level by its name
func (s *Store) DeleteGeoLevel(ctx context.Context, name string) error {
	if name == "" {
		return ErrGeoLevelNameRequired
	}

	// Convert name to uppercase for consistent operations
	name = strings.ToUpper(name)

	err := s.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Get the geo level ID first
		var geoLevel GeoLevel
		if err := tx.Where("name = ?", name).First(&geoLevel).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrGeoLevelNotFound
			}
			return err
		}

		// Check if any locations are using this geo level
		var count int64
		if err := tx.Model(&Location{}).Where("geo_level_id = ?", geoLevel.Id).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return ErrGeoLevelInUse
		}

		// Delete the geo level
		return tx.Delete(&geoLevel).Error
	})

	if err != nil {
		return fmt.Errorf("failed to delete geo level: %w", err)
	}

	return nil
}
