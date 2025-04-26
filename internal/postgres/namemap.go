package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// NameMap represents names including alternate ones by which the location is known
type NameMap struct {
	BaseModel
	LocationID uuid.UUID `gorm:"type:uuid;not null;index" json:"location_id"`
	Location   *Location `gorm:"foreignKey:LocationID;references:Id;constraint:OnDelete:CASCADE" json:"location"`
	Name       string    `gorm:"type:varchar(255);not null" json:"name"`
	IsPrimary  bool      `gorm:"not null;default:false" json:"is_primary"`
}

// TableName returns the table name for the NameMap model
func (NameMap) TableName() string {
	return "name_maps"
}

// BeforeCreate hook ensures only one primary name per location
func (nm *NameMap) BeforeCreate(tx *gorm.DB) error {
	if nm.IsPrimary {
		var count int64
		if err := tx.Model(&NameMap{}).
			Where("location_id = ? AND is_primary = ? AND deleted_at IS NULL", nm.LocationID, true).
			Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return ErrPrimaryNameExists
		}
	}
	return nil
}

// InsertNameMap inserts a new name map (alias or primary name) for a location
func (s *Store) InsertNameMap(ctx context.Context, locationID uuid.UUID, name string, isPrimary bool) error {
	if name == "" {
		return ErrNameRequired
	}

	return s.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Check if location exists
		var location Location
		if err := tx.First(&location, locationID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrLocationNotFound
			}
			return err
		}

		// Check if name already exists for this location
		var existingCount int64
		if err := tx.Model(&NameMap{}).
			Where("location_id = ? AND name = ? AND deleted_at IS NULL", locationID, name).
			Count(&existingCount).Error; err != nil {
			return err
		}
		if existingCount > 0 {
			return ErrNameAlreadyExists
		}

		if isPrimary {
			// Check if there's already a primary name
			var primaryCount int64
			if err := tx.Model(&NameMap{}).
				Where("location_id = ? AND is_primary = ? AND deleted_at IS NULL", locationID, true).
				Count(&primaryCount).Error; err != nil {
				return err
			}

			if primaryCount > 0 {
				// Update existing primary name to non-primary
				if err := tx.Model(&NameMap{}).
					Where("location_id = ? AND is_primary = ? AND deleted_at IS NULL", locationID, true).
					Update("is_primary", false).Error; err != nil {
					return err
				}
			}
		}

		// Create the new name
		nameMap := &NameMap{
			LocationID: locationID,
			Name:       name,
			IsPrimary:  isPrimary,
		}
		return tx.Create(nameMap).Error
	})
}

// InsertNames inserts multiple names for a location as a sql bulk insertion operation
func (s *Store) InsertNames(ctx context.Context, locationID uuid.UUID, names []string) error {
	if len(names) == 0 {
		return nil
	}

	nameMaps := make([]NameMap, 0, len(names))
	for _, name := range names {
		nameMaps = append(nameMaps, NameMap{
			LocationID: locationID,
			Name:       name,
		})
	}

	return s.DB.WithContext(ctx).Create(&nameMaps).Error
}

// GetNameMapByLocationID returns all name maps for a location
func (s *Store) GetNameMapByLocationID(ctx context.Context, locationID uuid.UUID) ([]NameMap, error) {
	var names []NameMap
	err := s.DB.WithContext(ctx).
		Where("location_id = ? AND deleted_at IS NULL", locationID).
		Order("is_primary DESC, name ASC"). // Primary name first, then alphabetically
		Find(&names).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get location names: %w", err)
	}

	if len(names) == 0 {
		// Check if location exists
		var count int64
		if err := s.DB.Model(&Location{}).Where("id = ?", locationID).Count(&count).Error; err != nil {
			return nil, err
		}
		if count == 0 {
			return nil, ErrLocationNotFound
		}
	}

	return names, nil
}

// GetPrimaryName returns the primary name for a location
func (s *Store) GetPrimaryName(ctx context.Context, locationID uuid.UUID) (string, error) {
	var nameMap NameMap
	err := s.DB.WithContext(ctx).
		Where("location_id = ? AND is_primary = ? AND deleted_at IS NULL", locationID, true).
		First(&nameMap).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", ErrPrimaryNameNotFound
		}
		return "", fmt.Errorf("failed to get primary name: %w", err)
	}

	return nameMap.Name, nil
}

// DeleteNameMap deletes a name map entry
// Returns error if trying to delete a primary name
func (s *Store) DeleteNameMap(ctx context.Context, locationID uuid.UUID, name string) error {
	if name == "" {
		return ErrNameRequired
	}

	return s.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var nameMap NameMap
		if err := tx.Where("location_id = ? AND name = ? AND deleted_at IS NULL", locationID, name).
			First(&nameMap).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Name doesn't exist, nothing to delete
				return nil
			}
			return err
		}

		// Cannot delete primary name
		if nameMap.IsPrimary {
			return ErrCannotDeletePrimary
		}

		return tx.Delete(&nameMap).Error
	})
}

// UpdateNameMap updates a name for a location
func (s *Store) UpdateNameMap(ctx context.Context, locationID uuid.UUID, oldName, newName string) error {
	if oldName == "" || newName == "" {
		return ErrNameRequired
	}

	return s.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Find the existing name
		var nameMap NameMap
		if err := tx.Where("location_id = ? AND name = ? AND deleted_at IS NULL", locationID, oldName).
			First(&nameMap).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("name %s not found for location", oldName)
			}
			return err
		}

		// Check if new name already exists for this location
		var existingCount int64
		if err := tx.Model(&NameMap{}).
			Where("location_id = ? AND name = ? AND id != ? AND deleted_at IS NULL",
				locationID, newName, nameMap.Id).
			Count(&existingCount).Error; err != nil {
			return err
		}
		if existingCount > 0 {
			return ErrNameAlreadyExists
		}

		// Update the name
		nameMap.Name = newName
		return tx.Save(&nameMap).Error
	})
}

// SetPrimaryName sets a name as the primary name for a location
// If the name doesn't exist, it will be created as primary
// Any existing primary name will be demoted to a regular alias
func (s *Store) SetPrimaryName(ctx context.Context, locationID uuid.UUID, name string) error {
	if name == "" {
		return ErrNameRequired
	}

	return s.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Check if location exists
		var location Location
		if err := tx.First(&location, locationID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrLocationNotFound
			}
			return err
		}

		// See if the name already exists
		var nameMap NameMap
		err := tx.Where("location_id = ? AND name = ? AND deleted_at IS NULL", locationID, name).
			First(&nameMap).Error

		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// If name doesn't exist, create it as primary
				nameMap = NameMap{
					LocationID: locationID,
					Name:       name,
					IsPrimary:  true,
				}

				// First demote any existing primary
				if err := tx.Model(&NameMap{}).
					Where("location_id = ? AND is_primary = ? AND deleted_at IS NULL", locationID, true).
					Update("is_primary", false).Error; err != nil {
					return err
				}

				return tx.Create(&nameMap).Error
			}
			return err
		}

		if nameMap.IsPrimary {
			return nil // Already primary, nothing to do
		}

		// Update existing primary to non-primary
		if err := tx.Model(&NameMap{}).
			Where("location_id = ? AND is_primary = ? AND deleted_at IS NULL", locationID, true).
			Update("is_primary", false).Error; err != nil {
			return err
		}

		// Set the new primary
		nameMap.IsPrimary = true
		return tx.Save(&nameMap).Error
	})
}

// SearchNamesByPattern searches for location names matching a pattern
func (s *Store) SearchNamesByPattern(ctx context.Context, pattern string) ([]NameMap, error) {
	if pattern == "" {
		return nil, ErrNameRequired
	}

	var names []NameMap
	err := s.DB.WithContext(ctx).
		Preload("Location.GeoLevel").
		Where("name LIKE ? AND deleted_at IS NULL", fmt.Sprintf("%%%s%%", pattern)).
		Order("is_primary DESC, name ASC").
		Find(&names).Error
	if err != nil {
		return nil, fmt.Errorf("failed to search names by pattern: %w", err)
	}

	return names, nil
}
