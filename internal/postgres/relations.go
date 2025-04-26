package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Relation represents the relationship between two locations
type Relation struct {
	BaseModel
	ParentID uuid.UUID `gorm:"type:uuid;not null;index" json:"parent_id"`
	ChildID  uuid.UUID `gorm:"type:uuid;not null;index" json:"child_id"`
	Parent   Location  `gorm:"foreignKey:ParentID;references:Id;constraint:OnDelete:CASCADE" json:"parent"`
	Child    Location  `gorm:"foreignKey:ChildID;references:Id;constraint:OnDelete:CASCADE" json:"child"`
}

// TableName returns the table name for the Relation model
func (Relation) TableName() string {
	return "relations"
}

// BeforeCreate hook validates hierarchy and enforces unique parent level constraint
func (r *Relation) BeforeCreate(tx *gorm.DB) error {
	var parent, child Location

	// Get parent and child with their geo levels
	if err := tx.Preload("GeoLevel").First(&parent, r.ParentID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrLocationNotFound
		}
		return err
	}

	if err := tx.Preload("GeoLevel").First(&child, r.ChildID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrLocationNotFound
		}
		return err
	}

	// Check ranks if both parent and child geo levels have ranks
	if parent.GeoLevel.Rank != nil && child.GeoLevel.Rank != nil {
		if *parent.GeoLevel.Rank >= *child.GeoLevel.Rank {
			return ErrInvalidHierarchy
		}
	}

	// Check for unique child-parent level combination
	// A child can have only one parent of a particular level
	var count int64
	if err := tx.Model(&Relation{}).
		Joins("JOIN locations parent ON parent.id = relations.parent_id").
		Where("relations.child_id = ? AND parent.geo_level_id = ? AND relations.deleted_at IS NULL",
			r.ChildID, parent.GeoLevelID).
		Count(&count).Error; err != nil {
		return err
	}

	if count > 0 {
		return ErrDuplicateRelation
	}

	return nil
}

// InsertRelation inserts a new relation
func (s *Store) InsertRelation(ctx context.Context, parentLocationID uuid.UUID, childLocationID uuid.UUID) (*Relation, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// GetChildren returns children of a location by its location id
func (s *Store) GetChildren(ctx context.Context, parentLocationID uuid.UUID) ([]Relation, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// GetParents returns parents of a location by its location id
func (s *Store) GetParents(ctx context.Context, childLocationID uuid.UUID) ([]Relation, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// DeleteRelation deletes a relation by its id
func (s *Store) DeleteRelation(ctx context.Context, id uuid.UUID) error {
	// TODO: Implement
	return fmt.Errorf("not implemented")
}

// DeleteAllRelations deletes all relations of a location by its location id
func (s *Store) DeleteAllRelations(ctx context.Context, locationID uuid.UUID) error {
	// TODO: Implement
	return fmt.Errorf("not implemented")
}
