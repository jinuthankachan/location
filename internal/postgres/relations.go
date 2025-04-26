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
	Parent   *Location `gorm:"foreignKey:ParentID;references:Id;constraint:OnDelete:CASCADE" json:"parent"`
	Child    *Location `gorm:"foreignKey:ChildID;references:Id;constraint:OnDelete:CASCADE" json:"child"`
}

// TableName returns the table name for the Relation model
func (Relation) TableName() string {
	return "relations"
}

// BeforeCreate hook validates hierarchy and enforces unique parent level constraint
func (r *Relation) BeforeCreate(tx *gorm.DB) error {
	// Call BaseModel's BeforeCreate to set UUID
	if err := r.BaseModel.BeforeCreate(tx); err != nil {
		return err
	}

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
	if parentLocationID == childLocationID {
		return nil, errors.New("parent and child cannot be the same location")
	}

	relation := &Relation{
		ParentID: parentLocationID,
		ChildID:  childLocationID,
	}

	err := s.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// The BeforeCreate hook will handle validation
		if err := tx.Create(relation).Error; err != nil {
			return fmt.Errorf("failed to create relation: %w", err)
		}

		// Load parent and child relationships
		if err := tx.Model(relation).Preload("Parent").Preload("Child").First(relation).Error; err != nil {
			return fmt.Errorf("failed to load relation details: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return relation, nil
}

// GetChildren returns children of a location by its location id
func (s *Store) GetChildren(ctx context.Context, parentLocationID uuid.UUID) ([]Relation, error) {
	var relations []Relation
	err := s.DB.WithContext(ctx).
		Where("parent_id = ?", parentLocationID).
		Preload("Child.GeoLevel").
		Preload("Parent.GeoLevel").
		Find(&relations).Error

	if err != nil {
		return nil, fmt.Errorf("failed to get children: %w", err)
	}

	return relations, nil
}

// GetParents returns parents of a location by its location id
func (s *Store) GetParents(ctx context.Context, childLocationID uuid.UUID) ([]Relation, error) {
	var relations []Relation
	err := s.DB.WithContext(ctx).
		Where("child_id = ?", childLocationID).
		Preload("Parent.GeoLevel").
		Preload("Child.GeoLevel").
		Find(&relations).Error

	if err != nil {
		return nil, fmt.Errorf("failed to get parents: %w", err)
	}

	return relations, nil
}

// DeleteRelation deletes a relation by its id
func (s *Store) DeleteRelation(ctx context.Context, id uuid.UUID) error {
	result := s.DB.WithContext(ctx).Delete(&Relation{}, id)
	if result.Error != nil {
		return fmt.Errorf("failed to delete relation: %w", result.Error)
	}

	if result.RowsAffected == 0 {
		return ErrRelationNotFound
	}

	return nil
}

// DeleteAllRelations deletes all relations of a location by its location id
func (s *Store) DeleteAllRelations(ctx context.Context, locationID uuid.UUID) error {
	// Delete all relations where the location is either parent or child
	err := s.DB.WithContext(ctx).
		Where("parent_id = ? OR child_id = ?", locationID, locationID).
		Delete(&Relation{}).Error

	if err != nil {
		return fmt.Errorf("failed to delete location relations: %w", err)
	}

	return nil
}
