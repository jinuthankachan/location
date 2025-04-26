package postgres

import (
	"context"
	"fmt"

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

// TableName returns the table name for the Relation model
func (Relation) TableName() string {
	return "relations"
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
