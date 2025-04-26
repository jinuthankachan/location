package location

import (
	"context"
	"fmt"

	"github.com/xaults/platform/location/internal/postgres"
	"gorm.io/gorm"
)

type ServiceOnPostgres struct {
	db postgres.Store
}

var _ LocationService = (*ServiceOnPostgres)(nil)

func NewServiceOnPostgres(db *gorm.DB) (*ServiceOnPostgres, error) {
	return &ServiceOnPostgres{db: postgres.Store{DB: db}}, nil
}

// AddLocation creates a new location
func (service *ServiceOnPostgres) AddLocation(ctx context.Context, geoID string, geoLevel string, name string) (Location, error) {
	// TODO: implement
	return Location{}, fmt.Errorf("not implemented")
}

// AddGeoLevel creates a new geo level
func (service *ServiceOnPostgres) AddGeoLevel(ctx context.Context, name string, rank *float64) error {
	// TODO: implement
	return fmt.Errorf("not implemented")
}

// AddAliasToLocation adds an alias to a location
func (service *ServiceOnPostgres) AddAliasToLocation(ctx context.Context, geoID string, name string) error {
	// TODO: implement
	return fmt.Errorf("not implemented")
}

// AddNewParent adds a new parent to a location.
func (service *ServiceOnPostgres) AddParent(ctx context.Context, geoID string, parentGeoID string) error {
	// TODO: implement; also should validate that the parentGeoID Rank is less than the childGeoID Rank, if rank exists for both
	return fmt.Errorf("not implemented")
}

// AddNewChildren adds new children to a location.
func (service *ServiceOnPostgres) AddChildren(ctx context.Context, geoID string, childGeoIDs []string) error {
	// TODO: implement; also should validate that the parentGeoID Rank is less than the childGeoID Rank, if rank exists for both
	return fmt.Errorf("not implemented")
}

// GetLocation retrieves a location by its geo ID
func (service *ServiceOnPostgres) GetLocation(ctx context.Context, geoID string) (*Location, error) {
	// TODO: implement
	return nil, fmt.Errorf("location not found")
}

func (service *ServiceOnPostgres) GetLocations(ctx context.Context, geoIDs []string) ([]Location, error) {
	// TODO: implement
	return nil, fmt.Errorf("locations not found")
}

// GetLocationByPattern finds location matching the pattern of the name or one of the aliases
func (service *ServiceOnPostgres) GetLocationByPattern(ctx context.Context, name string) (*Location, error) {
	// TODO: implement
	return nil, fmt.Errorf("location not found")
}

// GetAllParents returns all parents of a location
func (service *ServiceOnPostgres) GetAllParents(ctx context.Context, geoID string) ([]Location, error) {
	// TODO: implement
	return nil, fmt.Errorf("not implemented")
}

// GetAllChildren returns all children of a location
func (service *ServiceOnPostgres) GetAllChildren(ctx context.Context, geoID string) ([]Location, error) {
	// TODO: implement
	return nil, fmt.Errorf("not implemented")
}

// UpdateLocation updates a location by its geo ID
func (service *ServiceOnPostgres) UpdateLocation(ctx context.Context, geoID string, name string) (Location, error) {
	// TODO: Implement
	return Location{}, fmt.Errorf("not implemented")
}

// UpdateGeoLevel updates a geo level by its name
func (service *ServiceOnPostgres) UpdateGeoLevel(ctx context.Context, name string, rank *float64) error {
	// TODO: Implement
	return fmt.Errorf("not implemented")
}

// RemoveAlias removes an alias from a location
func (service *ServiceOnPostgres) RemoveAlias(ctx context.Context, geoID string, name string) error {
	// TODO: Implement
	return fmt.Errorf("not implemented")
}

// RemoveParent removes a parent from a location
func (service *ServiceOnPostgres) RemoveParent(ctx context.Context, geoID string, parentGeoID string) error {
	// TODO: Implement
	return fmt.Errorf("not implemented")
}

// DeleteLocation deletes a location by its geo ID
// This will also delete all the relations of the location
// This will also delete all the aliases of the location
func (service *ServiceOnPostgres) DeleteLocation(ctx context.Context, geoID string) error {
	// TODO: Implement
	return fmt.Errorf("not implemented")
}

// GetChildrenAtLevel returns the children of a location at a specific geo level.
func (service *ServiceOnPostgres) GetChildrenAtLevel(ctx context.Context, geoID string, geoLevel string) ([]Location, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// GetParentAtLevel returns the parent of a location at a specific geo level.
func (service *ServiceOnPostgres) GetParentAtLevel(ctx context.Context, geoID string, geoLevel string) (*Location, error) {
	// TODO: Implement
	return nil, fmt.Errorf("not implemented")
}

// RemoveChildren removes a child from a location.
func (service *ServiceOnPostgres) RemoveChildren(ctx context.Context, geoID string, childGeoIDs []string) error {
	// TODO: Implement
	return fmt.Errorf("not implemented")
}
