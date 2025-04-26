package location

import (
	"context"
	"fmt"

	"github.com/xaults/platform/location/internal/postgres"
	"gorm.io/gorm"

	"github.com/google/uuid"
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
	loc, err := service.db.InsertLocation(ctx, geoLevel, name)
	if err != nil {
		return Location{}, err
	}
	return Location{
		GeoID:    loc.Id.String(),
		GeoLevel: loc.GeoLevel.Name,
		Name:     name,
		Aliases:  []string{},
	}, nil
}

// AddGeoLevel creates a new geo level
func (service *ServiceOnPostgres) AddGeoLevel(ctx context.Context, name string, rank *float64) error {
	tx := service.db.DB.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	store := &postgres.Store{DB: tx}
	_, err := store.InsertGeoLevel(ctx, name, rank)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit().Error
}

// AddAliasToLocation adds an alias to a location
func (service *ServiceOnPostgres) AddAliasToLocation(ctx context.Context, geoID string, name string) error {
	id, err := uuidFromString(geoID)
	if err != nil {
		return err
	}
	tx := service.db.DB.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	store := &postgres.Store{DB: tx}
	err = store.InsertNameMap(ctx, id, name, false)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit().Error
}

// AddNewParent adds a new parent to a location.
func (service *ServiceOnPostgres) AddParent(ctx context.Context, geoID string, parentGeoID string) error {
	childID, err := uuidFromString(geoID)
	if err != nil {
		return err
	}
	parentID, err := uuidFromString(parentGeoID)
	if err != nil {
		return err
	}
	tx := service.db.DB.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	store := &postgres.Store{DB: tx}
	_, err = store.InsertRelation(ctx, parentID, childID)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit().Error
}

// AddNewChildren adds new children to a location.
func (service *ServiceOnPostgres) AddChildren(ctx context.Context, geoID string, childGeoIDs []string) error {
	parentID, err := uuidFromString(geoID)
	if err != nil {
		return err
	}
	tx := service.db.DB.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	store := &postgres.Store{DB: tx}
	for _, child := range childGeoIDs {
		childID, err := uuidFromString(child)
		if err != nil {
			tx.Rollback()
			return err
		}
		_, err = store.InsertRelation(ctx, parentID, childID)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit().Error
}

// GetLocation retrieves a location by its geo ID
func (service *ServiceOnPostgres) GetLocation(ctx context.Context, geoID string) (*Location, error) {
	id, err := uuidFromString(geoID)
	if err != nil {
		return nil, err
	}
	loc, err := service.db.GetLocation(ctx, id)
	if err != nil {
		return nil, err
	}
	return &Location{
		GeoID:    loc.Id.String(),
		GeoLevel: loc.GeoLevel,
		Name:     loc.Name,
		Aliases:  loc.Aliases,
	}, nil
}

func (service *ServiceOnPostgres) GetLocations(ctx context.Context, geoIDs []string) ([]Location, error) {
	var locations []Location
	for _, geoID := range geoIDs {
		loc, err := service.GetLocation(ctx, geoID)
		if err != nil {
			return nil, err
		}
		locations = append(locations, *loc)
	}
	return locations, nil
}

// GetLocationsByPattern finds locations matching the pattern of the name or one of the aliases
func (service *ServiceOnPostgres) GetLocationsByPattern(ctx context.Context, name string) ([]Location, error) {
	results, err := service.db.SearchLocationsByPattern(ctx, name, nil)
	if err != nil {
		return nil, err
	}
	var out []Location
	for _, loc := range results {
		out = append(out, Location{
			GeoID:    loc.Id.String(),
			GeoLevel: loc.GeoLevel,
			Name:     loc.Name,
			Aliases:  loc.Aliases,
		})
	}
	return out, nil
}

// GetAllParents returns all parents of a location
func (service *ServiceOnPostgres) GetAllParents(ctx context.Context, geoID string) ([]Location, error) {
	id, err := uuidFromString(geoID)
	if err != nil {
		return nil, err
	}
	relations, err := service.db.GetParents(ctx, id)
	if err != nil {
		return nil, err
	}
	var parents []Location
	for _, rel := range relations {
		if rel.Parent != nil {
			parents = append(parents, Location{
				GeoID:    rel.Parent.Id.String(),
				GeoLevel: rel.Parent.GeoLevel.Name,
				Name:     "", // Name can be loaded if needed
				Aliases:  []string{},
			})
		}
	}
	return parents, nil
}

// GetAllChildren returns all children of a location
func (service *ServiceOnPostgres) GetAllChildren(ctx context.Context, geoID string) ([]Location, error) {
	id, err := uuidFromString(geoID)
	if err != nil {
		return nil, err
	}
	relations, err := service.db.GetChildren(ctx, id)
	if err != nil {
		return nil, err
	}
	var children []Location
	for _, rel := range relations {
		if rel.Child != nil {
			children = append(children, Location{
				GeoID:    rel.Child.Id.String(),
				GeoLevel: rel.Child.GeoLevel.Name,
				Name:     "", // Name can be loaded if needed
				Aliases:  []string{},
			})
		}
	}
	return children, nil
}

// UpdateLocation updates a location by its geo ID
func (service *ServiceOnPostgres) UpdateLocation(ctx context.Context, geoID string, name string) (Location, error) {
	id, err := uuidFromString(geoID)
	if err != nil {
		return Location{}, err
	}
	loc, err := service.db.UpdateLocation(ctx, id, nil, &name)
	if err != nil {
		return Location{}, err
	}
	return Location{
		GeoID:    loc.Id.String(),
		GeoLevel: loc.GeoLevel.Name,
		Name:     name,
		Aliases:  []string{},
	}, nil
}

// UpdateGeoLevel updates a geo level by its name
func (service *ServiceOnPostgres) UpdateGeoLevel(ctx context.Context, name string, rank *float64) error {
	_, err := service.db.UpdateGeoLevel(ctx, name, "", rank)
	return err
}

// RemoveAlias removes an alias from a location
func (service *ServiceOnPostgres) RemoveAlias(ctx context.Context, geoID string, name string) error {
	id, err := uuidFromString(geoID)
	if err != nil {
		return err
	}
	tx := service.db.DB.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	store := &postgres.Store{DB: tx}
	err = store.DeleteNameMap(ctx, id, name)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit().Error
}

// RemoveParent removes a parent from a location
func (service *ServiceOnPostgres) RemoveParent(ctx context.Context, geoID string, parentGeoID string) error {
	childID, err := uuidFromString(geoID)
	if err != nil {
		return err
	}
	relations, err := service.db.GetParents(ctx, childID)
	if err != nil {
		return err
	}
	parentID, err := uuidFromString(parentGeoID)
	if err != nil {
		return err
	}
	tx := service.db.DB.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	store := &postgres.Store{DB: tx}
	for _, rel := range relations {
		if rel.ParentID == parentID {
			err := store.DeleteRelation(ctx, rel.Id)
			if err != nil {
				tx.Rollback()
				return err
			}
			return tx.Commit().Error
		}
	}
	tx.Rollback()
	return fmt.Errorf("relation not found for parent %s and child %s", parentGeoID, geoID)
}

// RemoveChildren removes a child from a location.
func (service *ServiceOnPostgres) RemoveChildren(ctx context.Context, geoID string, childGeoIDs []string) error {
	parentID, err := uuidFromString(geoID)
	if err != nil {
		return err
	}
	tx := service.db.DB.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	store := &postgres.Store{DB: tx}
	for _, child := range childGeoIDs {
		childID, err := uuidFromString(child)
		if err != nil {
			tx.Rollback()
			return err
		}
		relations, err := service.db.GetChildren(ctx, parentID)
		if err != nil {
			tx.Rollback()
			return err
		}
		found := false
		for _, rel := range relations {
			if rel.ChildID == childID {
				err := store.DeleteRelation(ctx, rel.Id)
				if err != nil {
					tx.Rollback()
					return err
				}
				found = true
			}
		}
		if !found {
			tx.Rollback()
			return fmt.Errorf("relation not found for parent %s and child %s", geoID, child)
		}
	}
	return tx.Commit().Error
}

// DeleteLocation deletes a location by its geo ID
// This will also delete all the relations of the location
// This will also delete all the aliases of the location
func (service *ServiceOnPostgres) DeleteLocation(ctx context.Context, geoID string) error {
	id, err := uuidFromString(geoID)
	if err != nil {
		return err
	}
	return service.db.DeleteLocation(ctx, id)
}

// GetChildrenAtLevel returns the children of a location at a specific geo level.
func (service *ServiceOnPostgres) GetChildrenAtLevel(ctx context.Context, geoID string, geoLevel string) ([]Location, error) {
	id, err := uuidFromString(geoID)
	if err != nil {
		return nil, err
	}
	relations, err := service.db.GetChildren(ctx, id)
	if err != nil {
		return nil, err
	}
	var children []Location
	for _, rel := range relations {
		if rel.Child != nil && rel.Child.GeoLevel.Name == geoLevel {
			children = append(children, Location{
				GeoID:    rel.Child.Id.String(),
				GeoLevel: rel.Child.GeoLevel.Name,
				Name:     "",
				Aliases:  []string{},
			})
		}
	}
	return children, nil
}

// GetParentAtLevel returns the parent of a location at a specific geo level.
func (service *ServiceOnPostgres) GetParentAtLevel(ctx context.Context, geoID string, geoLevel string) (*Location, error) {
	id, err := uuidFromString(geoID)
	if err != nil {
		return nil, err
	}
	relations, err := service.db.GetParents(ctx, id)
	if err != nil {
		return nil, err
	}
	for _, rel := range relations {
		if rel.Parent != nil && rel.Parent.GeoLevel.Name == geoLevel {
			return &Location{
				GeoID:    rel.Parent.Id.String(),
				GeoLevel: rel.Parent.GeoLevel.Name,
				Name:     "",
				Aliases:  []string{},
			}, nil
		}
	}
	return nil, fmt.Errorf("parent at level %s not found", geoLevel)
}

func uuidFromString(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}
