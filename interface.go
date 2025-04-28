package location

import "context"

type LocationService interface {
	AddLocation(ctx context.Context, geoID string, geoLevel string, name string) (Location, error)
	UpdateLocation(ctx context.Context, geoID string, name *string, geoLevel *string) (Location, error)
	AddGeoLevel(ctx context.Context, name string, rank *float64) error
	UpdateGeoLevel(ctx context.Context, name string, newName *string, newRank *float64) error
	AddAliasToLocation(ctx context.Context, geoID string, name string) error
	RemoveAlias(ctx context.Context, geoID string, name string) error
	AddParent(ctx context.Context, geoID string, parentGeoID string) error
	RemoveParent(ctx context.Context, geoID string, parentGeoID string) error
	AddChildren(ctx context.Context, geoID string, childGeoIDs []string) error
	RemoveChildren(ctx context.Context, geoID string, childGeoIDs []string) error
	DeleteLocation(ctx context.Context, geoID string) error
	GetLocation(ctx context.Context, geoID string) (*Location, error)
	GetLocations(ctx context.Context, geoIDs []string) ([]Location, error)
	GetLocationsByPattern(ctx context.Context, name string) ([]Location, error)
	GetAllParents(ctx context.Context, geoID string) ([]Location, error)
	GetParentAtLevel(ctx context.Context, geoID string, geoLevel string) (*Location, error)
	GetAllChildren(ctx context.Context, geoID string) ([]Location, error)
	GetChildrenAtLevel(ctx context.Context, geoID string, geoLevel string) ([]Location, error)
}

type Location struct {
	GeoID    string   `json:"geo_id"` // Location.Id is also referenced to as geo_id.
	GeoLevel string   `json:"geo_level"`
	Name     string   `json:"name"`    // primary name of the location
	Aliases  []string `json:"aliases"` // aliases of the location
}
