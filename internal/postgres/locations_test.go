package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupLocationTest(t *testing.T) (*Store, *GeoLevel) {
	store := setupTestDB(t)
	ctx := context.Background()

	// Create a test geo level
	geoLevel, err := store.InsertGeoLevel(ctx, "COUNTRY", float64Ptr(1.0))
	require.NoError(t, err)
	require.NotNil(t, geoLevel)

	// Create another geo level for testing relationships
	_, err = store.InsertGeoLevel(ctx, "STATE", float64Ptr(2.0))
	require.NoError(t, err)

	return store, geoLevel
}

func TestLocation_InsertLocation(t *testing.T) {
	store, _ := setupLocationTest(t)
	ctx := context.Background()

	tests := []struct {
		name     string
		geoLevel string
		locName  string
		wantErr  bool
		errType  error
	}{
		{
			name:     "valid location",
			geoLevel: "COUNTRY",
			locName:  "Test Country",
			wantErr:  false,
		},
		{
			name:     "case-insensitive geo level",
			geoLevel: "country",
			locName:  "Test Country 2",
			wantErr:  false,
		},
		{
			name:     "empty name",
			geoLevel: "COUNTRY",
			locName:  "",
			wantErr:  true,
			errType:  ErrNameRequired,
		},
		{
			name:     "non-existent geo level",
			geoLevel: "INVALID",
			locName:  "Test Location",
			wantErr:  true,
			errType:  ErrGeoLevelNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			location, err := store.InsertLocation(ctx, tt.geoLevel, tt.locName)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, location)

			// Verify location was created with correct geo level
			assert.NotEqual(t, uuid.Nil, location.Id)
			assert.NotEqual(t, uuid.Nil, location.GeoLevelID)

			// Verify primary name was created
			locWithNames, err := store.GetLocation(ctx, location.Id)
			assert.NoError(t, err)
			assert.Equal(t, tt.locName, locWithNames.Name)
			assert.Equal(t, strings.ToUpper(tt.geoLevel), locWithNames.GeoLevel)
		})
	}
}

func TestLocation_GetLocation(t *testing.T) {
	store, _ := setupLocationTest(t)
	ctx := context.Background()

	// Create a test location with names
	location, err := store.InsertLocation(ctx, "COUNTRY", "Primary Name")
	require.NoError(t, err)

	// Add some aliases
	aliases := []string{"Alias1", "Alias2"}
	for _, alias := range aliases {
		err := store.DB.Create(&NameMap{
			LocationID: location.Id,
			Name:       alias,
			IsPrimary:  false,
		}).Error
		require.NoError(t, err)
	}

	tests := []struct {
		name    string
		id      uuid.UUID
		want    *LocationWithNames
		wantErr bool
		errType error
	}{
		{
			name: "existing location with names",
			id:   location.Id,
			want: &LocationWithNames{
				Id:       location.Id,
				GeoLevel: "COUNTRY",
				Name:     "Primary Name",
				Aliases:  aliases,
			},
			wantErr: false,
		},
		{
			name:    "non-existent location",
			id:      uuid.New(),
			wantErr: true,
			errType: ErrLocationNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.GetLocation(ctx, tt.id)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want.Id, got.Id)
			assert.Equal(t, tt.want.GeoLevel, got.GeoLevel)
			assert.Equal(t, tt.want.Name, got.Name)
			assert.ElementsMatch(t, tt.want.Aliases, got.Aliases)
		})
	}
}

func TestLocation_GetLocationsByPattern(t *testing.T) {
	store, _ := setupLocationTest(t)
	ctx := context.Background()

	// Create test locations with different names
	testData := []struct {
		geoLevel string
		name     string
		aliases  []string
	}{
		{
			geoLevel: "COUNTRY",
			name:     "Test Country",
			aliases:  []string{"Test Land", "Test Nation"},
		},
		{
			geoLevel: "COUNTRY",
			name:     "Another Country",
			aliases:  []string{"Other Land"},
		},
		{
			geoLevel: "STATE",
			name:     "Test State",
			aliases:  []string{"Test Province"},
		},
	}

	locations := make([]Location, 0, len(testData))
	for _, td := range testData {
		location, err := store.InsertLocation(ctx, td.geoLevel, td.name)
		require.NoError(t, err)
		locations = append(locations, *location)

		for _, alias := range td.aliases {
			err := store.DB.Create(&NameMap{
				LocationID: location.Id,
				Name:       alias,
				IsPrimary:  false,
			}).Error
			require.NoError(t, err)
		}
	}

	// Get a country geo level ID for filtering
	var countryGeoLevel GeoLevel
	err := store.DB.Where("name = ?", "COUNTRY").First(&countryGeoLevel).Error
	require.NoError(t, err)

	tests := []struct {
		name        string
		pattern     string
		geoLevelID  *uuid.UUID
		wantCount   int
		wantPattern string
		wantErr     bool
		errType     error
	}{
		{
			name:        "match primary name",
			pattern:     "Test Country",
			wantCount:   1,
			wantPattern: "Test Country",
			wantErr:     false,
		},
		{
			name:        "match partial name",
			pattern:     "Country",
			wantCount:   2, // Should match both "Test Country" and "Another Country"
			wantPattern: "Country",
			wantErr:     false,
		},
		{
			name:        "match alias",
			pattern:     "Land",
			wantCount:   2, // Should match both locations with "Land" in aliases
			wantPattern: "Land",
			wantErr:     false,
		},
		{
			name:        "case insensitive search",
			pattern:     "test",
			wantCount:   3, // Should match all locations with "Test" in name or alias
			wantPattern: "Test",
			wantErr:     false,
		},
		{
			name:        "filter by geo level",
			pattern:     "Test",
			geoLevelID:  &countryGeoLevel.Id,
			wantCount:   1, // Should only match "Test Country"
			wantPattern: "Test",
			wantErr:     false,
		},
		{
			name:    "empty pattern",
			pattern: "",
			wantErr: true,
			errType: ErrNameRequired,
		},
		{
			name:      "no matches",
			pattern:   "NonExistent",
			wantCount: 0,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.SearchLocationsByPattern(ctx, tt.pattern, tt.geoLevelID)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)
			assert.Len(t, got, tt.wantCount)

			if tt.wantCount > 0 {
				for _, loc := range got {
					matched := false
					if strings.Contains(strings.ToLower(loc.Name), strings.ToLower(tt.wantPattern)) {
						matched = true
					} else {
						for _, alias := range loc.Aliases {
							if strings.Contains(strings.ToLower(alias), strings.ToLower(tt.wantPattern)) {
								matched = true
								break
							}
						}
					}
					assert.True(t, matched, "Pattern should match either name or alias")

					// Verify geo level filter worked
					if tt.geoLevelID != nil {
						// Get the geo level ID for this location
						var location Location
						err := store.DB.Where("id = ?", loc.Id).First(&location).Error
						assert.NoError(t, err)
						assert.Equal(t, *tt.geoLevelID, location.GeoLevelID)
					}
				}
			}
		})
	}
}

func TestLocation_UpdateLocation(t *testing.T) {
	store, _ := setupLocationTest(t)
	ctx := context.Background()

	// Create initial location
	location, err := store.InsertLocation(ctx, "COUNTRY", "Initial Name")
	require.NoError(t, err)

	tests := []struct {
		name    string
		id      uuid.UUID
		newName string
		wantErr bool
		errType error
	}{
		{
			name:    "valid update",
			id:      location.Id,
			newName: "Updated Name",
			wantErr: false,
		},
		{
			name:    "empty name",
			id:      location.Id,
			newName: "",
			wantErr: true,
			errType: ErrNameRequired,
		},
		{
			name:    "non-existent location",
			id:      uuid.New(),
			newName: "Test Name",
			wantErr: true,
			errType: ErrLocationNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updated, err := store.UpdateLocation(ctx, tt.id, nil, &tt.newName)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, updated)

			// Verify the update
			got, err := store.GetLocation(ctx, tt.id)
			assert.NoError(t, err)
			assert.Equal(t, tt.newName, got.Name)

			// Verify geo level was not changed
			assert.Equal(t, "COUNTRY", got.GeoLevel)
		})
	}
}

func TestLocation_DeleteLocation(t *testing.T) {
	store, _ := setupLocationTest(t)
	ctx := context.Background()

	// Create a location with names
	location, err := store.InsertLocation(ctx, "COUNTRY", "Test Location")
	require.NoError(t, err)

	// Add an alias
	err = store.DB.Create(&NameMap{
		LocationID: location.Id,
		Name:       "Test Alias",
		IsPrimary:  false,
	}).Error
	require.NoError(t, err)

	// Create a second location to test relations
	childLocation, err := store.InsertLocation(ctx, "STATE", "Child Location")
	require.NoError(t, err)

	// Create a relation between the locations
	err = store.DB.Create(&Relation{
		ParentID: location.Id,
		ChildID:  childLocation.Id,
	}).Error
	require.NoError(t, err)

	tests := []struct {
		name    string
		id      uuid.UUID
		wantErr bool
		errType error
	}{
		{
			name:    "existing location",
			id:      location.Id,
			wantErr: false,
		},
		{
			name:    "non-existent location",
			id:      uuid.New(),
			wantErr: true,
			errType: ErrLocationNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.DeleteLocation(ctx, tt.id)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)

			// Verify location was deleted
			_, err = store.GetLocation(ctx, tt.id)
			assert.ErrorIs(t, err, ErrLocationNotFound)

			// Verify names were deleted
			var nameCount int64
			err = store.DB.Model(&NameMap{}).Where("location_id = ?", tt.id).Count(&nameCount).Error
			assert.NoError(t, err)
			assert.Equal(t, int64(0), nameCount)

			// Verify relations were deleted
			var relationCount int64
			err = store.DB.Model(&Relation{}).
				Where("parent_id = ? OR child_id = ?", tt.id, tt.id).
				Count(&relationCount).Error
			assert.NoError(t, err)
			assert.Equal(t, int64(0), relationCount)
		})
	}
}

func TestLocation_ListLocations(t *testing.T) {
	store, _ := setupLocationTest(t)
	ctx := context.Background()

	// Create test locations
	testLocations := []struct {
		geoLevel string
		name     string
		aliases  []string
	}{
		{
			geoLevel: "COUNTRY",
			name:     "Location 1",
			aliases:  []string{"Alias 1", "Alias 2"},
		},
		{
			geoLevel: "STATE",
			name:     "Location 2",
			aliases:  []string{"Alias 3"},
		},
		{
			geoLevel: "COUNTRY",
			name:     "Location 3",
			aliases:  []string{},
		},
	}

	// Create test locations with their names
	for _, tl := range testLocations {
		location, err := store.InsertLocation(ctx, tl.geoLevel, tl.name)
		require.NoError(t, err)

		for _, alias := range tl.aliases {
			err := store.DB.Create(&NameMap{
				LocationID: location.Id,
				Name:       alias,
				IsPrimary:  false,
			}).Error
			require.NoError(t, err)
		}
	}

	// Test listing all locations
	t.Run("list all locations", func(t *testing.T) {
		locations, err := store.ListLocations(ctx)
		assert.NoError(t, err)
		assert.Len(t, locations, len(testLocations))

		// Create maps for easier verification
		locationMap := make(map[string]*LocationWithNames)
		for _, loc := range locations {
			locationMap[loc.Name] = loc
		}

		// Verify each location has correct data
		for _, tl := range testLocations {
			loc, exists := locationMap[tl.name]
			assert.True(t, exists, "Location %s should exist", tl.name)
			if exists {
				assert.Equal(t, tl.name, loc.Name)
				assert.Equal(t, strings.ToUpper(tl.geoLevel), loc.GeoLevel)
				assert.ElementsMatch(t, tl.aliases, loc.Aliases)
			}
		}
	})

	// Test empty list case
	t.Run("list locations after deletion", func(t *testing.T) {
		// Delete all locations
		err := store.DB.Exec("DELETE FROM locations").Error
		require.NoError(t, err)

		locations, err := store.ListLocations(ctx)
		assert.NoError(t, err)
		assert.Empty(t, locations)
	})
}
