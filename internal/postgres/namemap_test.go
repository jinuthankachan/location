package postgres

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupNameMapTest(t *testing.T) (*Store, *Location, *Location) {
	store := setupTestDB(t)
	ctx := context.Background()

	// Ensure the NameMap model is also migrated
	err := store.DB.AutoMigrate(&NameMap{})
	require.NoError(t, err)

	// Create a test geo level
	countryLevel, err := store.InsertGeoLevel(ctx, "COUNTRY", float64Ptr(1.0))
	require.NoError(t, err)
	require.NotNil(t, countryLevel)

	// Create another geo level for testing
	stateLevel, err := store.InsertGeoLevel(ctx, "STATE", float64Ptr(2.0))
	require.NoError(t, err)
	require.NotNil(t, stateLevel)

	// Create test locations
	location1 := &Location{
		GeoLevelID: countryLevel.Id,
	}
	err = store.DB.Create(location1).Error
	require.NoError(t, err)

	location2 := &Location{
		GeoLevelID: stateLevel.Id,
	}
	err = store.DB.Create(location2).Error
	require.NoError(t, err)

	return store, location1, location2
}

func TestNameMap_InsertNameMap(t *testing.T) {
	store, location1, location2 := setupNameMapTest(t)
	ctx := context.Background()

	tests := []struct {
		name        string
		locationID  uuid.UUID
		nameName    string
		isPrimary   bool
		wantErr     bool
		errType     error
		verifyAfter func(t *testing.T, store *Store, locationID uuid.UUID, name string)
	}{
		{
			name:       "valid name non-primary",
			locationID: location1.Id,
			nameName:   "Test Name",
			isPrimary:  false,
			wantErr:    false,
			verifyAfter: func(t *testing.T, store *Store, locationID uuid.UUID, name string) {
				// Verify name was created
				var nameMap NameMap
				err := store.DB.Where("location_id = ? AND name = ?", locationID, name).First(&nameMap).Error
				assert.NoError(t, err)
				assert.Equal(t, name, nameMap.Name)
				assert.Equal(t, locationID, nameMap.LocationID)
				assert.False(t, nameMap.IsPrimary)
			},
		},
		{
			name:       "valid name primary",
			locationID: location2.Id,
			nameName:   "Primary Name",
			isPrimary:  true,
			wantErr:    false,
			verifyAfter: func(t *testing.T, store *Store, locationID uuid.UUID, name string) {
				// Verify name was created as primary
				var nameMap NameMap
				err := store.DB.Where("location_id = ? AND name = ?", locationID, name).First(&nameMap).Error
				assert.NoError(t, err)
				assert.Equal(t, name, nameMap.Name)
				assert.Equal(t, locationID, nameMap.LocationID)
				assert.True(t, nameMap.IsPrimary)
			},
		},
		{
			name:       "empty name",
			locationID: location1.Id,
			nameName:   "",
			isPrimary:  false,
			wantErr:    true,
			errType:    ErrNameRequired,
		},
		{
			name:       "non-existent location",
			locationID: uuid.New(),
			nameName:   "Test Name",
			isPrimary:  false,
			wantErr:    true,
			errType:    ErrLocationNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.InsertNameMap(ctx, tt.locationID, tt.nameName, tt.isPrimary)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)

			// Run additional verification if provided
			if tt.verifyAfter != nil {
				tt.verifyAfter(t, store, tt.locationID, tt.nameName)
			}
		})
	}

	// Test adding another primary name to a location that already has one
	t.Run("adding second primary name", func(t *testing.T) {
		// First add a primary name
		err := store.InsertNameMap(ctx, location1.Id, "First Primary", true)
		assert.NoError(t, err)

		// Then try to add another primary name
		err = store.InsertNameMap(ctx, location1.Id, "Second Primary", true)
		assert.NoError(t, err)

		// Verify only one primary name exists and it's the second one
		var names []NameMap
		err = store.DB.Where("location_id = ?", location1.Id).Find(&names).Error
		assert.NoError(t, err)
		assert.Len(t, names, 2)

		var firstNameIsPrimary, secondNameIsPrimary bool
		for _, name := range names {
			if name.Name == "First Primary" {
				firstNameIsPrimary = name.IsPrimary
			} else if name.Name == "Second Primary" {
				secondNameIsPrimary = name.IsPrimary
			}
		}
		assert.False(t, firstNameIsPrimary, "First name should no longer be primary")
		assert.True(t, secondNameIsPrimary, "Second name should be primary")
	})

	// Test duplicate name detection
	t.Run("duplicate name", func(t *testing.T) {
		name := "Duplicate Test"
		// Add the name first
		err := store.InsertNameMap(ctx, location1.Id, name, false)
		assert.NoError(t, err)

		// Try to add the same name again
		err = store.InsertNameMap(ctx, location1.Id, name, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrNameAlreadyExists)
	})
}

func TestNameMap_InsertNames(t *testing.T) {
	store, location1, _ := setupNameMapTest(t)
	ctx := context.Background()

	tests := []struct {
		name        string
		locationID  uuid.UUID
		names       []string
		wantErr     bool
		errType     error
		verifyAfter func(t *testing.T, store *Store, locationID uuid.UUID, names []string)
	}{
		{
			name:       "valid names",
			locationID: location1.Id,
			names:      []string{"Name1", "Name2", "Name3"},
			wantErr:    false,
			verifyAfter: func(t *testing.T, store *Store, locationID uuid.UUID, names []string) {
				// Verify names were created
				var dbNames []NameMap
				err := store.DB.Where("location_id = ?", locationID).Find(&dbNames).Error
				assert.NoError(t, err)
				assert.Len(t, dbNames, len(names))

				// Verify all provided names are in the database
				foundNames := make([]string, 0, len(dbNames))
				for _, nameMap := range dbNames {
					foundNames = append(foundNames, nameMap.Name)
				}
				for _, name := range names {
					assert.Contains(t, foundNames, name)
				}
			},
		},
		{
			name:       "empty name list",
			locationID: location1.Id,
			names:      []string{},
			wantErr:    false, // This should be a no-op, not an error
		},
		{
			name:       "non-existent location",
			locationID: uuid.New(),
			names:      []string{"Name1", "Name2"},
			wantErr:    true,
			// Note: The InsertNames function doesn't currently check if location exists,
			// but it would fail due to foreign key constraints
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.InsertNames(ctx, tt.locationID, tt.names)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)

			// Run additional verification if provided
			if tt.verifyAfter != nil && len(tt.names) > 0 {
				tt.verifyAfter(t, store, tt.locationID, tt.names)
			}
		})
	}
}

func TestNameMap_GetNameMapByLocationID(t *testing.T) {
	store, location1, location2 := setupNameMapTest(t)
	ctx := context.Background()

	// Set up test data
	err := store.InsertNameMap(ctx, location1.Id, "Primary Location1", true)
	require.NoError(t, err)
	err = store.InsertNameMap(ctx, location1.Id, "Alias1 Location1", false)
	require.NoError(t, err)
	err = store.InsertNameMap(ctx, location1.Id, "Alias2 Location1", false)
	require.NoError(t, err)

	// Just add a primary name to location2
	err = store.InsertNameMap(ctx, location2.Id, "Primary Location2", true)
	require.NoError(t, err)

	tests := []struct {
		name       string
		locationID uuid.UUID
		wantCount  int
		wantErr    bool
		errType    error
	}{
		{
			name:       "location with multiple names",
			locationID: location1.Id,
			wantCount:  3,
			wantErr:    false,
		},
		{
			name:       "location with single name",
			locationID: location2.Id,
			wantCount:  1,
			wantErr:    false,
		},
		{
			name:       "non-existent location",
			locationID: uuid.New(),
			wantCount:  0,
			wantErr:    true,
			errType:    ErrLocationNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			names, err := store.GetNameMapByLocationID(ctx, tt.locationID)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)
			assert.Len(t, names, tt.wantCount)

			// Verify primary name is first
			if len(names) > 0 && tt.locationID == location1.Id {
				assert.True(t, names[0].IsPrimary, "Primary name should be first in the list")
				assert.Equal(t, "Primary Location1", names[0].Name)
			}
		})
	}

	// Test for a location with no names but which exists
	t.Run("location with no names", func(t *testing.T) {
		// Create a new location
		newLocation := &Location{
			GeoLevelID: location1.GeoLevelID,
		}
		err := store.DB.Create(newLocation).Error
		require.NoError(t, err)

		names, err := store.GetNameMapByLocationID(ctx, newLocation.Id)
		assert.NoError(t, err)
		assert.Empty(t, names)
	})
}

func TestNameMap_GetPrimaryName(t *testing.T) {
	store, location1, _ := setupNameMapTest(t)
	ctx := context.Background()

	// Set up test data
	err := store.InsertNameMap(ctx, location1.Id, "Primary Location1", true)
	require.NoError(t, err)
	err = store.InsertNameMap(ctx, location1.Id, "Alias1 Location1", false)
	require.NoError(t, err)

	// Location without a primary name
	locationWithoutPrimary := &Location{
		GeoLevelID: location1.GeoLevelID,
	}
	err = store.DB.Create(locationWithoutPrimary).Error
	require.NoError(t, err)
	err = store.InsertNameMap(ctx, locationWithoutPrimary.Id, "Just an alias", false)
	require.NoError(t, err)

	tests := []struct {
		name       string
		locationID uuid.UUID
		want       string
		wantErr    bool
		errType    error
	}{
		{
			name:       "location with primary name",
			locationID: location1.Id,
			want:       "Primary Location1",
			wantErr:    false,
		},
		{
			name:       "location without primary name",
			locationID: locationWithoutPrimary.Id,
			want:       "",
			wantErr:    true,
			errType:    ErrPrimaryNameNotFound,
		},
		{
			name:       "non-existent location",
			locationID: uuid.New(),
			want:       "",
			wantErr:    true,
			errType:    ErrPrimaryNameNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, err := store.GetPrimaryName(ctx, tt.locationID)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, name)
		})
	}
}

func TestNameMap_DeleteNameMap(t *testing.T) {
	store, location1, _ := setupNameMapTest(t)
	ctx := context.Background()

	// Set up test data
	err := store.InsertNameMap(ctx, location1.Id, "Primary Location1", true)
	require.NoError(t, err)
	err = store.InsertNameMap(ctx, location1.Id, "Alias1 Location1", false)
	require.NoError(t, err)
	err = store.InsertNameMap(ctx, location1.Id, "Alias2 Location1", false)
	require.NoError(t, err)

	tests := []struct {
		name        string
		locationID  uuid.UUID
		nameName    string
		wantErr     bool
		errType     error
		verifyAfter func(t *testing.T, store *Store, locationID uuid.UUID, name string)
	}{
		{
			name:       "delete existing non-primary name",
			locationID: location1.Id,
			nameName:   "Alias1 Location1",
			wantErr:    false,
			verifyAfter: func(t *testing.T, store *Store, locationID uuid.UUID, name string) {
				// Verify name was deleted
				var count int64
				err := store.DB.Model(&NameMap{}).
					Where("location_id = ? AND name = ? AND deleted_at IS NULL", locationID, name).
					Count(&count).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(0), count)

				// Verify other names still exist
				var names []NameMap
				err = store.DB.Where("location_id = ? AND deleted_at IS NULL", locationID).Find(&names).Error
				assert.NoError(t, err)
				assert.Len(t, names, 2) // Primary and one alias should remain
			},
		},
		{
			name:       "delete primary name",
			locationID: location1.Id,
			nameName:   "Primary Location1",
			wantErr:    true,
			errType:    ErrCannotDeletePrimary,
			verifyAfter: func(t *testing.T, store *Store, locationID uuid.UUID, name string) {
				// Verify primary name still exists
				var count int64
				err := store.DB.Model(&NameMap{}).
					Where("location_id = ? AND name = ? AND is_primary = ? AND deleted_at IS NULL",
						locationID, name, true).
					Count(&count).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(1), count)
			},
		},
		{
			name:       "delete non-existent name",
			locationID: location1.Id,
			nameName:   "Non-existent Name",
			wantErr:    false, // Should be a no-op, not an error
		},
		{
			name:       "empty name",
			locationID: location1.Id,
			nameName:   "",
			wantErr:    true,
			errType:    ErrNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.DeleteNameMap(ctx, tt.locationID, tt.nameName)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
			} else {
				assert.NoError(t, err)
			}

			// Run additional verification if provided
			if tt.verifyAfter != nil {
				tt.verifyAfter(t, store, tt.locationID, tt.nameName)
			}
		})
	}
}

func TestNameMap_UpdateNameMap(t *testing.T) {
	store, location1, _ := setupNameMapTest(t)
	ctx := context.Background()

	// Set up test data
	err := store.InsertNameMap(ctx, location1.Id, "Primary Location1", true)
	require.NoError(t, err)
	err = store.InsertNameMap(ctx, location1.Id, "Alias1 Location1", false)
	require.NoError(t, err)
	err = store.InsertNameMap(ctx, location1.Id, "Alias2 Location1", false)
	require.NoError(t, err)

	tests := []struct {
		name        string
		locationID  uuid.UUID
		oldName     string
		newName     string
		wantErr     bool
		errType     error
		verifyAfter func(t *testing.T, store *Store, locationID uuid.UUID, oldName, newName string)
	}{
		{
			name:       "update non-primary name",
			locationID: location1.Id,
			oldName:    "Alias1 Location1",
			newName:    "Updated Alias1",
			wantErr:    false,
			verifyAfter: func(t *testing.T, store *Store, locationID uuid.UUID, oldName, newName string) {
				// Verify name was updated
				var nameMap NameMap
				err := store.DB.Where("location_id = ? AND name = ? AND deleted_at IS NULL", locationID, newName).
					First(&nameMap).Error
				assert.NoError(t, err)
				assert.Equal(t, newName, nameMap.Name)

				// Verify old name no longer exists
				var count int64
				err = store.DB.Model(&NameMap{}).
					Where("location_id = ? AND name = ? AND deleted_at IS NULL", locationID, oldName).
					Count(&count).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(0), count)
			},
		},
		{
			name:       "update primary name",
			locationID: location1.Id,
			oldName:    "Primary Location1",
			newName:    "Updated Primary",
			wantErr:    false,
			verifyAfter: func(t *testing.T, store *Store, locationID uuid.UUID, oldName, newName string) {
				// Verify name was updated and is still primary
				var nameMap NameMap
				err := store.DB.Where("location_id = ? AND name = ? AND deleted_at IS NULL", locationID, newName).
					First(&nameMap).Error
				assert.NoError(t, err)
				assert.Equal(t, newName, nameMap.Name)
				assert.True(t, nameMap.IsPrimary)
			},
		},
		{
			name:       "update to existing name",
			locationID: location1.Id,
			oldName:    "Alias2 Location1",
			newName:    "Updated Alias1", // This name was created in the first test case
			wantErr:    true,
			errType:    ErrNameAlreadyExists,
		},
		{
			name:       "empty old name",
			locationID: location1.Id,
			oldName:    "",
			newName:    "Some Name",
			wantErr:    true,
			errType:    ErrNameRequired,
		},
		{
			name:       "empty new name",
			locationID: location1.Id,
			oldName:    "Alias2 Location1",
			newName:    "",
			wantErr:    true,
			errType:    ErrNameRequired,
		},
		{
			name:       "non-existent old name",
			locationID: location1.Id,
			oldName:    "Non-existent Name",
			newName:    "New Name",
			wantErr:    true,
			// Should return a fmt.Errorf, not a specific error type
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.UpdateNameMap(ctx, tt.locationID, tt.oldName, tt.newName)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)

			// Run additional verification if provided
			if tt.verifyAfter != nil {
				tt.verifyAfter(t, store, tt.locationID, tt.oldName, tt.newName)
			}
		})
	}
}

func TestNameMap_SetPrimaryName(t *testing.T) {
	store, location1, location2 := setupNameMapTest(t)
	ctx := context.Background()

	// Set up test data
	err := store.InsertNameMap(ctx, location1.Id, "Primary Location1", true)
	require.NoError(t, err)
	err = store.InsertNameMap(ctx, location1.Id, "Alias1 Location1", false)
	require.NoError(t, err)

	// Create empty location with no names
	emptyLocation := &Location{
		GeoLevelID: location1.GeoLevelID,
	}
	err = store.DB.Create(emptyLocation).Error
	require.NoError(t, err)

	tests := []struct {
		name        string
		locationID  uuid.UUID
		primaryName string
		wantErr     bool
		errType     error
		verifyAfter func(t *testing.T, store *Store, locationID uuid.UUID, primaryName string)
	}{
		{
			name:        "set existing name as primary",
			locationID:  location1.Id,
			primaryName: "Alias1 Location1",
			wantErr:     false,
			verifyAfter: func(t *testing.T, store *Store, locationID uuid.UUID, primaryName string) {
				// Verify the name was set as primary
				var nameMap NameMap
				err := store.DB.Where("location_id = ? AND name = ?", locationID, primaryName).First(&nameMap).Error
				assert.NoError(t, err)
				assert.True(t, nameMap.IsPrimary)

				// Verify the old primary is no longer primary
				var oldPrimary NameMap
				err = store.DB.Where("location_id = ? AND name = ?", locationID, "Primary Location1").First(&oldPrimary).Error
				assert.NoError(t, err)
				assert.False(t, oldPrimary.IsPrimary)

				// Verify only one primary name exists
				var count int64
				err = store.DB.Model(&NameMap{}).
					Where("location_id = ? AND is_primary = ? AND deleted_at IS NULL", locationID, true).
					Count(&count).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(1), count)
			},
		},
		{
			name:        "create new primary name",
			locationID:  location2.Id,
			primaryName: "New Primary Name",
			wantErr:     false,
			verifyAfter: func(t *testing.T, store *Store, locationID uuid.UUID, primaryName string) {
				// Verify the name was created and set as primary
				var nameMap NameMap
				err := store.DB.Where("location_id = ? AND name = ?", locationID, primaryName).First(&nameMap).Error
				assert.NoError(t, err)
				assert.True(t, nameMap.IsPrimary)

				// Verify only one primary name exists
				var count int64
				err = store.DB.Model(&NameMap{}).
					Where("location_id = ? AND is_primary = ? AND deleted_at IS NULL", locationID, true).
					Count(&count).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(1), count)
			},
		},
		{
			name:        "create primary for location with no names",
			locationID:  emptyLocation.Id,
			primaryName: "First Primary",
			wantErr:     false,
			verifyAfter: func(t *testing.T, store *Store, locationID uuid.UUID, primaryName string) {
				// Verify the name was created and set as primary
				var nameMap NameMap
				err := store.DB.Where("location_id = ? AND name = ?", locationID, primaryName).First(&nameMap).Error
				assert.NoError(t, err)
				assert.True(t, nameMap.IsPrimary)
			},
		},
		{
			name:        "empty name",
			locationID:  location1.Id,
			primaryName: "",
			wantErr:     true,
			errType:     ErrNameRequired,
		},
		{
			name:        "non-existent location",
			locationID:  uuid.New(),
			primaryName: "Test Primary",
			wantErr:     true,
			errType:     ErrLocationNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.SetPrimaryName(ctx, tt.locationID, tt.primaryName)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)

			// Run additional verification if provided
			if tt.verifyAfter != nil {
				tt.verifyAfter(t, store, tt.locationID, tt.primaryName)
			}
		})
	}

	// Test case: setting a name as primary that's already primary (no-op)
	t.Run("set already-primary name as primary again", func(t *testing.T) {
		// Get the current primary name for location1
		primaryName, err := store.GetPrimaryName(ctx, location1.Id)
		require.NoError(t, err)

		// Set it as primary again
		err = store.SetPrimaryName(ctx, location1.Id, primaryName)
		assert.NoError(t, err)

		// Verify it's still primary and nothing changed
		newPrimaryName, err := store.GetPrimaryName(ctx, location1.Id)
		assert.NoError(t, err)
		assert.Equal(t, primaryName, newPrimaryName)
	})
}

func TestNameMap_SearchNamesByPattern(t *testing.T) {
	store, location1, location2 := setupNameMapTest(t)
	ctx := context.Background()

	// Set up test data with various names to search for
	names := []struct {
		locationID uuid.UUID
		name       string
		isPrimary  bool
	}{
		{location1.Id, "United States", true},
		{location1.Id, "USA", false},
		{location1.Id, "America", false},
		{location2.Id, "California", true},
		{location2.Id, "CA", false},
		{location2.Id, "Golden State", false},
	}

	for _, n := range names {
		err := store.InsertNameMap(ctx, n.locationID, n.name, n.isPrimary)
		require.NoError(t, err)
	}

	tests := []struct {
		name      string
		pattern   string
		wantCount int
		wantNames []string
		wantErr   bool
		errType   error
	}{
		{
			name:      "exact match",
			pattern:   "USA",
			wantCount: 1,
			wantNames: []string{"USA"},
			wantErr:   false,
		},
		{
			name:      "partial match",
			pattern:   "State",
			wantCount: 2,
			wantNames: []string{"United States", "Golden State"},
			wantErr:   false,
		},
		{
			name:      "case insensitive match",
			pattern:   "ca",
			wantCount: 2,
			wantNames: []string{"California", "CA"},
			wantErr:   false,
		},
		{
			name:      "multiple matches across locations",
			pattern:   "a",
			wantCount: 5, // USA, America, California, CA, Golden State
			wantErr:   false,
		},
		{
			name:      "no matches",
			pattern:   "XYZ123",
			wantCount: 0,
			wantNames: []string{},
			wantErr:   false,
		},
		{
			name:    "empty pattern",
			pattern: "",
			wantErr: true,
			errType: ErrNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			names, err := store.SearchNamesByPattern(ctx, tt.pattern)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)
			assert.Len(t, names, tt.wantCount)

			// If specific names to check were provided, verify them
			if len(tt.wantNames) > 0 {
				foundNames := make([]string, 0, len(names))
				for _, nameMap := range names {
					foundNames = append(foundNames, nameMap.Name)
				}

				for _, wantName := range tt.wantNames {
					assert.Contains(t, foundNames, wantName)
				}
			}

			// Verify primary names are first for each location
			locationToPrimaryIdx := make(map[uuid.UUID]int)
			for i, nameMap := range names {
				locID := nameMap.LocationID
				if _, exists := locationToPrimaryIdx[locID]; !exists || nameMap.IsPrimary {
					locationToPrimaryIdx[locID] = i
				}
			}

			// Check that for each location, the primary name appears first
			for locID, idx := range locationToPrimaryIdx {
				for i, nameMap := range names {
					if nameMap.LocationID == locID && nameMap.IsPrimary {
						assert.LessOrEqual(t, i, idx, "Primary name should appear first for each location")
						break
					}
				}
			}
		})
	}

	// Test that each name includes its location and geo level via preloading
	t.Run("preloaded relationships", func(t *testing.T) {
		names, err := store.SearchNamesByPattern(ctx, "State")
		assert.NoError(t, err)
		assert.NotEmpty(t, names)

		for _, nameMap := range names {
			assert.NotEmpty(t, nameMap.Location.Id)
			assert.NotEmpty(t, nameMap.Location.GeoLevel.Name)
		}
	})
}
