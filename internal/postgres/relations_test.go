package postgres

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRelationsTest(t *testing.T) (*Store, map[string]*Location, map[string]*GeoLevel) {
	store := setupTestDB(t)
	ctx := context.Background()

	// Ensure the Relation model is also migrated
	err := store.DB.AutoMigrate(&Relation{})
	require.NoError(t, err)

	// Create geo levels with different ranks
	geoLevels := make(map[string]*GeoLevel)

	// Create levels with sequential ranks for testing hierarchy
	levelData := []struct {
		name string
		rank float64
	}{
		{"COUNTRY", 1.0},
		{"STATE", 2.0},
		{"DISTRICT", 3.0},
		{"CITY", 4.0},
	}

	for _, ld := range levelData {
		level, err := store.InsertGeoLevel(ctx, ld.name, float64Ptr(ld.rank))
		require.NoError(t, err)
		require.NotNil(t, level)
		geoLevels[ld.name] = level
	}

	// Create test locations at each level
	locations := make(map[string]*Location)
	locationData := []struct {
		name     string
		geoLevel string
	}{
		{"Country1", "COUNTRY"},
		{"Country2", "COUNTRY"},
		{"State1", "STATE"},
		{"State2", "STATE"},
		{"District1", "DISTRICT"},
		{"District2", "DISTRICT"},
		{"City1", "CITY"},
		{"City2", "CITY"},
	}

	for _, ld := range locationData {
		location := &Location{
			GeoLevelID: geoLevels[ld.geoLevel].Id,
		}
		err := store.DB.Create(location).Error
		require.NoError(t, err)
		locations[ld.name] = location

		// Add a primary name for the location for easier identification in tests
		err = store.InsertNameMap(ctx, location.Id, ld.name, true)
		require.NoError(t, err)
	}

	return store, locations, geoLevels
}

func TestRelation_InsertRelation(t *testing.T) {
	store, locations, _ := setupRelationsTest(t)
	ctx := context.Background()

	tests := []struct {
		name        string
		parentID    uuid.UUID
		childID     uuid.UUID
		wantErr     bool
		errType     error
		verifyAfter func(t *testing.T, store *Store, relation *Relation)
	}{
		{
			name:     "valid relation country-state",
			parentID: locations["Country1"].Id,
			childID:  locations["State1"].Id,
			wantErr:  false,
			verifyAfter: func(t *testing.T, store *Store, relation *Relation) {
				// Verify relation was created correctly
				assert.Equal(t, locations["Country1"].Id, relation.ParentID)
				assert.Equal(t, locations["State1"].Id, relation.ChildID)

				// Verify relation exists in database
				var count int64
				err := store.DB.Model(&Relation{}).
					Where("parent_id = ? AND child_id = ?", relation.ParentID, relation.ChildID).
					Count(&count).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(1), count)
			},
		},
		{
			name:     "valid relation state-district",
			parentID: locations["State1"].Id,
			childID:  locations["District1"].Id,
			wantErr:  false,
			verifyAfter: func(t *testing.T, store *Store, relation *Relation) {
				// Verify relation was created correctly
				assert.Equal(t, locations["State1"].Id, relation.ParentID)
				assert.Equal(t, locations["District1"].Id, relation.ChildID)
			},
		},
		{
			name:     "invalid hierarchy district-state",
			parentID: locations["District1"].Id,
			childID:  locations["State2"].Id,
			wantErr:  true,
			errType:  ErrInvalidHierarchy,
		},
		{
			name:     "same location",
			parentID: locations["State1"].Id,
			childID:  locations["State1"].Id,
			wantErr:  true,
			// Should return an error about parent and child being the same
		},
		{
			name:     "non-existent parent",
			parentID: uuid.New(),
			childID:  locations["State1"].Id,
			wantErr:  true,
			errType:  ErrLocationNotFound,
		},
		{
			name:     "non-existent child",
			parentID: locations["Country1"].Id,
			childID:  uuid.New(),
			wantErr:  true,
			errType:  ErrLocationNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relation, err := store.InsertRelation(ctx, tt.parentID, tt.childID)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, relation)

			// Run additional verification if provided
			if tt.verifyAfter != nil {
				tt.verifyAfter(t, store, relation)
			}
		})
	}

	// Test duplicate relation constraint
	t.Run("duplicate parent geo level", func(t *testing.T) {
		// First create a valid relation
		_, err := store.InsertRelation(ctx, locations["Country1"].Id, locations["State2"].Id)
		assert.NoError(t, err)

		// Try to create another relation with a different country but same child
		_, err = store.InsertRelation(ctx, locations["Country2"].Id, locations["State2"].Id)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrDuplicateRelation)
	})
}

func TestRelation_GetChildren(t *testing.T) {
	store, locations, _ := setupRelationsTest(t)
	ctx := context.Background()

	// Set up test data - create relations
	parentChildPairs := []struct {
		parent string
		child  string
	}{
		{"Country1", "State1"},
		{"Country1", "State2"},
		{"State1", "District1"},
		{"State2", "District2"},
		{"District1", "City1"},
		{"District2", "City2"},
	}

	for _, pair := range parentChildPairs {
		_, err := store.InsertRelation(ctx, locations[pair.parent].Id, locations[pair.child].Id)
		require.NoError(t, err)
	}

	tests := []struct {
		name       string
		parentID   uuid.UUID
		wantCount  int
		childNames []string
		wantErr    bool
	}{
		{
			name:       "country with multiple children",
			parentID:   locations["Country1"].Id,
			wantCount:  2,
			childNames: []string{"State1", "State2"},
			wantErr:    false,
		},
		{
			name:       "state with one child",
			parentID:   locations["State1"].Id,
			wantCount:  1,
			childNames: []string{"District1"},
			wantErr:    false,
		},
		{
			name:      "location with no children",
			parentID:  locations["City1"].Id,
			wantCount: 0,
			wantErr:   false,
		},
		{
			name:      "non-existent location",
			parentID:  uuid.New(),
			wantCount: 0,
			wantErr:   false, // GetChildren doesn't return an error for non-existent locations
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relations, err := store.GetChildren(ctx, tt.parentID)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Len(t, relations, tt.wantCount)

			// Verify the correct children are returned
			if len(tt.childNames) > 0 {
				childIDs := make([]uuid.UUID, 0, len(relations))
				for _, relation := range relations {
					childIDs = append(childIDs, relation.ChildID)
				}

				for _, name := range tt.childNames {
					assert.Contains(t, childIDs, locations[name].Id)
				}
			}

			// Verify preloaded data is available
			for _, relation := range relations {
				assert.NotNil(t, relation.Child)
				assert.NotNil(t, relation.Child.GeoLevel)
				assert.NotNil(t, relation.Parent)
				assert.NotNil(t, relation.Parent.GeoLevel)
			}
		})
	}
}

func TestRelation_GetParents(t *testing.T) {
	store, locations, _ := setupRelationsTest(t)
	ctx := context.Background()

	// Set up test data - create relations
	parentChildPairs := []struct {
		parent string
		child  string
	}{
		{"Country1", "State1"},
		{"State1", "District1"},
		{"District1", "City1"},
		// City1 has a hierarchy of parents: District1 -> State1 -> Country1

		{"Country2", "State2"},
		// State2 has only one parent
	}

	for _, pair := range parentChildPairs {
		_, err := store.InsertRelation(ctx, locations[pair.parent].Id, locations[pair.child].Id)
		require.NoError(t, err)
	}

	tests := []struct {
		name        string
		childID     uuid.UUID
		wantCount   int
		parentNames []string
		wantErr     bool
	}{
		{
			name:        "location with one parent",
			childID:     locations["State1"].Id,
			wantCount:   1,
			parentNames: []string{"Country1"},
			wantErr:     false,
		},
		{
			name:      "location with no parents",
			childID:   locations["Country1"].Id,
			wantCount: 0,
			wantErr:   false,
		},
		{
			name:      "non-existent location",
			childID:   uuid.New(),
			wantCount: 0,
			wantErr:   false, // GetParents doesn't return an error for non-existent locations
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relations, err := store.GetParents(ctx, tt.childID)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Len(t, relations, tt.wantCount)

			// Verify the correct parents are returned
			if len(tt.parentNames) > 0 {
				parentIDs := make([]uuid.UUID, 0, len(relations))
				for _, relation := range relations {
					parentIDs = append(parentIDs, relation.ParentID)
				}

				for _, name := range tt.parentNames {
					assert.Contains(t, parentIDs, locations[name].Id)
				}
			}

			// Verify preloaded data is available
			for _, relation := range relations {
				assert.NotNil(t, relation.Parent)
				assert.NotNil(t, relation.Parent.GeoLevel)
				assert.NotNil(t, relation.Child)
				assert.NotNil(t, relation.Child.GeoLevel)
			}
		})
	}
}

func TestRelation_DeleteRelation(t *testing.T) {
	store, locations, _ := setupRelationsTest(t)
	ctx := context.Background()

	// Create test relations
	relation1, err := store.InsertRelation(ctx, locations["Country1"].Id, locations["State1"].Id)
	require.NoError(t, err)

	tests := []struct {
		name        string
		relationID  uuid.UUID
		wantErr     bool
		errType     error
		verifyAfter func(t *testing.T, store *Store, relationID uuid.UUID)
	}{
		{
			name:       "delete existing relation",
			relationID: relation1.Id,
			wantErr:    false,
			verifyAfter: func(t *testing.T, store *Store, relationID uuid.UUID) {
				// Verify relation was deleted
				var count int64
				err := store.DB.Model(&Relation{}).
					Where("id = ?", relationID).
					Count(&count).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(0), count)
			},
		},
		{
			name:       "delete non-existent relation",
			relationID: uuid.New(),
			wantErr:    true,
			errType:    ErrRelationNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.DeleteRelation(ctx, tt.relationID)

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
				tt.verifyAfter(t, store, tt.relationID)
			}
		})
	}
}

func TestRelation_DeleteAllRelations(t *testing.T) {
	store, locations, _ := setupRelationsTest(t)
	ctx := context.Background()

	// Create test relations
	// State1 has relationships as both parent and child
	_, err := store.InsertRelation(ctx, locations["Country1"].Id, locations["State1"].Id)
	require.NoError(t, err)

	_, err = store.InsertRelation(ctx, locations["State1"].Id, locations["District1"].Id)
	require.NoError(t, err)

	// Create more test relations
	_, err = store.InsertRelation(ctx, locations["Country2"].Id, locations["State2"].Id)
	require.NoError(t, err)

	tests := []struct {
		name        string
		locationID  uuid.UUID
		wantErr     bool
		verifyAfter func(t *testing.T, store *Store, locationID uuid.UUID)
	}{
		{
			name:       "delete relations for location with parent and child relations",
			locationID: locations["State1"].Id,
			wantErr:    false,
			verifyAfter: func(t *testing.T, store *Store, locationID uuid.UUID) {
				// Verify all relations for this location were deleted
				var count int64
				err := store.DB.Model(&Relation{}).
					Where("parent_id = ? OR child_id = ?", locationID, locationID).
					Count(&count).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(0), count)

				// Verify other relations still exist
				err = store.DB.Model(&Relation{}).
					Where("parent_id = ? OR child_id = ?", locations["Country2"].Id, locations["State2"].Id).
					Count(&count).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(1), count)
			},
		},
		{
			name:       "delete relations for location with no relations",
			locationID: locations["City2"].Id,
			wantErr:    false,
			verifyAfter: func(t *testing.T, store *Store, locationID uuid.UUID) {
				// Should be a no-op, but we verify no error occurs
				var count int64
				err := store.DB.Model(&Relation{}).
					Where("parent_id = ? OR child_id = ?", locationID, locationID).
					Count(&count).Error
				assert.NoError(t, err)
				assert.Equal(t, int64(0), count)
			},
		},
		{
			name:       "delete relations for non-existent location",
			locationID: uuid.New(),
			wantErr:    false, // Should be a no-op, not an error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.DeleteAllRelations(ctx, tt.locationID)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			// Run additional verification if provided
			if tt.verifyAfter != nil {
				tt.verifyAfter(t, store, tt.locationID)
			}
		})
	}
}
