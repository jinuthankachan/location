package location

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaults/platform/location/internal/postgres" // To access error types like ErrGeoLevelNotExist
	pg "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// setupTestDB remains the same as provided
func setupTestDB(t *testing.T) *ServiceOnPostgres {
	// Check if the test postgres container is already running
	psCmd := exec.Command("docker", "compose", "-f", "test.docker-compose.yaml", "ps", "--status=running")
	psOut, psErr := psCmd.Output()
	if psErr != nil || !strings.Contains(string(psOut), "test-location-postgres") {
		// Not running, so start the test postgres container using docker-compose
		upCmd := exec.Command("docker", "compose", "-f", "test.docker-compose.yaml", "up", "-d", "--wait")
		err := upCmd.Run()
		if err != nil {
			t.Fatalf("Failed to start test postgres container: %v", err)
		}
	} else {
		t.Logf("INFO: Test postgres container already running.")
	}

	// Wait for the database to be ready - Increased timeout
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var db *gorm.DB
	dsn := "host=localhost user=postgres password=postgres dbname=test_location port=5432 sslmode=disable TimeZone=Asia/Kolkata"
	// Try to connect to the database with retries
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timed out waiting for database to be ready at %s", dsn)
			return nil // Should be unreachable due to t.Fatalf
		default:
			var err error
			db, err = gorm.Open(pg.Open(dsn), &gorm.Config{})
			if err == nil {
				sqlDB, dbErr := db.DB()
				if dbErr != nil {
					t.Fatalf("Failed to get underlying *sql.DB: %v", dbErr)
				}
				pingErr := sqlDB.PingContext(ctx)
				if pingErr == nil {
					// Successfully connected and pinged
					goto connected // break out of the loop
				}
				t.Logf("INFO: DB connection ping failed (will retry): err- %v", pingErr)
			} else {
				// Log warning instead of failing
				t.Logf("INFO: failed to connect to test DB (will retry): %v", err)
			}

			// Wait a bit before retrying
			time.Sleep(2 * time.Second)
		}
	}

connected:
	// Auto migrate the schemas
	err := db.AutoMigrate(&postgres.GeoLevel{}, &postgres.Location{}, &postgres.NameMap{}, &postgres.Relation{})
	if err != nil {
		t.Fatalf("Failed to auto-migrate schemas: %v", err)
	}

	// Truncate all tables for a clean slate FOR EACH TEST
	tables := []string{"relations", "name_maps", "locations", "geo_levels"}
	sqlDB, _ := db.DB()
	for _, table := range tables {
		_, err := sqlDB.ExecContext(ctx, "TRUNCATE TABLE "+table+" RESTART IDENTITY CASCADE;")
		if err != nil {
			t.Fatalf("Failed to truncate table %s: %v", table, err)
		}
	}

	pgStore := postgres.Store{DB: db}
	return &ServiceOnPostgres{db: pgStore}
}

// Helper to create a GeoLevel for tests
func createTestGeoLevel(t *testing.T, service *ServiceOnPostgres, name string, rank *float64) {
	t.Helper()
	err := service.AddGeoLevel(context.Background(), name, rank)
	require.NoError(t, err, "Failed to create prerequisite GeoLevel: %s", name)
}

// Helper to create a Location for tests
func createTestLocation(t *testing.T, service *ServiceOnPostgres, geoLevel, name string) Location {
	t.Helper()
	// Ensure GeoLevel exists first (handle case where it might already exist)
	_ = service.AddGeoLevel(context.Background(), geoLevel, nil)              // Ignore error if exists
	loc, err := service.AddLocation(context.Background(), "", geoLevel, name) // geoID is ignored by AddLocation
	require.NoError(t, err, "Failed to create prerequisite Location: %s", name)
	require.NotEmpty(t, loc.GeoID, "Created location must have a GeoID")
	return loc
}

// --- Test Functions ---

func TestServiceOnPostgres_AddGeoLevel(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		level   string
		rank    *float64
		wantErr bool
		errType error // Expected error type from internal/postgres
	}{
		{
			name:    "valid geo level",
			level:   "COUNTRY",
			rank:    float64Ptr(1.0),
			wantErr: false,
		},
		{
			name:    "valid geo level with nil rank",
			level:   "STATE",
			rank:    nil,
			wantErr: false,
		},
		{
			name:    "duplicate geo level",
			level:   "COUNTRY", // Added in first test case
			rank:    float64Ptr(1.0),
			wantErr: true,
			errType: postgres.ErrGeoLevelAlreadyExists,
		},
		{
			name:    "empty name",
			level:   "",
			rank:    float64Ptr(2.0),
			wantErr: true,
			errType: postgres.ErrGeoLevelNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := service.AddGeoLevel(ctx, tt.level, tt.rank)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					// Check the underlying error type if possible
					// Note: This relies on service.db returning the raw postgres error
					underlyingErr := service.db.DB.Error
					if underlyingErr != nil && errors.Is(underlyingErr, tt.errType) {
						assert.ErrorIs(t, underlyingErr, tt.errType, "Expected error type %v, got %v", tt.errType, underlyingErr)
					} else if errors.Is(err, tt.errType) {
						assert.ErrorIs(t, err, tt.errType, "Expected error type %v, got %v", tt.errType, err)
					} else {
						// Fallback to checking the error message if ErrorIs fails
						assert.ErrorContains(t, err, tt.errType.Error())
					}
				}
			} else {
				assert.NoError(t, err)
				// Optional: Verify the level was actually created in DB if needed
				var gl postgres.GeoLevel
				dbErr := service.db.DB.Where("upper(name) = upper(?)", tt.level).First(&gl).Error
				assert.NoError(t, dbErr)
				assert.Equal(t, tt.level, gl.Name)
			}
		})
	}
}

func TestServiceOnPostgres_AddLocation(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Prerequisite: Create a GeoLevel
	createTestGeoLevel(t, service, "COUNTRY", float64Ptr(1.0))

	tests := []struct {
		name      string
		geoLevel  string
		locName   string
		wantErr   bool
		errType   error
		wantGeoID bool // Check if a non-empty GeoID is expected
		wantName  string
		wantLevel string
	}{
		{
			name:      "valid location",
			geoLevel:  "COUNTRY",
			locName:   "Test Country",
			wantErr:   false,
			wantGeoID: true,
			wantName:  "Test Country",
			wantLevel: "COUNTRY",
		},
		{
			name:      "case-insensitive geo level",
			geoLevel:  "country", // Should match 'COUNTRY'
			locName:   "Test Country 2",
			wantErr:   false,
			wantGeoID: true,
			wantName:  "Test Country 2",
			wantLevel: "COUNTRY", // Service should return the canonical name
		},
		{
			name:     "empty name",
			geoLevel: "COUNTRY",
			locName:  "",
			wantErr:  true,
			errType:  postgres.ErrNameRequired,
		},
		{
			name:     "non-existent geo level",
			geoLevel: "STATE", // Not created yet
			locName:  "Test State",
			wantErr:  true,
			errType:  postgres.ErrGeoLevelNotExist,
		},
		{
			// Note: AddLocation doesn't check for duplicate names currently
			name:      "duplicate location name (should succeed)",
			geoLevel:  "COUNTRY",
			locName:   "Test Country", // Same as first test
			wantErr:   false,
			wantGeoID: true,
			wantName:  "Test Country",
			wantLevel: "COUNTRY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// geoID input to AddLocation is ignored, pass empty string
			location, err := service.AddLocation(ctx, "", tt.geoLevel, tt.locName)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					underlyingErr := service.db.DB.Error // Check GORM's last error
					if underlyingErr != nil && errors.Is(underlyingErr, tt.errType) {
						assert.ErrorIs(t, underlyingErr, tt.errType)
					} else if errors.Is(err, tt.errType) {
						// Fallback check on returned error
						assert.ErrorIs(t, err, tt.errType)
					} else {
						// Or check contains if ErrorIs doesn't work due to wrapping
						assert.ErrorContains(t, err, tt.errType.Error())
					}
				}
				assert.Empty(t, location.GeoID) // Expect empty location on error
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, location)
				if tt.wantGeoID {
					_, uuidErr := uuid.Parse(location.GeoID)
					assert.NoError(t, uuidErr, "GeoID should be a valid UUID")
					assert.NotEmpty(t, location.GeoID)
				}
				assert.Equal(t, tt.wantName, location.Name)
				assert.Equal(t, tt.wantLevel, location.GeoLevel)
				assert.Empty(t, location.Aliases, "AddLocation should not create aliases")

				// Optional: Verify with GetLocation
				fetchedLoc, getErr := service.GetLocation(ctx, location.GeoID)
				assert.NoError(t, getErr)
				require.NotNil(t, fetchedLoc)
				assert.Equal(t, tt.wantName, fetchedLoc.Name)
				assert.Equal(t, tt.wantLevel, fetchedLoc.GeoLevel)
				assert.Empty(t, fetchedLoc.Aliases)
			}
		})
	}
}

func TestServiceOnPostgres_AddAliasToLocation(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Prerequisite: Create Location
	loc := createTestLocation(t, service, "COUNTRY", "Test Country")

	tests := []struct {
		name    string
		geoID   string
		alias   string
		wantErr bool
		errType error
	}{
		{
			name:    "valid alias",
			geoID:   loc.GeoID,
			alias:   "Alias 1",
			wantErr: false,
		},
		{
			name:    "another valid alias",
			geoID:   loc.GeoID,
			alias:   "Alias 2",
			wantErr: false,
		},
		{
			name:    "duplicate alias (should fail)",
			geoID:   loc.GeoID,
			alias:   "Alias 1", // Added in first test
			wantErr: true,
			errType: postgres.ErrNameAlreadyExists,
		},
		{
			name:    "alias same as primary name (should fail)",
			geoID:   loc.GeoID,
			alias:   "Test Country", // Primary name
			wantErr: true,
			errType: postgres.ErrNameAlreadyExists,
		},
		{
			name:    "empty alias name",
			geoID:   loc.GeoID,
			alias:   "",
			wantErr: true,
			errType: postgres.ErrNameRequired,
		},
		{
			name:    "non-existent location",
			geoID:   uuid.NewString(),
			alias:   "Some Alias",
			wantErr: true,
			errType: postgres.ErrLocationNotFound, // Foreign key violation or check in InsertNameMap
		},
		{
			name:    "invalid geoID format",
			geoID:   "not-a-uuid",
			alias:   "Some Alias",
			wantErr: true,
			// Expecting error from uuid.Parse in service layer
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := service.AddAliasToLocation(ctx, tt.geoID, tt.alias)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					underlyingErr := service.db.DB.Error
					if underlyingErr != nil && errors.Is(underlyingErr, tt.errType) {
						assert.ErrorIs(t, underlyingErr, tt.errType)
					} else if errors.Is(err, tt.errType) {
						assert.ErrorIs(t, err, tt.errType)
					} else if tt.geoID != "not-a-uuid" { // Don't check contains for uuid error
						// Fallback check contains
						assert.ErrorContains(t, err, tt.errType.Error())
					}
				} else if tt.geoID == "not-a-uuid" {
					assert.ErrorContains(t, err, "invalid UUID format")
				}
			} else {
				assert.NoError(t, err)
				// Verify alias was added
				fetchedLoc, getErr := service.GetLocation(ctx, tt.geoID)
				require.NoError(t, getErr)
				require.NotNil(t, fetchedLoc)
				assert.Contains(t, fetchedLoc.Aliases, tt.alias)
			}
		})
	}
}

func TestServiceOnPostgres_AddParent(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Prerequisites
	country := createTestLocation(t, service, "COUNTRY", "Test Country")
	state := createTestLocation(t, service, "STATE", "Test State")
	city := createTestLocation(t, service, "CITY", "Test City")

	tests := []struct {
		name        string
		childGeoID  string
		parentGeoID string
		wantErr     bool
		errType     error
	}{
		{
			name:        "valid parent relation (state -> country)",
			childGeoID:  state.GeoID,
			parentGeoID: country.GeoID,
			wantErr:     false,
		},
		{
			name:        "valid parent relation (city -> state)",
			childGeoID:  city.GeoID,
			parentGeoID: state.GeoID, // State already has country as parent, this adds city->state
			wantErr:     false,
		},
		{
			name:        "duplicate relation (state -> country)",
			childGeoID:  state.GeoID,
			parentGeoID: country.GeoID, // Added in first test
			wantErr:     true,
			errType:     postgres.ErrDuplicateRelation,
		},
		{
			name:        "invalid child geoID format",
			childGeoID:  "not-a-uuid",
			parentGeoID: country.GeoID,
			wantErr:     true,
		},
		{
			name:        "invalid parent geoID format",
			childGeoID:  state.GeoID,
			parentGeoID: "not-a-uuid",
			wantErr:     true,
		},
		{
			name:        "non-existent child location",
			childGeoID:  uuid.NewString(),
			parentGeoID: country.GeoID,
			wantErr:     true,
			errType:     postgres.ErrLocationNotFound, // Or foreign key constraint
		},
		{
			name:        "non-existent parent location",
			childGeoID:  state.GeoID,
			parentGeoID: uuid.NewString(),
			wantErr:     true,
			errType:     postgres.ErrLocationNotFound, // Or foreign key constraint
		},
		{
			name:        "relation to self (should fail)",
			childGeoID:  country.GeoID,
			parentGeoID: country.GeoID,
			wantErr:     true,
			// errType: postgres.ErrRelationToSelf, // Not defined, check message
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := service.AddParent(ctx, tt.childGeoID, tt.parentGeoID)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					if errors.Is(err, tt.errType) {
						assert.ErrorIs(t, err, tt.errType)
					} else {
						// Fallback for specific errors if ErrorIs fails
						assert.ErrorContains(t, err, tt.errType.Error())
					}
				} else if tt.name == "relation to self (should fail)" {
					// Special case for self-relation check via message
					assert.ErrorContains(t, err, "self", "Expected error message containing 'self' for self-relation")
				} else if tt.childGeoID == "not-a-uuid" || tt.parentGeoID == "not-a-uuid" {
					assert.ErrorContains(t, err, "invalid UUID format")
				}
			} else {
				assert.NoError(t, err)
				// Verify relation by checking parents/children
				parents, pErr := service.GetAllParents(ctx, tt.childGeoID)
				assert.NoError(t, pErr)
				found := false
				for _, p := range parents {
					if p.GeoID == tt.parentGeoID {
						found = true
						break
					}
				}
				assert.True(t, found, "Parent relation not found after AddParent")
			}
		})
	}
}

func TestServiceOnPostgres_AddChildren(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Prerequisites
	country := createTestLocation(t, service, "COUNTRY", "Test Country")
	state1 := createTestLocation(t, service, "STATE", "Test State 1")
	state2 := createTestLocation(t, service, "STATE", "Test State 2")
	state3 := createTestLocation(t, service, "STATE", "Test State 3")

	tests := []struct {
		name        string
		parentGeoID string
		childGeoIDs []string
		wantErr     bool
		errType     error
	}{
		{
			name:        "add multiple children",
			parentGeoID: country.GeoID,
			childGeoIDs: []string{state1.GeoID, state2.GeoID},
			wantErr:     false,
		},
		{
			name:        "add one child",
			parentGeoID: country.GeoID,
			childGeoIDs: []string{state3.GeoID}, // state1, state2 already added
			wantErr:     false,
		},
		{
			name:        "add duplicate child (should fail transaction)",
			parentGeoID: country.GeoID,
			childGeoIDs: []string{state1.GeoID}, // Already a child
			wantErr:     true,
			errType:     postgres.ErrDuplicateRelation,
		},
		{
			name:        "add mixture of new and duplicate children (should fail transaction)",
			parentGeoID: country.GeoID,
			childGeoIDs: []string{createTestLocation(t, service, "STATE", "New State").GeoID, state1.GeoID},
			wantErr:     true,
			errType:     postgres.ErrDuplicateRelation,
		},
		{
			name:        "add non-existent child (should fail transaction)",
			parentGeoID: country.GeoID,
			childGeoIDs: []string{uuid.NewString()},
			wantErr:     true,
			errType:     postgres.ErrLocationNotFound,
		},
		{
			name:        "add child with invalid uuid format (should fail transaction)",
			parentGeoID: country.GeoID,
			childGeoIDs: []string{"not-a-uuid"},
			wantErr:     true,
			// Error comes from uuid.Parse
		},
		{
			name:        "invalid parent uuid format",
			parentGeoID: "not-a-uuid",
			childGeoIDs: []string{state1.GeoID},
			wantErr:     true,
			// Error comes from uuid.Parse
		},
		{
			name:        "non-existent parent",
			parentGeoID: uuid.NewString(),
			childGeoIDs: []string{state1.GeoID},
			wantErr:     true,
			errType:     postgres.ErrLocationNotFound,
		},
		{
			name:        "add self as child (should fail transaction)",
			parentGeoID: country.GeoID,
			childGeoIDs: []string{country.GeoID},
			wantErr:     true,
			// errType: postgres.ErrRelationToSelf, // Not defined, check message
		},
		{
			name:        "empty children list",
			parentGeoID: country.GeoID,
			childGeoIDs: []string{},
			wantErr:     false, // Adding no children is a no-op, should succeed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Get initial children count to check transaction rollback on error
			initialCount := 0
			if _, err := uuid.Parse(tt.parentGeoID); err == nil {
				initialChildren, _ := service.GetAllChildren(ctx, tt.parentGeoID)
				initialCount = len(initialChildren)
			}

			err := service.AddChildren(ctx, tt.parentGeoID, tt.childGeoIDs)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					// Check the error returned by the service function
					if errors.Is(err, tt.errType) {
						assert.ErrorIs(t, err, tt.errType)
					} else {
						// Fallback contains check
						assert.ErrorContains(t, err, tt.errType.Error())
					}
				} else if tt.name == "add self as child (should fail transaction)" {
					// Special case for self-relation check via message
					assert.ErrorContains(t, err, "self", "Expected error message containing 'self' for self-relation")
				} else if tt.parentGeoID == "not-a-uuid" || containsInvalidUUID(tt.childGeoIDs) {
					assert.ErrorContains(t, err, "invalid UUID format")
				}
				// Verify transaction rollback: child count should not increase
				if _, pErr := uuid.Parse(tt.parentGeoID); pErr == nil { // Can't get children if parent ID is invalid
					finalChildren, _ := service.GetAllChildren(ctx, tt.parentGeoID)
					assert.Len(t, finalChildren, initialCount, "Child count should not change on error due to transaction rollback")
				}
			} else {
				assert.NoError(t, err)
				// Verify children were added
				finalChildren, cErr := service.GetAllChildren(ctx, tt.parentGeoID)
				assert.NoError(t, cErr)
				finalChildIDs := make([]string, len(finalChildren))
				for i, c := range finalChildren {
					finalChildIDs[i] = c.GeoID
				}
				for _, addedChildID := range tt.childGeoIDs {
					assert.Contains(t, finalChildIDs, addedChildID)
				}
				// Check count increase if new children were added
				newValidChildren := 0
				for _, cid := range tt.childGeoIDs {
					if _, err := uuid.Parse(cid); err == nil {
						newValidChildren++
					}
				}

				if newValidChildren > 0 {
					// This check assumes the test case didn't add duplicates that existed before this run
					assert.Len(t, finalChildren, initialCount+newValidChildren, "Child count check failed")
				} else {
					assert.Len(t, finalChildren, initialCount) // Count should be same if empty/invalid list added
				}
			}
		})
	}
}

// Helper for AddChildren test
func containsInvalidUUID(ids []string) bool {
	for _, id := range ids {
		if _, err := uuid.Parse(id); err != nil {
			return true
		}
	}
	return false
}

func TestServiceOnPostgres_GetLocation(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Prerequisites
	loc1 := createTestLocation(t, service, "COUNTRY", "Test Country")
	require.NoError(t, service.AddAliasToLocation(ctx, loc1.GeoID, "Alias1"))
	require.NoError(t, service.AddAliasToLocation(ctx, loc1.GeoID, "Alias2"))

	tests := []struct {
		name    string
		geoID   string
		wantErr bool
		errType error
		wantLoc *Location
	}{
		{
			name:    "get existing location with aliases",
			geoID:   loc1.GeoID,
			wantErr: false,
			wantLoc: &Location{
				GeoID:    loc1.GeoID,
				GeoLevel: "COUNTRY",
				Name:     "Test Country",
				Aliases:  []string{"Alias1", "Alias2"},
			},
		},
		{
			name:    "get non-existent location",
			geoID:   uuid.NewString(),
			wantErr: true,
			errType: postgres.ErrLocationNotFound,
		},
		{
			name:    "invalid geoID format",
			geoID:   "not-a-uuid",
			wantErr: true,
			// Error from uuid.Parse
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			location, err := service.GetLocation(ctx, tt.geoID)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				} else if tt.geoID == "not-a-uuid" {
					// Check specifically for uuid parse error message if needed
					assert.ErrorContains(t, err, "invalid UUID")
				}
				assert.Nil(t, location)
			} else {
				assert.NoError(t, err)
				require.NotNil(t, location)
				assert.Equal(t, tt.wantLoc.GeoID, location.GeoID)
				assert.Equal(t, tt.wantLoc.GeoLevel, location.GeoLevel)
				assert.Equal(t, tt.wantLoc.Name, location.Name)
				assert.ElementsMatch(t, tt.wantLoc.Aliases, location.Aliases)
			}
		})
	}
}

func TestServiceOnPostgres_GetLocations(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Prerequisites
	loc1 := createTestLocation(t, service, "COUNTRY", "Country 1")
	loc2 := createTestLocation(t, service, "STATE", "State 1")
	loc3 := createTestLocation(t, service, "COUNTRY", "Country 2")
	createTestLocation(t, service, "CITY", "City 1") // Another location not requested

	tests := []struct {
		name          string
		geoIDs        []string
		wantErr       bool
		errType       error // If one ID fails, the whole operation fails
		wantLen       int
		wantLocations []Location // Order might not be guaranteed, use ElementsMatch
	}{
		{
			name:    "get multiple existing locations",
			geoIDs:  []string{loc1.GeoID, loc3.GeoID}, // Get both countries
			wantErr: false,
			wantLen: 2,
			wantLocations: []Location{
				{GeoID: loc1.GeoID, GeoLevel: "COUNTRY", Name: "Country 1", Aliases: []string{}},
				{GeoID: loc3.GeoID, GeoLevel: "COUNTRY", Name: "Country 2", Aliases: []string{}},
			},
		},
		{
			name:    "get single existing location",
			geoIDs:  []string{loc2.GeoID},
			wantErr: false,
			wantLen: 1,
			wantLocations: []Location{
				{GeoID: loc2.GeoID, GeoLevel: "STATE", Name: "State 1", Aliases: []string{}},
			},
		},
		{
			name:    "get non-existent location among existing",
			geoIDs:  []string{loc1.GeoID, uuid.NewString()},
			wantErr: true, // Current implementation fails if any ID is not found
			errType: postgres.ErrLocationNotFound,
			wantLen: 0,
		},
		{
			name:    "get only non-existent location",
			geoIDs:  []string{uuid.NewString()},
			wantErr: true,
			errType: postgres.ErrLocationNotFound,
			wantLen: 0,
		},
		{
			name:    "get with invalid geoID format",
			geoIDs:  []string{loc1.GeoID, "not-a-uuid"},
			wantErr: true, // Fails on the first invalid UUID
			wantLen: 0,
		},
		{
			name:          "get empty list of geoIDs",
			geoIDs:        []string{},
			wantErr:       false,
			wantLen:       0,
			wantLocations: []Location{},
		},
		{
			name:          "get nil list of geoIDs",
			geoIDs:        nil,
			wantErr:       false,
			wantLen:       0,
			wantLocations: []Location{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			locations, err := service.GetLocations(ctx, tt.geoIDs)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				} else if containsInvalidUUID(tt.geoIDs) {
					assert.ErrorContains(t, err, "invalid UUID")
				}
				assert.Nil(t, locations) // Expect nil slice on error
				assert.Len(t, locations, 0)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, locations) // Expect empty slice, not nil
				assert.Len(t, locations, tt.wantLen)
				if tt.wantLen > 0 {
					// Strip Aliases if they are not populated by GetLocations implementation detail if necessary
					// But current GetLocation populates them, so GetLocations should too.
					assert.ElementsMatch(t, tt.wantLocations, locations)
				}
			}
		})
	}
}

func TestServiceOnPostgres_GetLocationsByPattern(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Prerequisites
	loc1 := createTestLocation(t, service, "COUNTRY", "United States")
	require.NoError(t, service.AddAliasToLocation(ctx, loc1.GeoID, "USA"))
	require.NoError(t, service.AddAliasToLocation(ctx, loc1.GeoID, "America"))
	loc2 := createTestLocation(t, service, "STATE", "United Kingdom") // Intentional naming clash part
	require.NoError(t, service.AddAliasToLocation(ctx, loc2.GeoID, "UK"))
	_ = createTestLocation(t, service, "COUNTRY", "Canada") // Create loc3 but ignore variable if not used later
	// loc3 := createTestLocation(t, service, "COUNTRY", "Canada") // Assign if loc3 needed below

	tests := []struct {
		name      string
		pattern   string
		wantErr   bool
		errType   error
		wantCount int
		wantIDs   []string // IDs expected in the result (order doesn't matter)
	}{
		{
			name:      "match full primary name",
			pattern:   "United States",
			wantErr:   false,
			wantCount: 1,
			wantIDs:   []string{loc1.GeoID},
		},
		{
			name:      "match partial primary name (case insensitive)",
			pattern:   "united",
			wantErr:   false,
			wantCount: 2, // Matches "United States" and "United Kingdom"
			wantIDs:   []string{loc1.GeoID, loc2.GeoID},
		},
		{
			name:      "match full alias",
			pattern:   "USA",
			wantErr:   false,
			wantCount: 1,
			wantIDs:   []string{loc1.GeoID},
		},
		{
			name:      "match partial alias (case insensitive)",
			pattern:   "merica",
			wantErr:   false,
			wantCount: 1,
			wantIDs:   []string{loc1.GeoID},
		},
		{
			name:      "no matches",
			pattern:   "NonExistent",
			wantErr:   false,
			wantCount: 0,
			wantIDs:   []string{},
		},
		{
			name:    "empty pattern",
			pattern: "",
			wantErr: true, // Underlying SearchLocationsByPattern expects non-empty pattern
			errType: postgres.ErrNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			locations, err := service.GetLocationsByPattern(ctx, tt.pattern)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				assert.Nil(t, locations)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, locations)
				assert.Len(t, locations, tt.wantCount)

				if tt.wantCount > 0 {
					foundIDs := make([]string, len(locations))
					for i, loc := range locations {
						foundIDs[i] = loc.GeoID
					}
					assert.ElementsMatch(t, tt.wantIDs, foundIDs)
					// Also check other fields are populated
					for _, loc := range locations {
						assert.NotEmpty(t, loc.GeoLevel)
						assert.NotEmpty(t, loc.Name)
						// Aliases should also be populated by the underlying GetLocation call pattern
					}
				}
			}
		})
	}
}

// Test GetAllParents and GetAllChildren requires relations to be set up
func setupRelationsForHierarchyTest(t *testing.T, service *ServiceOnPostgres) (country, state, city Location) {
	country = createTestLocation(t, service, "COUNTRY", "Test Country")
	state = createTestLocation(t, service, "STATE", "Test State")
	city = createTestLocation(t, service, "CITY", "Test City")
	town := createTestLocation(t, service, "TOWN", "Test Town") // Child of city
	createTestGeoLevel(t, service, "TOWN", float64Ptr(4.0))     // Ensure TOWN level exists

	// Create hierarchy: Country -> State -> City -> Town
	require.NoError(t, service.AddParent(context.Background(), state.GeoID, country.GeoID))
	require.NoError(t, service.AddParent(context.Background(), city.GeoID, state.GeoID))
	require.NoError(t, service.AddParent(context.Background(), town.GeoID, city.GeoID))

	// Add aliases to check they are populated
	require.NoError(t, service.AddAliasToLocation(context.Background(), country.GeoID, "TC"))
	require.NoError(t, service.AddAliasToLocation(context.Background(), state.GeoID, "TS"))
	require.NoError(t, service.AddAliasToLocation(context.Background(), city.GeoID, "TCity"))

	return country, state, city // Return key locations
}

func TestServiceOnPostgres_GetAllParents(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()
	country, state, city := setupRelationsForHierarchyTest(t, service)

	tests := []struct {
		name        string
		geoID       string
		wantErr     bool
		errType     error
		wantCount   int
		wantParents []Location                // Expected parents (order might matter depending on impl, check with ElementsMatch)
		setup       func(t *testing.T) string // Optional setup for this specific test case
	}{
		{
			name:      "get parents of city",
			geoID:     city.GeoID,
			wantErr:   false,
			wantCount: 1, // Direct parent only based on current implementation
			wantParents: []Location{
				{GeoID: state.GeoID, GeoLevel: "STATE", Name: "Test State", Aliases: []string{"TS"}},
			},
		},
		{
			name:      "get parents of state",
			geoID:     state.GeoID,
			wantErr:   false,
			wantCount: 1,
			wantParents: []Location{
				{GeoID: country.GeoID, GeoLevel: "COUNTRY", Name: "Test Country", Aliases: []string{"TC"}},
			},
		},
		{
			name:        "get parents of country (should be none)",
			geoID:       country.GeoID,
			wantErr:     false,
			wantCount:   0,
			wantParents: []Location{},
		},
		{
			name:        "non-existent location",
			geoID:       uuid.NewString(),
			wantErr:     false, // GetParents returns empty list for non-existent ID, service reflects this
			wantCount:   0,
			wantParents: []Location{},
		},
		{
			name:    "invalid geoID format",
			geoID:   "not-a-uuid",
			wantErr: true, // Error from uuid.Parse
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			currentGeoID := tt.geoID
			if tt.setup != nil {
				currentGeoID = tt.setup(t)
			}

			parents, err := service.GetAllParents(ctx, currentGeoID)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				} else if currentGeoID == "not-a-uuid" {
					assert.ErrorContains(t, err, "invalid UUID")
				}
				assert.Nil(t, parents)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, parents) // Should be empty slice, not nil
				assert.Len(t, parents, tt.wantCount)
				if tt.wantCount > 0 {
					assert.ElementsMatch(t, tt.wantParents, parents)
					// Verify details within the returned parents
					for _, p := range parents {
						assert.NotEmpty(t, p.GeoID)
						assert.NotEmpty(t, p.GeoLevel)
						assert.NotEmpty(t, p.Name)
						// Check aliases populated
						fetchedParent, fpErr := service.GetLocation(ctx, p.GeoID)
						require.NoError(t, fpErr)
						assert.ElementsMatch(t, fetchedParent.Aliases, p.Aliases)
					}
				}
			}
		})
	}
}

func TestServiceOnPostgres_GetAllChildren(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()
	country, state, city := setupRelationsForHierarchyTest(t, service)
	// Get the town ID as well
	towns, _ := service.GetAllChildren(ctx, city.GeoID)
	require.Len(t, towns, 1)
	town := towns[0]

	tests := []struct {
		name         string
		geoID        string
		wantErr      bool
		errType      error
		wantCount    int
		wantChildren []Location                // Expected children
		setup        func(t *testing.T) string // Optional setup
	}{
		{
			name:      "get children of country",
			geoID:     country.GeoID,
			wantErr:   false,
			wantCount: 1, // Direct children only
			wantChildren: []Location{
				{GeoID: state.GeoID, GeoLevel: "STATE", Name: "Test State", Aliases: []string{"TS"}},
			},
		},
		{
			name:      "get children of state",
			geoID:     state.GeoID,
			wantErr:   false,
			wantCount: 1,
			wantChildren: []Location{
				{GeoID: city.GeoID, GeoLevel: "CITY", Name: "Test City", Aliases: []string{"TCity"}},
			},
		},
		{
			name:      "get children of city",
			geoID:     city.GeoID,
			wantErr:   false,
			wantCount: 1,
			wantChildren: []Location{
				{GeoID: town.GeoID, GeoLevel: "TOWN", Name: "Test Town", Aliases: []string{}}, // Town has no alias
			},
		},
		{
			name:         "get children of town (should be none)",
			geoID:        town.GeoID,
			wantErr:      false,
			wantCount:    0,
			wantChildren: []Location{},
		},
		{
			name:         "non-existent location",
			geoID:        uuid.NewString(),
			wantErr:      false, // GetChildren returns empty list, service reflects this
			wantCount:    0,
			wantChildren: []Location{},
		},
		{
			name:    "invalid geoID format",
			geoID:   "not-a-uuid",
			wantErr: true, // Error from uuid.Parse
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			currentGeoID := tt.geoID
			if tt.setup != nil {
				currentGeoID = tt.setup(t)
			}

			children, err := service.GetAllChildren(ctx, currentGeoID)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				} else if currentGeoID == "not-a-uuid" {
					assert.ErrorContains(t, err, "invalid UUID")
				}
				assert.Nil(t, children)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, children)
				assert.Len(t, children, tt.wantCount)
				if tt.wantCount > 0 {
					assert.ElementsMatch(t, tt.wantChildren, children)
					// Verify details within the returned children
					for _, c := range children {
						assert.NotEmpty(t, c.GeoID)
						assert.NotEmpty(t, c.GeoLevel)
						assert.NotEmpty(t, c.Name)
						// Check aliases populated
						fetchedChild, fcErr := service.GetLocation(ctx, c.GeoID)
						require.NoError(t, fcErr)
						assert.ElementsMatch(t, fetchedChild.Aliases, c.Aliases)
					}
				}
			}
		})
	}
}

func TestServiceOnPostgres_UpdateLocation(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Prerequisites
	loc1 := createTestLocation(t, service, "COUNTRY", "Initial Name")
	createTestGeoLevel(t, service, "REGION", float64Ptr(0.5)) // For testing geo level change

	initialGeoLevel := "COUNTRY"
	newGeoLevel := "REGION"
	newName := "Updated Name"

	tests := []struct {
		name           string
		geoID          string                    // If set, use this ID. If setup is present, setup provides the ID.
		setup          func(t *testing.T) string // Returns the geoID to use for the test
		updateName     *string
		updateGeoLevel *string
		wantErr        bool
		errType        error
		wantLocation   Location // Expected state after update (GeoID filled in dynamically)
	}{
		{
			name:           "update only name",
			geoID:          loc1.GeoID,
			updateName:     &newName,
			updateGeoLevel: nil,
			wantErr:        false,
			wantLocation:   Location{Name: newName, GeoLevel: initialGeoLevel, Aliases: []string{}},
		},
		{
			name:           "update only geo level",
			geoID:          loc1.GeoID, // Name is now "Updated Name" from previous test on same entity
			updateName:     nil,
			updateGeoLevel: &newGeoLevel,
			wantErr:        false,
			// Aliases are not touched by update, so they persist from loc1's creation (empty in this case)
			wantLocation: Location{Name: newName, GeoLevel: newGeoLevel, Aliases: []string{}},
		},
		{
			// Reset state for next test
			setup: func(t *testing.T) string {
				loc := createTestLocation(t, service, "COUNTRY", "Another Initial")
				return loc.GeoID
			},
			name:           "update both name and geo level",
			updateName:     &newName,
			updateGeoLevel: &newGeoLevel,
			wantErr:        false,
			wantLocation:   Location{Name: newName, GeoLevel: newGeoLevel, Aliases: []string{}}, // GeoID set dynamically
		},
		{
			setup: func(t *testing.T) string {
				loc := createTestLocation(t, service, "COUNTRY", "Yet Another")
				return loc.GeoID
			},
			name:           "update name to empty string (should fail)",
			updateName:     stringPtr(""),
			updateGeoLevel: nil,
			wantErr:        true,
			errType:        postgres.ErrNameRequired,
		},
		{
			setup: func(t *testing.T) string {
				loc := createTestLocation(t, service, "COUNTRY", "Update Fail")
				return loc.GeoID
			},
			name:           "update geo level to non-existent",
			updateName:     nil,
			updateGeoLevel: stringPtr("NON_EXISTENT_LEVEL"),
			wantErr:        true,
			errType:        postgres.ErrGeoLevelNotExist,
		},
		{
			name:           "update non-existent location",
			geoID:          uuid.NewString(),
			updateName:     &newName,
			updateGeoLevel: nil,
			wantErr:        true,
			errType:        postgres.ErrLocationNotFound,
		},
		{
			name:           "update with invalid geoID format",
			geoID:          "not-a-uuid",
			updateName:     &newName,
			updateGeoLevel: nil,
			wantErr:        true, // Error from uuid.Parse
		},
		{
			// Test updating name to one that exists as an alias for *another* location
			setup: func(t *testing.T) string {
				locToUpdate := createTestLocation(t, service, "COUNTRY", "Target Loc")
				otherLoc := createTestLocation(t, service, "STATE", "Other Loc")
				require.NoError(t, service.AddAliasToLocation(ctx, otherLoc.GeoID, "Existing Alias"))
				return locToUpdate.GeoID
			},
			name:           "update name to existing alias of another location (should succeed)",
			updateName:     stringPtr("Existing Alias"),
			updateGeoLevel: nil,
			wantErr:        false,                                                                      // Name/alias uniqueness is checked within a location, not globally by default
			wantLocation:   Location{Name: "Existing Alias", GeoLevel: "COUNTRY", Aliases: []string{}}, // GeoID set dynamically
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			currentGeoID := tt.geoID
			if tt.setup != nil {
				currentGeoID = tt.setup(t)
			}
			// Fetch initial state for comparison if needed, or rely on wantLocation
			initialLoc, initialGetErr := service.GetLocation(ctx, currentGeoID)
			if errors.Is(initialGetErr, postgres.ErrLocationNotFound) {
				initialLoc = nil // Ensure initialLoc is nil if ID doesn't exist yet
			}

			updatedLoc, err := service.UpdateLocation(ctx, currentGeoID, tt.updateName, tt.updateGeoLevel)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					underlyingErr := service.db.DB.Error
					if underlyingErr != nil && errors.Is(underlyingErr, tt.errType) {
						assert.ErrorIs(t, underlyingErr, tt.errType)
					} else if errors.Is(err, tt.errType) {
						assert.ErrorIs(t, err, tt.errType)
					} else if currentGeoID != "not-a-uuid" {
						if tt.errType == postgres.ErrGeoLevelNotExist || tt.errType == postgres.ErrNameRequired || tt.errType == postgres.ErrLocationNotFound {
							assert.ErrorContains(t, err, tt.errType.Error())
						} else {
							// Fallback for other potential errors if needed
						}
					}
				} else if currentGeoID == "not-a-uuid" {
					assert.ErrorContains(t, err, "invalid UUID format")
				}
				// Check that the location wasn't actually changed in the DB on error
				if currentGeoID != "not-a-uuid" && tt.errType != postgres.ErrLocationNotFound && initialLoc != nil {
					locAfterError, _ := service.GetLocation(ctx, currentGeoID)
					// Compare relevant fields that might have been attempted to change
					if initialLoc != nil && locAfterError != nil {
						assert.Equal(t, initialLoc.Name, locAfterError.Name, "Location name should not change on update error")
						assert.Equal(t, initialLoc.GeoLevel, locAfterError.GeoLevel, "Location geo level should not change on update error")
					}
				}

			} else {
				assert.NoError(t, err)
				require.NotNil(t, updatedLoc)

				// Fill in dynamic GeoID for comparison
				expected := tt.wantLocation
				expected.GeoID = currentGeoID
				// Carry over aliases if not explicitly modified (UpdateLocation doesn't touch aliases)
				if initialLoc != nil {
					expected.Aliases = initialLoc.Aliases
				}

				assert.Equal(t, expected.GeoID, updatedLoc.GeoID)
				assert.Equal(t, expected.Name, updatedLoc.Name)
				assert.Equal(t, expected.GeoLevel, updatedLoc.GeoLevel)
				assert.ElementsMatch(t, expected.Aliases, updatedLoc.Aliases)

				// Verify with GetLocation again
				fetchedLoc, getErr := service.GetLocation(ctx, currentGeoID)
				assert.NoError(t, getErr)
				require.NotNil(t, fetchedLoc)
				assert.Equal(t, expected.Name, fetchedLoc.Name)
				assert.Equal(t, expected.GeoLevel, fetchedLoc.GeoLevel)
				assert.ElementsMatch(t, expected.Aliases, fetchedLoc.Aliases) // Aliases should persist
			}
		})
	}
}

func TestServiceOnPostgres_UpdateGeoLevel(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Prerequisites
	createTestGeoLevel(t, service, "COUNTRY", float64Ptr(1.0))
	createTestGeoLevel(t, service, "STATE", float64Ptr(2.0)) // For duplicate name test

	newName := "NATION"
	newRank := 0.5
	existingRankOfState := 2.0 // Rank of STATE

	tests := []struct {
		name        string
		currentName string
		setup       func(t *testing.T) // Optional setup for specific case
		updateName  *string
		updateRank  *float64
		wantErr     bool
		errType     error
	}{
		{
			name:        "update only name",
			currentName: "COUNTRY",
			updateName:  &newName,
			updateRank:  nil,
			wantErr:     false,
		},
		{
			name:        "update only rank",
			currentName: newName, // Name is now NATION from previous test run
			updateName:  nil,
			updateRank:  &newRank,
			wantErr:     false,
		},
		{
			// Reset state
			setup: func(t *testing.T) {
				createTestGeoLevel(t, service, "REGION", float64Ptr(3.0))
			},
			name:        "update both name and rank",
			currentName: "REGION",
			updateName:  stringPtr("AREA"),
			updateRank:  float64Ptr(4.0),
			wantErr:     false,
		},
		{
			setup: func(t *testing.T) {
				createTestGeoLevel(t, service, "PROVINCE", float64Ptr(5.0))
			},
			name:        "update name to empty string (should fail)",
			currentName: "PROVINCE",
			updateName:  stringPtr(""),
			updateRank:  nil,
			wantErr:     true,
			errType:     postgres.ErrGeoLevelNameRequired,
		},
		{
			setup: func(t *testing.T) {
				createTestGeoLevel(t, service, "DISTRICT", float64Ptr(6.0))
			},
			name:        "update name to existing name (should fail)",
			currentName: "DISTRICT",
			updateName:  stringPtr("STATE"), // STATE already exists
			updateRank:  nil,
			wantErr:     true,
			errType:     postgres.ErrGeoLevelAlreadyExists, // Check constraint or UpdateGeoLevel logic
		},
		{
			// Note: postgres.UpdateGeoLevel might not check rank uniqueness by default
			// Depending on its implementation, this test might pass or fail
			setup: func(t *testing.T) {
				createTestGeoLevel(t, service, "COUNTY", float64Ptr(7.0))
			},
			name:        "update rank to existing rank (may or may not fail)",
			currentName: "COUNTY",
			updateName:  nil,
			updateRank:  &existingRankOfState, // Rank of STATE
			wantErr:     false,                // Assuming rank doesn't have unique constraint
			// errType: postgres.ErrGeoLevelRankExists, // If rank is unique
		},
		{
			name:        "update non-existent geo level",
			currentName: "NON_EXISTENT",
			updateName:  stringPtr("NEW_NAME"),
			updateRank:  nil,
			wantErr:     true,
			errType:     postgres.ErrGeoLevelNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { // ***** Finish implementation from here *****
			if tt.setup != nil {
				tt.setup(t)
			}
			// Fetch initial state for verification on error if needed
			var initialGL postgres.GeoLevel
			_ = service.db.DB.Where("upper(name) = upper(?)", tt.currentName).First(&initialGL)

			err := service.UpdateGeoLevel(ctx, tt.currentName, tt.updateName, tt.updateRank)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					underlyingErr := service.db.DB.Error // GORM might set error here
					if underlyingErr != nil && errors.Is(underlyingErr, tt.errType) {
						assert.ErrorIs(t, underlyingErr, tt.errType)
					} else if errors.Is(err, tt.errType) {
						assert.ErrorIs(t, err, tt.errType)
					} else {
						// Fallback contains check
						assert.ErrorContains(t, err, tt.errType.Error())
					}
				}
				// Verify state hasn't changed on error
				var finalGL postgres.GeoLevel
				dbErr := service.db.DB.Where("upper(name) = upper(?)", tt.currentName).First(&finalGL).Error
				if dbErr == nil && initialGL.Id != uuid.Nil { // If the original record existed
					assert.Equal(t, initialGL.Name, finalGL.Name)
					assert.Equal(t, initialGL.Rank, finalGL.Rank)
				}
				// Also check that the intended new name wasn't accidentally created
				if tt.updateName != nil && *tt.updateName != "" {
					var count int64
					service.db.DB.Model(&postgres.GeoLevel{}).Where("upper(name) = upper(?)", *tt.updateName).Count(&count)
					assert.Zero(t, count, "GeoLevel with the new name should not exist after failed update")
				}

			} else {
				assert.NoError(t, err)
				// Verify the update happened
				targetName := tt.currentName
				if tt.updateName != nil && *tt.updateName != "" {
					targetName = *tt.updateName
				}
				// Need a way to get GeoLevel to verify, maybe add GetGeoLevel to service/store?
				// For now, assume success if no error
				// Optional: Check DB directly
				var gl postgres.GeoLevel
				dbErr := service.db.DB.Where("upper(name) = upper(?)", targetName).First(&gl).Error
				assert.NoError(t, dbErr, "Failed to fetch GeoLevel after update")
				if tt.updateRank != nil {
					require.NotNil(t, gl.Rank, "Rank should not be nil after update")
					assert.Equal(t, *tt.updateRank, *gl.Rank)
				} else if initialGL.Id != uuid.Nil && initialGL.Rank != nil { // Check rank wasn't cleared if not updated
					require.NotNil(t, gl.Rank, "Rank should persist if not updated")
					assert.Equal(t, *initialGL.Rank, *gl.Rank)
				}

				if tt.updateName != nil && *tt.updateName != "" {
					assert.Equal(t, *tt.updateName, gl.Name)
				} else {
					assert.Equal(t, tt.currentName, gl.Name) // Name should remain the same
				}

				// Verify old name is gone if name was updated
				if tt.updateName != nil && *tt.updateName != tt.currentName {
					var count int64
					service.db.DB.Model(&postgres.GeoLevel{}).Where("upper(name) = upper(?)", tt.currentName).Count(&count)
					assert.Zero(t, count, "GeoLevel with the old name should not exist after name update")
				}
			}
		})
	}
}

// --- Start of new tests ---

func TestServiceOnPostgres_RemoveAlias(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Prerequisites
	loc1 := createTestLocation(t, service, "COUNTRY", "Location With Aliases")
	require.NoError(t, service.AddAliasToLocation(ctx, loc1.GeoID, "Alias1"))
	require.NoError(t, service.AddAliasToLocation(ctx, loc1.GeoID, "Alias2"))

	tests := []struct {
		name    string
		geoID   string
		alias   string
		wantErr bool
		errType error
	}{
		{
			name:    "remove existing alias",
			geoID:   loc1.GeoID,
			alias:   "Alias1",
			wantErr: false,
		},
		{
			name:    "remove another existing alias",
			geoID:   loc1.GeoID,
			alias:   "Alias2",
			wantErr: false,
		},
		{
			name:    "attempt to remove primary name (should fail)",
			geoID:   loc1.GeoID,
			alias:   "Location With Aliases", // Primary name
			wantErr: true,
			errType: postgres.ErrCannotDeletePrimary,
		},
		{
			name:    "invalid geoID format",
			geoID:   "not-a-uuid",
			alias:   "SomeAlias",
			wantErr: true, // Error from uuid.Parse
		},
		{
			name:    "remove empty alias name (should fail)",
			geoID:   loc1.GeoID,
			alias:   "",
			wantErr: true,
			errType: postgres.ErrNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := service.RemoveAlias(ctx, tt.geoID, tt.alias)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					underlyingErr := service.db.DB.Error
					if underlyingErr != nil && errors.Is(underlyingErr, tt.errType) {
						assert.ErrorIs(t, underlyingErr, tt.errType)
					} else if errors.Is(err, tt.errType) {
						assert.ErrorIs(t, err, tt.errType)
					} else if tt.geoID != "not-a-uuid" {
						// Fallback check contains only if ErrorIs fails and not a UUID error
						if !errors.Is(err, tt.errType) {
							assert.ErrorContains(t, err, tt.errType.Error())
						}
					}
				} else if tt.geoID == "not-a-uuid" {
					assert.ErrorContains(t, err, "invalid UUID format")
				}
			} else {
				assert.NoError(t, err)
				// Verify alias was removed
				if _, err := uuid.Parse(tt.geoID); err == nil { // Only verify if GeoID was valid
					fetchedLoc, getErr := service.GetLocation(ctx, tt.geoID)
					require.NoError(t, getErr)
					require.NotNil(t, fetchedLoc)
					assert.NotContains(t, fetchedLoc.Aliases, tt.alias)
				}
			}
		})
	}
}

func TestServiceOnPostgres_RemoveParent(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Prerequisite hierarchy setup within each test case needing it for isolation

	tests := []struct {
		name        string
		setup       func(t *testing.T) (childID, parentID string) // Setup returns IDs for the test
		childGeoID  string                                        // Use if setup is nil
		parentGeoID string                                        // Use if setup is nil
		wantErr     bool
		errType     error // Expecting service level fmt.Errorf or potentially a specific error type
	}{
		{
			name: "remove existing parent (city -> state)",
			setup: func(t *testing.T) (string, string) {
				_, state, city := setupRelationsForHierarchyTest(t, service)
				return city.GeoID, state.GeoID
			},
			wantErr: false,
		},
		{
			name: "remove existing parent (state -> country)",
			setup: func(t *testing.T) (string, string) {
				country, state, _ := setupRelationsForHierarchyTest(t, service)
				return state.GeoID, country.GeoID
			},
			wantErr: false,
		},
		{
			name: "remove non-existent parent relation (country has no parent)",
			setup: func(t *testing.T) (string, string) {
				country, state, _ := setupRelationsForHierarchyTest(t, service)
				return country.GeoID, state.GeoID // Try removing state as parent of country
			},
			wantErr: true, // Service returns fmt.Errorf("relation not found...")
			// errType: postgres.ErrRelationNotFound, // Ideally would be this
		},
		{
			name: "remove relation that was already removed (city -> state)",
			setup: func(t *testing.T) (string, string) {
				_, state, city := setupRelationsForHierarchyTest(t, service)
				// Remove it once
				require.NoError(t, service.RemoveParent(ctx, city.GeoID, state.GeoID))
				return city.GeoID, state.GeoID // Try removing again
			},
			wantErr: true, // Service returns fmt.Errorf("relation not found...")
		},
		{
			name: "remove from non-existent child",
			setup: func(t *testing.T) (string, string) {
				_, state, _ := setupRelationsForHierarchyTest(t, service)
				return uuid.NewString(), state.GeoID
			},
			wantErr: true, // Service returns fmt.Errorf("relation not found...") because GetParents is empty
		},
		{
			name: "remove non-existent parent ID from existing child",
			setup: func(t *testing.T) (string, string) {
				_, _, city := setupRelationsForHierarchyTest(t, service)
				return city.GeoID, uuid.NewString()
			},
			wantErr: true, // Service returns fmt.Errorf("relation not found...")
		},
		{
			name:        "invalid child geoID format",
			childGeoID:  "not-a-uuid",
			parentGeoID: uuid.NewString(), // Need a valid UUID otherwise test is ambiguous
			wantErr:     true,             // Error from uuid.Parse
		},
		{
			name:        "invalid parent geoID format",
			childGeoID:  uuid.NewString(), // Need a valid UUID
			parentGeoID: "not-a-uuid",
			wantErr:     true, // Error from uuid.Parse
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			childID := tt.childGeoID
			parentID := tt.parentGeoID
			if tt.setup != nil {
				childID, parentID = tt.setup(t)
			}

			err := service.RemoveParent(ctx, childID, parentID)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					// Current service implementation returns fmt.Errorf, so ErrorIs won't work directly
					assert.ErrorIs(t, err, tt.errType) // Check if underlying error matches
					if !errors.Is(err, tt.errType) {
						assert.ErrorContains(t, err, "relation not found") // Check for the specific message if ErrorIs fails
					}
				} else if childID == "not-a-uuid" || parentID == "not-a-uuid" {
					assert.ErrorContains(t, err, "invalid UUID")
				} else {
					// Default check for relation not found message for fmt.Errorf cases
					assert.ErrorContains(t, err, "relation not found")
				}
			} else {
				assert.NoError(t, err)
				// Verify parent was removed
				if _, err := uuid.Parse(childID); err == nil { // Check only if childID was valid
					parents, pErr := service.GetAllParents(ctx, childID)
					assert.NoError(t, pErr)
					found := false
					for _, p := range parents {
						if p.GeoID == parentID {
							found = true
							break
						}
					}
					assert.False(t, found, "Parent relation should be removed")
				}
			}
		})
	}
}

func TestServiceOnPostgres_RemoveChildren(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Define setup function for reuse
	setupFunc := func(t *testing.T) (parentID string, childIDs []string) {
		country := createTestLocation(t, service, "COUNTRY", "ParentCountry")
		state1 := createTestLocation(t, service, "STATE", "ChildState1")
		state2 := createTestLocation(t, service, "STATE", "ChildState2")
		require.NoError(t, service.AddChildren(ctx, country.GeoID, []string{state1.GeoID, state2.GeoID}))
		return country.GeoID, []string{state1.GeoID, state2.GeoID}
	}

	nonExistentChildID := uuid.NewString()

	tests := []struct {
		name                string
		setup               func(t *testing.T) (parentID string, childIDs []string) // Returns parent and *initial* children
		parentGeoID         string                                                  // Used if setup is nil
		childGeoIDsToRemove []string                                                // Children to attempt removing
		wantErr             bool
		errType             error
		wantRemainingIDs    []string // IDs expected to remain children after removal attempt
	}{
		{
			name:  "remove single existing child",
			setup: setupFunc,
			// parentGeoID set by setup
			childGeoIDsToRemove: []string{ /* childIDs[0] set dynamically */ },
			wantErr:             false,
			wantRemainingIDs:    []string{ /* childIDs[1] set dynamically */ },
		},
		{
			name:  "remove multiple existing children",
			setup: setupFunc,
			// parentGeoID, childGeoIDsToRemove set by setup
			wantErr:          false,
			wantRemainingIDs: []string{},
		},
		{
			name:  "remove non-existent child among existing",
			setup: setupFunc,
			// parentGeoID set by setup
			childGeoIDsToRemove: []string{ /* childIDs[0] set dynamically */ nonExistentChildID},
			wantErr:             false,                                         // Service ignores non-matching children, only removes existing ones
			wantRemainingIDs:    []string{ /* childIDs[1] set dynamically */ }, // Only child 0 removed
		},
		{
			name:  "remove only non-existent child",
			setup: setupFunc,
			// parentGeoID set by setup
			childGeoIDsToRemove: []string{nonExistentChildID},
			wantErr:             false,                                                      // Service finds no matching children to delete, succeeds (no-op)
			wantRemainingIDs:    []string{ /* childIDs[0], childIDs[1] set dynamically */ }, // Both children should remain
		},
		{
			name:  "remove list containing invalid uuid",
			setup: setupFunc,
			// parentGeoID set by setup
			childGeoIDsToRemove: []string{ /* childIDs[0] set dynamically */ "not-a-uuid"},
			wantErr:             false,                                         // Service likely ignores invalid UUIDs in the loop, removes valid ones
			wantRemainingIDs:    []string{ /* childIDs[1] set dynamically */ }, // child 0 removed
		},
		{
			name:                "remove children from non-existent parent",
			parentGeoID:         uuid.NewString(),
			childGeoIDsToRemove: []string{uuid.NewString()}, // Doesn't matter which valid ID
			wantErr:             false,                      // GetChildren returns empty, loop doesn't run, commit succeeds
			wantRemainingIDs:    []string{},                 // Not applicable as parent doesn't exist
		},
		{
			name:                "invalid parent geoID format",
			parentGeoID:         "not-a-uuid",
			childGeoIDsToRemove: []string{uuid.NewString()},
			wantErr:             true, // Error from uuid.Parse
		},
		{
			name:  "remove empty list of children",
			setup: setupFunc,
			// parentGeoID set by setup
			childGeoIDsToRemove: []string{},
			wantErr:             false,                                                      // No-op, should succeed
			wantRemainingIDs:    []string{ /* childIDs[0], childIDs[1] set dynamically */ }, // Both children should remain
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parentID := tt.parentGeoID
			childrenToRemove := make([]string, len(tt.childGeoIDsToRemove))
			copy(childrenToRemove, tt.childGeoIDsToRemove) // Copy slice
			expectedRemaining := make([]string, len(tt.wantRemainingIDs))
			copy(expectedRemaining, tt.wantRemainingIDs) // Copy slice
			var initialChildIDs []string

			if tt.setup != nil {
				parentID, initialChildIDs = tt.setup(t)
				// Dynamically adjust childrenToRemove and expectedRemaining based on setup and test case intent
				if tt.name == "remove single existing child" {
					childrenToRemove = []string{initialChildIDs[0]}
					expectedRemaining = []string{initialChildIDs[1]}
				} else if tt.name == "remove multiple existing children" {
					childrenToRemove = []string{initialChildIDs[0], initialChildIDs[1]}
					expectedRemaining = []string{}
				} else if tt.name == "remove non-existent child among existing" {
					childrenToRemove = []string{initialChildIDs[0], nonExistentChildID}
					expectedRemaining = []string{initialChildIDs[1]}
				} else if tt.name == "remove only non-existent child" {
					childrenToRemove = []string{nonExistentChildID}
					expectedRemaining = []string{initialChildIDs[0], initialChildIDs[1]}
				} else if tt.name == "remove list containing invalid uuid" {
					childrenToRemove = []string{initialChildIDs[0], "not-a-uuid"}
					expectedRemaining = []string{initialChildIDs[1]}
				} else if tt.name == "remove empty list of children" {
					childrenToRemove = []string{}
					expectedRemaining = []string{initialChildIDs[0], initialChildIDs[1]}
				}
			}

			err := service.RemoveChildren(ctx, parentID, childrenToRemove)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				} else if parentID == "not-a-uuid" {
					assert.ErrorContains(t, err, "invalid UUID")
				}
			} else {
				assert.NoError(t, err)
				// Verify children were removed / correct ones remain
				if _, err := uuid.Parse(parentID); err == nil { // Check only if parent was valid
					finalChildren, cErr := service.GetAllChildren(ctx, parentID)
					assert.NoError(t, cErr)
					finalChildIDs := make([]string, len(finalChildren))
					for i, c := range finalChildren {
						finalChildIDs[i] = c.GeoID
					}
					assert.ElementsMatch(t, expectedRemaining, finalChildIDs)
				}
			}
		})
	}
}

func TestServiceOnPostgres_DeleteLocation(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Setup function for reuse
	setupHierarchyForDelete := func(t *testing.T) (country, state, city Location) {
		country = createTestLocation(t, service, "COUNTRY", "ToDelete Country")
		state = createTestLocation(t, service, "STATE", "ToDelete State")
		city = createTestLocation(t, service, "CITY", "ToDelete City")
		require.NoError(t, service.AddAliasToLocation(ctx, country.GeoID, "AliasC"))
		require.NoError(t, service.AddAliasToLocation(ctx, state.GeoID, "AliasS"))
		require.NoError(t, service.AddParent(ctx, state.GeoID, country.GeoID))
		require.NoError(t, service.AddParent(ctx, city.GeoID, state.GeoID))
		return country, state, city
	}

	tests := []struct {
		name          string
		setup         func(t *testing.T) string // Returns ID to delete
		geoIDToDelete string                    // Used if setup is nil
		wantErr       bool
		errType       error
		verifyDeleted func(t *testing.T, service *ServiceOnPostgres, deletedID string)
		verifyCascade func(t *testing.T, service *ServiceOnPostgres, deletedID string)
	}{
		{
			name: "delete leaf location (city)",
			setup: func(t *testing.T) string {
				_, _, city := setupHierarchyForDelete(t)
				return city.GeoID
			},
			wantErr:       false,
			verifyDeleted: verifyLocationAndNamesDeleted,
			verifyCascade: verifyRelationsDeleted,
		},
		{
			name: "delete middle location (state)",
			setup: func(t *testing.T) string {
				_, state, _ := setupHierarchyForDelete(t)
				return state.GeoID
			},
			wantErr:       false,
			verifyDeleted: verifyLocationAndNamesDeleted,
			verifyCascade: verifyRelationsDeleted, // Should delete state->country and city->state relations
		},
		{
			name: "delete root location (country)",
			setup: func(t *testing.T) string {
				country, _, _ := setupHierarchyForDelete(t)
				return country.GeoID
			},
			wantErr:       false,
			verifyDeleted: verifyLocationAndNamesDeleted,
			verifyCascade: verifyRelationsDeleted, // Should delete state->country relation
		},
		{
			name:          "delete non-existent location",
			geoIDToDelete: uuid.NewString(),
			wantErr:       true, // DeleteLocation checks if location exists
			errType:       postgres.ErrLocationNotFound,
		},
		{
			name:          "invalid geoID format",
			geoIDToDelete: "not-a-uuid",
			wantErr:       true, // Error from uuid.Parse
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idToDelete := tt.geoIDToDelete
			if tt.setup != nil {
				idToDelete = tt.setup(t)
			}

			err := service.DeleteLocation(ctx, idToDelete)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				} else if idToDelete == "not-a-uuid" {
					assert.ErrorContains(t, err, "invalid UUID")
				}
			} else {
				assert.NoError(t, err)
				// Verify deletion using helper funcs
				if _, err := uuid.Parse(idToDelete); err == nil { // Check only if ID was valid
					if tt.verifyDeleted != nil {
						tt.verifyDeleted(t, service, idToDelete)
					}
					if tt.verifyCascade != nil {
						tt.verifyCascade(t, service, idToDelete)
					}
				}
			}
		})
	}
}

// Helper verification function for DeleteLocation tests
func verifyLocationAndNamesDeleted(t *testing.T, service *ServiceOnPostgres, deletedID string) {
	t.Helper()
	// Verify location is gone
	_, err := service.GetLocation(context.Background(), deletedID)
	assert.ErrorIs(t, err, postgres.ErrLocationNotFound, "Location should not be found after delete")

	// Verify names are gone (check DB directly)
	var nameCount int64
	id, _ := uuid.Parse(deletedID)
	dbErr := service.db.DB.Model(&postgres.NameMap{}).Where("location_id = ?", id).Count(&nameCount).Error
	assert.NoError(t, dbErr)
	assert.Equal(t, int64(0), nameCount, "NameMap entries should be deleted")
}

// Helper verification function for DeleteLocation tests
func verifyRelationsDeleted(t *testing.T, service *ServiceOnPostgres, deletedID string) {
	t.Helper()
	// Verify relations are gone (check DB directly)
	var relationCount int64
	id, _ := uuid.Parse(deletedID)
	dbErr := service.db.DB.Model(&postgres.Relation{}).
		Where("parent_id = ? OR child_id = ?", id, id).
		Count(&relationCount).Error
	assert.NoError(t, dbErr)
	assert.Equal(t, int64(0), relationCount, "Relations involving the deleted location should be removed")
}

func TestServiceOnPostgres_GetChildrenAtLevel(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Setup: Country -> State1 (STATE), State2 (STATE); State1 -> City1 (CITY), City2 (CITY); State2 -> Borough1 (BOROUGH)
	setupHierarchyForLevelTest := func(t *testing.T) (country, state1, state2, city1, city2, borough1 Location) {
		country = createTestLocation(t, service, "COUNTRY", "Test Country")    // Level 1 (Assume)
		state1 = createTestLocation(t, service, "STATE", "Test State 1")       // Level 2
		state2 = createTestLocation(t, service, "STATE", "Test State 2")       // Level 2
		city1 = createTestLocation(t, service, "CITY", "Test City 1")          // Level 3
		city2 = createTestLocation(t, service, "CITY", "Test City 2")          // Level 3
		createTestGeoLevel(t, service, "BOROUGH", float64Ptr(3.0))             // Define Borough level if not exists
		borough1 = createTestLocation(t, service, "BOROUGH", "Test Borough 1") // Level 3 (different level name)

		require.NoError(t, service.AddChildren(ctx, country.GeoID, []string{state1.GeoID, state2.GeoID}))
		require.NoError(t, service.AddChildren(ctx, state1.GeoID, []string{city1.GeoID, city2.GeoID}))
		require.NoError(t, service.AddChildren(ctx, state2.GeoID, []string{borough1.GeoID}))
		return country, state1, state2, city1, city2, borough1
	}
	country, state1, state2, city1, city2, borough1 := setupHierarchyForLevelTest(t)

	tests := []struct {
		name        string
		parentGeoID string
		levelName   string
		wantErr     bool
		errType     error
		wantCount   int
		wantIDs     []string // Expected GeoIDs of children at that level
	}{
		{
			name:        "get children of country at STATE level",
			parentGeoID: country.GeoID,
			levelName:   "STATE",
			wantErr:     false,
			wantCount:   2,
			wantIDs:     []string{state1.GeoID, state2.GeoID},
		},
		{
			name:        "get children of country at CITY level (should be none)",
			parentGeoID: country.GeoID,
			levelName:   "CITY",
			wantErr:     false,
			wantCount:   0,
			wantIDs:     []string{},
		},
		{
			name:        "get children of state1 at CITY level",
			parentGeoID: state1.GeoID,
			levelName:   "CITY",
			wantErr:     false,
			wantCount:   2,
			wantIDs:     []string{city1.GeoID, city2.GeoID},
		},
		{
			name:        "get children of state2 at BOROUGH level",
			parentGeoID: state2.GeoID,
			levelName:   "BOROUGH",
			wantErr:     false,
			wantCount:   1,
			wantIDs:     []string{borough1.GeoID},
		},
		{
			name:        "get children of state1 at non-existent level",
			parentGeoID: state1.GeoID,
			levelName:   "NON_EXISTENT_LEVEL",
			wantErr:     false, // Service filters by name, returns empty if level name doesn't match any children
			wantCount:   0,
			wantIDs:     []string{},
		},
		{
			name:        "get children of leaf node (city1)",
			parentGeoID: city1.GeoID,
			levelName:   "ANY_LEVEL", // Level name doesn't matter if there are no children
			wantErr:     false,
			wantCount:   0,
			wantIDs:     []string{},
		},
		{
			name:        "get children from non-existent parent",
			parentGeoID: uuid.NewString(),
			levelName:   "STATE",
			wantErr:     false, // GetChildren returns empty, result is empty
			wantCount:   0,
			wantIDs:     []string{},
		},
		{
			name:        "invalid parent geoID format",
			parentGeoID: "not-a-uuid",
			levelName:   "STATE",
			wantErr:     true, // Error from uuid.Parse
		},
		{
			name:        "empty level name", // Should likely return empty as no level name matches ""
			parentGeoID: country.GeoID,
			levelName:   "",
			wantErr:     false,
			wantCount:   0,
			wantIDs:     []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			children, err := service.GetChildrenAtLevel(ctx, tt.parentGeoID, tt.levelName)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				} else if tt.parentGeoID == "not-a-uuid" {
					assert.ErrorContains(t, err, "invalid UUID")
				}
				assert.Nil(t, children)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, children)
				assert.Len(t, children, tt.wantCount)
				if tt.wantCount > 0 {
					foundIDs := make([]string, len(children))
					for i, c := range children {
						foundIDs[i] = c.GeoID
						assert.Equal(t, tt.levelName, c.GeoLevel, "Child GeoLevel should match requested level")
						// Note: Current implementation doesn't populate Name/Aliases here
						assert.Empty(t, c.Name)
						assert.Empty(t, c.Aliases)
					}
					assert.ElementsMatch(t, tt.wantIDs, foundIDs)
				}
			}
		})
	}
}

func TestServiceOnPostgres_GetParentAtLevel(t *testing.T) {
	service := setupTestDB(t)
	ctx := context.Background()

	// Setup: Country -> State -> City -> Town
	setupHierarchyForParentLevelTest := func(t *testing.T) (country, state, city, town Location) {
		country = createTestLocation(t, service, "COUNTRY", "Test Country")
		state = createTestLocation(t, service, "STATE", "Test State")
		city = createTestLocation(t, service, "CITY", "Test City")
		createTestGeoLevel(t, service, "TOWN", float64Ptr(4.0))
		town = createTestLocation(t, service, "TOWN", "Test Town")
		require.NoError(t, service.AddParent(ctx, state.GeoID, country.GeoID))
		require.NoError(t, service.AddParent(ctx, city.GeoID, state.GeoID))
		require.NoError(t, service.AddParent(ctx, town.GeoID, city.GeoID))
		return country, state, city, town
	}
	country, state, city, town := setupHierarchyForParentLevelTest(t)

	tests := []struct {
		name         string
		childGeoID   string
		levelName    string
		wantErr      bool
		errType      error  // Expecting service level fmt.Errorf or potentially a specific error type
		wantParentID string // Expected GeoID of parent at that level
	}{
		{
			name:         "get parent of town at CITY level",
			childGeoID:   town.GeoID,
			levelName:    "CITY",
			wantErr:      false,
			wantParentID: city.GeoID,
		},
		{
			name:         "get parent of city at STATE level",
			childGeoID:   city.GeoID,
			levelName:    "STATE",
			wantErr:      false,
			wantParentID: state.GeoID,
		},
		{
			name:         "get parent of state at COUNTRY level",
			childGeoID:   state.GeoID,
			levelName:    "COUNTRY",
			wantErr:      false,
			wantParentID: country.GeoID,
		},
		{
			name:       "get parent of city at COUNTRY level (should fail)",
			childGeoID: city.GeoID, // Parent is STATE
			levelName:  "COUNTRY",
			wantErr:    true, // Service returns fmt.Errorf("parent at level %s not found", ...)
			// errType: postgres.ErrRelationNotFound, // Or similar
		},
		{
			name:       "get parent of root node (country)",
			childGeoID: country.GeoID,
			levelName:  "ANY_LEVEL",
			wantErr:    true, // No parents exist
		},
		{
			name:       "get parent at non-existent level name",
			childGeoID: city.GeoID,
			levelName:  "NON_EXISTENT_LEVEL",
			wantErr:    true, // No parent matches this level name
		},
		{
			name:       "get parent from non-existent child",
			childGeoID: uuid.NewString(),
			levelName:  "STATE",
			wantErr:    true, // GetParents returns empty, loop doesn't find match
		},
		{
			name:       "invalid child geoID format",
			childGeoID: "not-a-uuid",
			levelName:  "STATE",
			wantErr:    true, // Error from uuid.Parse
		},
		{
			name:       "empty level name",
			childGeoID: city.GeoID,
			levelName:  "",
			wantErr:    true, // No parent matches empty level name
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent, err := service.GetParentAtLevel(ctx, tt.childGeoID, tt.levelName)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					// assert.ErrorIs(t, err, tt.errType) // ErrorIs won't work with fmt.Errorf
					assert.ErrorContains(t, err, "not found")
				} else if tt.childGeoID == "not-a-uuid" {
					assert.ErrorContains(t, err, "invalid UUID")
				} else {
					// Default check for not found message
					assert.ErrorContains(t, err, "not found")
				}
				assert.Nil(t, parent)
			} else {
				assert.NoError(t, err)
				require.NotNil(t, parent)
				assert.Equal(t, tt.wantParentID, parent.GeoID)
				assert.Equal(t, tt.levelName, parent.GeoLevel)
				// Note: Current implementation doesn't populate Name/Aliases here
				assert.Empty(t, parent.Name)
				assert.Empty(t, parent.Aliases)
			}
		})
	}
}

// Helper functions for pointers used in tests

func float64Ptr(f float64) *float64 {
	return &f
}

func stringPtr(s string) *string {
	return &s
}
