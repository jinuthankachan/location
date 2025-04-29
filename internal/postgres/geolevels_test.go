package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGeoLevel_InsertGeoLevel(t *testing.T) {
	store := setupTestDB(t)
	ctx := context.Background()

	tests := []struct {
		name  string
		input struct {
			name string
			rank *float64
		}
		wantErr bool
		errType error
	}{
		{
			name: "valid geo level uppercase",
			input: struct {
				name string
				rank *float64
			}{
				name: "COUNTRY",
				rank: float64Ptr(1.0),
			},
			wantErr: false,
		},
		{
			name: "valid geo level lowercase conversion",
			input: struct {
				name string
				rank *float64
			}{
				name: "state",
				rank: float64Ptr(2.0),
			},
			wantErr: false,
		},
		{
			name: "empty name",
			input: struct {
				name string
				rank *float64
			}{
				name: "",
				rank: float64Ptr(1.0),
			},
			wantErr: true,
			errType: ErrGeoLevelNameRequired,
		},
		{
			name: "null rank",
			input: struct {
				name string
				rank *float64
			}{
				name: "DISTRICT",
				rank: nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geoLevel, err := store.InsertGeoLevel(ctx, tt.input.name, tt.input.rank)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, geoLevel)
			assert.Equal(t, strings.ToUpper(tt.input.name), geoLevel.Name)
			if tt.input.rank != nil {
				assert.Equal(t, *tt.input.rank, *geoLevel.Rank)
			} else {
				assert.Nil(t, geoLevel.Rank)
			}

			// Verify uniqueness constraint
			_, err = store.InsertGeoLevel(ctx, tt.input.name, tt.input.rank)
			assert.Error(t, err)
			assert.True(t, errors.Is(err, ErrGeoLevelAlreadyExists))
		})
	}
}

func TestGeoLevel_GetGeoLevelByName(t *testing.T) {
	store := setupTestDB(t)
	ctx := context.Background()

	// Insert test data
	expectedLevel := &GeoLevel{
		Name: "COUNTRY",
		Rank: float64Ptr(1.0),
	}
	level, err := store.InsertGeoLevel(ctx, expectedLevel.Name, expectedLevel.Rank)
	require.NoError(t, err)
	expectedLevel.Id = level.Id

	tests := []struct {
		name    string
		input   string
		want    *GeoLevel
		wantErr bool
		errType error
	}{
		{
			name:    "existing level exact match",
			input:   "COUNTRY",
			want:    expectedLevel,
			wantErr: false,
		},
		{
			name:    "existing level case insensitive",
			input:   "country",
			want:    expectedLevel,
			wantErr: false,
		},
		{
			name:    "non-existent level",
			input:   "CITY",
			want:    nil,
			wantErr: true,
			errType: ErrGeoLevelNotFound,
		},
		{
			name:    "empty name",
			input:   "",
			want:    nil,
			wantErr: true,
			errType: ErrGeoLevelNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geoLevel, err := store.GetGeoLevelByName(ctx, tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, geoLevel)
			assert.Equal(t, strings.ToUpper(tt.input), geoLevel.Name)
			assert.Equal(t, tt.want.Id, geoLevel.Id)
		})
	}
}

func TestGeoLevel_GetGeoLevelByPattern(t *testing.T) {
	store := setupTestDB(t)
	ctx := context.Background()

	// Setup test data
	testLevels := []struct {
		name string
		rank *float64
	}{
		{"COUNTRY", float64Ptr(1.0)},
		{"STATE", float64Ptr(2.0)},
		{"DISTRICT", float64Ptr(3.0)},
	}

	for _, level := range testLevels {
		_, err := store.InsertGeoLevel(ctx, level.name, level.rank)
		require.NoError(t, err)
	}

	tests := []struct {
		name     string
		pattern  string
		wantName string
		wantErr  bool
		errType  error
	}{
		{
			name:     "exact match",
			pattern:  "COUNTRY",
			wantName: "COUNTRY",
			wantErr:  false,
		},
		{
			name:     "partial match start",
			pattern:  "COU",
			wantName: "COUNTRY",
			wantErr:  false,
		},
		{
			name:     "partial match middle",
			pattern:  "STAT",
			wantName: "STATE",
			wantErr:  false,
		},
		{
			name:     "case insensitive",
			pattern:  "district",
			wantName: "DISTRICT",
			wantErr:  false,
		},
		{
			name:    "no match",
			pattern: "CITY",
			wantErr: true,
			errType: ErrGeoLevelNotFound,
		},
		{
			name:    "empty pattern",
			pattern: "",
			wantErr: true,
			errType: ErrGeoLevelNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geoLevel, err := store.GetGeoLevelsByPattern(ctx, tt.pattern)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, geoLevel)
			assert.Equal(t, tt.wantName, geoLevel[0].Name)
		})
	}
}

func TestGeoLevel_UpdateGeoLevel(t *testing.T) {
	store := setupTestDB(t)
	ctx := context.Background()

	// Setup initial geo levels
	initRank := float64Ptr(1.0)
	_, err := store.InsertGeoLevel(ctx, "COUNTRY", initRank)
	require.NoError(t, err)

	_, err = store.InsertGeoLevel(ctx, "STATE", float64Ptr(2.0))
	require.NoError(t, err)

	tests := []struct {
		name  string
		input struct {
			name    string
			newName *string
			newRank *float64
		}
		wantErr bool
		errType error
	}{
		{
			name: "update rank only",
			input: struct {
				name    string
				newName *string
				newRank *float64
			}{
				name:    "COUNTRY",
				newName: nil,
				newRank: float64Ptr(1.5),
			},
			wantErr: false,
		},
		{
			name: "update name only",
			input: struct {
				name    string
				newName *string
				newRank *float64
			}{
				name:    "COUNTRY",
				newName: stringPtr("NATION"),
				newRank: nil,
			},
			wantErr: false,
		},
		{
			name: "update to existing name",
			input: struct {
				name    string
				newName *string
				newRank *float64
			}{
				name:    "NATION",
				newName: stringPtr("STATE"),
				newRank: nil,
			},
			wantErr: true,
			errType: ErrGeoLevelAlreadyExists,
		},
		{
			name: "update non-existent level",
			input: struct {
				name    string
				newName *string
				newRank *float64
			}{
				name:    "CITY",
				newName: stringPtr("TOWN"),
				newRank: nil,
			},
			wantErr: true,
			errType: ErrGeoLevelNotFound,
		},
		{
			name: "update with empty name",
			input: struct {
				name    string
				newName *string
				newRank *float64
			}{
				name:    "",
				newName: stringPtr("NEW_NAME"),
				newRank: nil,
			},
			wantErr: true,
			errType: ErrGeoLevelNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geoLevel, err := store.UpdateGeoLevel(ctx, tt.input.name, tt.input.newName, tt.input.newRank)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, geoLevel)

			if tt.input.newName != nil {
				assert.Equal(t, strings.ToUpper(*tt.input.newName), geoLevel.Name)
			}
			if tt.input.newRank != nil {
				assert.Equal(t, *tt.input.newRank, *geoLevel.Rank)
			}

			// Verify the update by getting the geo level
			if tt.input.newName != nil {
				updated, err := store.GetGeoLevelByName(ctx, *tt.input.newName)
				assert.NoError(t, err)
				assert.Equal(t, geoLevel.Id, updated.Id)
			}
		})
	}
}

func TestGeoLevel_DeleteGeoLevel(t *testing.T) {
	store := setupTestDB(t)
	ctx := context.Background()

	// Setup test data
	_, err := store.InsertGeoLevel(ctx, "COUNTRY", float64Ptr(1.0))
	require.NoError(t, err)

	// Insert a geo level that will be referenced by a location
	referencedLevel, err := store.InsertGeoLevel(ctx, "STATE", float64Ptr(2.0))
	require.NoError(t, err)

	// Create a location that references the geo level
	location := &Location{
		GeoLevelID: referencedLevel.Id,
	}
	err = store.DB.Create(location).Error
	require.NoError(t, err)

	tests := []struct {
		name    string
		input   string
		wantErr bool
		errType error
	}{
		{
			name:    "delete existing unused level",
			input:   "COUNTRY",
			wantErr: false,
		},
		{
			name:    "delete referenced level",
			input:   "STATE",
			wantErr: true,
			errType: ErrGeoLevelInUse,
		},
		{
			name:    "delete non-existent level",
			input:   "CITY",
			wantErr: true,
			errType: ErrGeoLevelNotFound,
		},
		{
			name:    "delete with empty name",
			input:   "",
			wantErr: true,
			errType: ErrGeoLevelNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.DeleteGeoLevel(ctx, tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				return
			}

			assert.NoError(t, err)

			// Verify deletion
			_, err = store.GetGeoLevelByName(ctx, tt.input)
			assert.ErrorIs(t, err, ErrGeoLevelNotFound)
		})
	}
}
