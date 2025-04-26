package postgres

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func setupTestDB(t *testing.T) *Store {
	// Connection string for the test database
	// This assumes you have PostgreSQL running locally with a 'test_location' database
	// and a user 'postgres' with password 'postgres'
	dsn := "host=localhost user=postgres password=postgres dbname=test_location port=5432 sslmode=disable TimeZone=Asia/Kolkata"

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)

	// Clean the database by truncating all tables
	// This ensures a clean state before each test
	tables := []string{"relations", "name_maps", "locations", "geo_levels"}
	for _, table := range tables {
		err := db.Exec("TRUNCATE TABLE " + table + " CASCADE").Error
		if err != nil {
			// If tables don't exist yet, that's okay
			if !strings.Contains(err.Error(), "does not exist") {
				require.NoError(t, err)
			}
		}
	}

	// Auto migrate the schemas
	err = db.AutoMigrate(&GeoLevel{}, &Location{}, &NameMap{}, &Relation{})
	require.NoError(t, err)

	return &Store{DB: db}
}

func float64Ptr(v float64) *float64 {
	return &v
}
