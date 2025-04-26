package postgres

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func setupTestDB(t *testing.T) *Store {
	// Start the test postgres container using docker-compose
	cmd := exec.Command("docker", "compose", "-f", "../../test.docker-compose.yaml", "up", "-d")
	err := cmd.Run()
	require.NoError(t, err, "Failed to start test postgres container")

	// Clean up the container when the test finishes
	t.Cleanup(func() {
		cmd := exec.Command("docker", "compose", "-f", "../../test.docker-compose.yaml", "down")
		err := cmd.Run()
		if err != nil {
			t.Logf("Failed to stop test postgres container: %v", err)
		}
	})

	// Wait for the database to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var db *gorm.DB
	// Try to connect to the database with retries
	for {
		select {
		case <-ctx.Done():
			require.Fail(t, "Timed out waiting for database to be ready")
			return nil
		default:
			// Connection string for the test database
			dsn := "host=localhost user=postgres password=postgres dbname=test_location port=5432 sslmode=disable TimeZone=Asia/Kolkata"
			var err error
			db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
			if err == nil {
				// Successfully connected
				break
			}
			// Wait a bit before retrying
			time.Sleep(1 * time.Second)
		}
		if db != nil {
			break
		}
	}

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
