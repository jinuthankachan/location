package postgres

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func setupTestDB(t *testing.T) *Store {
	// Check if the test postgres container is already running
	psCmd := exec.Command("docker", "compose", "-f", "../test.docker-compose.yaml", "ps", "--status=running")
	psOut, psErr := psCmd.Output()
	if psErr != nil || !strings.Contains(string(psOut), "test-location-postgres") {
		// Not running, so start the test postgres container using docker-compose
		upCmd := exec.Command("docker", "compose", "-f", "../test.docker-compose.yaml", "up", "-d", "--wait")
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
			db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
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
	err := db.AutoMigrate(&GeoLevel{}, &Location{}, &NameMap{}, &Relation{})
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

	return &Store{DB: db}
}

func float64Ptr(v float64) *float64 {
	return &v
}

func stringPtr(v string) *string {
	return &v
}
