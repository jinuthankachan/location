package postgres

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func setupTestDB(t *testing.T) *Store {
	// Start the test postgres container using docker-compose
	cmd := exec.Command("docker", "compose", "-f", "../../test.docker-compose.yaml", "up", "-d")
	err := cmd.Run()
	if err != nil {
		t.Logf("WARNING: Failed to start test postgres container: %v", err)
	}

	// Clean up the container when the test finishes
	t.Cleanup(func() {
		cmd := exec.Command("docker", "compose", "-f", "../../test.docker-compose.yaml", "down")
		err := cmd.Run()
		if err != nil {
			t.Logf("WARNING: Failed to stop test postgres container: %v", err)
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
			t.Fatalf("Timed out waiting for database to be ready")
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
			// Log warning instead of failing
			t.Logf("WARNING: failed to connect to test DB (will retry): %v", err)
			// Wait a bit before retrying
			time.Sleep(1 * time.Second)
		}
		if db != nil {
			break
		}
	}

	// Auto migrate the schemas
	err = db.AutoMigrate(&GeoLevel{}, &Location{}, &NameMap{}, &Relation{})
	if err != nil {
		t.Fatalf("Failed to auto-migrate schemas: %v", err)
	}

	return &Store{DB: db}
}

func float64Ptr(v float64) *float64 {
	return &v
}
