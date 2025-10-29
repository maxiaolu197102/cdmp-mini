package user

import (
	"context"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	if err := db.Exec("DROP TABLE IF EXISTS user").Error; err != nil {
		t.Fatalf("failed to cleanup user table: %v", err)
	}

	createTable := `CREATE TABLE user (
        name TEXT PRIMARY KEY,
        email TEXT,
        phone TEXT,
        status INTEGER
    )`
	if err := db.Exec(createTable).Error; err != nil {
		t.Fatalf("failed to create user table: %v", err)
	}
	return db
}

func TestPreflightConflicts_NoMatches(t *testing.T) {
	db := setupTestDB(t)
	store := NewUsers(db, nil)

	ctx := context.Background()
	result, err := store.PreflightConflicts(ctx, "alice", "alice@example.com", "13800000000", nil)
	if err != nil {
		t.Fatalf("PreflightConflicts returned error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty result, got %v", result)
	}
}

func TestPreflightConflicts_ReturnsMatches(t *testing.T) {
	db := setupTestDB(t)
	insert := `INSERT INTO user (name, email, phone, status) VALUES (?, ?, ?, ?)`
	rows := [][]interface{}{
		{"alice", "alice@example.com", "+8613800000000", 1},
		{"bob", "bob@example.com", "13900000000", 1},
	}
	for _, row := range rows {
		if err := db.Exec(insert, row...).Error; err != nil {
			t.Fatalf("failed to insert seed row: %v", err)
		}
	}

	store := NewUsers(db, nil)
	ctx := context.Background()
	result, err := store.PreflightConflicts(ctx, "alice", "Bob@Example.com", " 13900000000 ", nil)
	if err != nil {
		t.Fatalf("PreflightConflicts returned error: %v", err)
	}
	if _, ok := result["username"]; !ok {
		t.Fatalf("expected username match for existing alice")
	}
	emailHit, ok := result["email"]
	if !ok {
		t.Fatalf("expected email conflict entry")
	}
	if emailHit.Name != "bob" {
		t.Fatalf("expected email conflict with bob, got %s", emailHit.Name)
	}
	phoneHit, ok := result["phone"]
	if !ok {
		t.Fatalf("expected phone conflict entry")
	}
	if phoneHit.Name != "bob" {
		t.Fatalf("expected phone conflict with bob, got %s", phoneHit.Name)
	}
}
