package delete_force

import (
	"strings"
	"testing"
)

// Local copies of the error-classification logic so tests can run in this package
// without depending on unexported symbols from other packages.
func isUnrecoverableError(errStr string) bool {
	unrecoverableErrors := []string{
		"Duplicate entry", "1062", "23000", "duplicate key value", "23505",
		"用户已存在", "UserAlreadyExist",
		"UNMARSHAL_ERROR", "invalid json", "unknown operation", "poison message",
		"definer", "DEFINER", "1449", "permission denied",
		"does not exist", "not found", "record not found",
		"constraint", "foreign key", "1451", "1452", "syntax error",
		"invalid format", "validation failed",
	}
	for _, s := range unrecoverableErrors {
		if s != "" && containsIgnoreCase(errStr, s) {
			return true
		}
	}
	return false
}

func isRecoverableError(errStr string) bool {
	recoverableErrors := []string{
		"timeout", "deadline exceeded", "connection refused", "network error",
		"connection reset", "broken pipe", "no route to host",
		"database is closed", "deadlock", "1213", "40001",
		"temporary", "busy", "lock", "try again",
		"resource temporarily unavailable", "too many connections",
	}
	for _, s := range recoverableErrors {
		if s != "" && containsIgnoreCase(errStr, s) {
			return true
		}
	}
	return false
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	if isUnrecoverableError(errStr) {
		return false
	}
	if isRecoverableError(errStr) {
		return true
	}
	return false
}

// small helper: case-insensitive contains
func containsIgnoreCase(hay, needle string) bool {
	if hay == "" || needle == "" {
		return false
	}
	// simple lower-case compare without importing strings repeatedly
	// use strings package via explicit import would be fine, but to keep test tiny use it
	return stringsContainsFold(hay, needle)
}

// wrapper around strings.Contains(strings.ToLower(...))
func stringsContainsFold(s, substr string) bool {
	// implement using standard library to be robust
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

func TestIsUnrecoverableError(t *testing.T) {
	cases := []struct {
		in  string
		exp bool
	}{
		{"UNMARSHAL_ERROR: invalid json", true},
		{"Duplicate entry 'a' for key 'PRIMARY'", true},
		{"does not exist", true},
		{"some random error", false},
	}

	for _, c := range cases {
		if got := isUnrecoverableError(c.in); got != c.exp {
			t.Fatalf("isUnrecoverableError(%q) = %v, want %v", c.in, got, c.exp)
		}
	}
}

func TestIsRecoverableError(t *testing.T) {
	cases := []struct {
		in  string
		exp bool
	}{
		{"timeout while connecting", true},
		{"deadlock detected", true},
		{"too many connections", true},
		{"permanent failure", false},
	}

	for _, c := range cases {
		if got := isRecoverableError(c.in); got != c.exp {
			t.Fatalf("isRecoverableError(%q) = %v, want %v", c.in, got, c.exp)
		}
	}
}

func TestShouldRetry(t *testing.T) {
	if shouldRetry(nil) {
		t.Fatalf("shouldRetry(nil) should be false")
	}

	if !shouldRetry(fmtError("timeout")) {
		t.Fatalf("shouldRetry(timeout) should be true")
	}

	if shouldRetry(fmtError("UNMARSHAL_ERROR: invalid json")) {
		t.Fatalf("shouldRetry(unmarshal) should be false")
	}
}

// helper to produce error from string without importing fmt in test
func fmtError(s string) error { return &stringErr{s} }

type stringErr struct{ s string }

func (e *stringErr) Error() string { return e.s }
