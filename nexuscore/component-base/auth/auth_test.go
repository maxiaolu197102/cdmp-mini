package auth

import (
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestEncryptWithConfig_Argon2idRoundTrip(t *testing.T) {
	password := "Sup3r$ecret!"
	cfg := HashConfig{Algorithm: AlgorithmArgon2id}

	hash, err := EncryptWithConfig(password, cfg)
	if err != nil {
		t.Fatalf("EncryptWithConfig returned error: %v", err)
	}

	if err := Compare(hash, password); err != nil {
		t.Fatalf("Compare returned unexpected error for valid password: %v", err)
	}

	err = Compare(hash, "wrong-password")
	if err == nil {
		t.Fatalf("Compare should fail for incorrect password")
	}
	if err.Error() != bcrypt.ErrMismatchedHashAndPassword.Error() {
		t.Fatalf("Compare returned unexpected error type: %v", err)
	}
}
