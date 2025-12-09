package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
)

const (
	AlgorithmBcrypt   = "bcrypt"
	AlgorithmArgon2id = "argon2id"

	defaultArgon2Time        = uint32(1)
	defaultArgon2MemoryKB    = uint32(8 * 1024) // 8 MB to reduce per-hash memory pressure
	defaultArgon2Parallelism = uint8(1)
	defaultArgon2KeyLength   = uint32(32)
	defaultArgon2SaltLength  = uint32(16)
)

// HashConfig 哈希加密配置结构体
// 用于配置不同哈希算法（如 bcrypt、argon2id 等）的参数，适配各类密码/数据加密场景
type HashConfig struct {
	Algorithm         string // 哈希算法名称，支持的取值："bcrypt"、"argon2id" 等
	BcryptCost        int    // bcrypt 算法的加密成本因子（取值范围 4~31，值越大加密越慢、安全性越高）
	Argon2Time        uint32 // Argon2 算法的时间成本（迭代次数），推荐值 3~4
	Argon2MemoryKB    uint32 // Argon2 算法的内存成本（单位：KB），推荐值 65536（64MB）
	Argon2Parallelism uint8  // Argon2 算法的并行度（线程数），推荐值 1~4
	Argon2KeyLength   uint32 // Argon2 算法生成的密钥长度（单位：字节），推荐值 32
	Argon2SaltLength  uint32 // Argon2 算法使用的盐值长度（单位：字节），推荐值 16
}

// Encrypt encrypts the plain text with the default configuration (bcrypt).
func Encrypt(source string) (string, error) {
	return EncryptWithConfig(source, HashConfig{})
}

// EncryptWithCost maintains backward compatibility for bcrypt-only usage.
func EncryptWithCost(source string, cost int) (string, error) {
	return EncryptWithConfig(source, HashConfig{Algorithm: AlgorithmBcrypt, BcryptCost: cost})
}

// EncryptWithConfig encrypts the plain text using the supplied configuration.
func EncryptWithConfig(source string, cfg HashConfig) (string, error) {
	normalized, err := normalizeHashConfig(cfg)
	if err != nil {
		return "", err
	}

	switch normalized.Algorithm {
	case AlgorithmBcrypt:
		hashedBytes, err := bcrypt.GenerateFromPassword([]byte(source), normalized.BcryptCost)
		if err != nil {
			return "", err
		}
		return string(hashedBytes), nil
	case AlgorithmArgon2id:
		return encryptArgon2id(source, normalized)
	default:
		return "", fmt.Errorf("unsupported password hash algorithm: %s", normalized.Algorithm)
	}
}

// Compare compares the encrypted text with the plain text if it's the same.
func Compare(hashedPassword, password string) error {
	if strings.HasPrefix(hashedPassword, "$argon2id$") {
		return compareArgon2id(hashedPassword, password)
	}
	return bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
}

// Sign issue a jwt token based on secretID, secretKey, iss and aud.
func Sign(secretID string, secretKey string, iss, aud string) string {
	claims := jwt.MapClaims{
		"exp": time.Now().Add(time.Minute).Unix(),
		"iat": time.Now().Unix(),
		"nbf": time.Now().Add(0).Unix(),
		"aud": aud,
		"iss": iss,
	}

	// create a new token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = secretID

	// Sign the token with the specified secret.
	tokenString, _ := token.SignedString([]byte(secretKey))

	return tokenString
}

func normalizeHashConfig(cfg HashConfig) (HashConfig, error) {
	normalized := cfg
	algorithm := strings.ToLower(strings.TrimSpace(normalized.Algorithm))
	if algorithm == "" {
		algorithm = AlgorithmBcrypt
	}
	normalized.Algorithm = algorithm

	switch normalized.Algorithm {
	case AlgorithmBcrypt:
		switch {
		case normalized.BcryptCost <= 0:
			normalized.BcryptCost = bcrypt.DefaultCost
		case normalized.BcryptCost < bcrypt.MinCost:
			normalized.BcryptCost = bcrypt.MinCost
		case normalized.BcryptCost > bcrypt.MaxCost:
			normalized.BcryptCost = bcrypt.MaxCost
		}
	case AlgorithmArgon2id:
		if normalized.Argon2Time == 0 {
			normalized.Argon2Time = defaultArgon2Time
		}
		if normalized.Argon2MemoryKB == 0 {
			normalized.Argon2MemoryKB = defaultArgon2MemoryKB
		}
		if normalized.Argon2Parallelism == 0 {
			normalized.Argon2Parallelism = defaultArgon2Parallelism
		}
		if normalized.Argon2KeyLength == 0 {
			normalized.Argon2KeyLength = defaultArgon2KeyLength
		}
		if normalized.Argon2SaltLength == 0 {
			normalized.Argon2SaltLength = defaultArgon2SaltLength
		}
	default:
		return HashConfig{}, fmt.Errorf("unknown password hash algorithm: %s", normalized.Algorithm)
	}

	return normalized, nil
}

func encryptArgon2id(plain string, cfg HashConfig) (string, error) {
	salt := make([]byte, cfg.Argon2SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate argon2 salt: %w", err)
	}

	hash := argon2.IDKey([]byte(plain), salt, cfg.Argon2Time, cfg.Argon2MemoryKB, cfg.Argon2Parallelism, cfg.Argon2KeyLength)

	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)

	encoded := fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d,l=%d$%s$%s", argon2.Version, cfg.Argon2MemoryKB, cfg.Argon2Time, cfg.Argon2Parallelism, cfg.Argon2KeyLength, b64Salt, b64Hash)
	return encoded, nil
}

func compareArgon2id(hashed, password string) error {
	parts := strings.Split(hashed, "$")
	if len(parts) != 6 {
		return errors.New("invalid argon2 hash format")
	}

	paramsPart := parts[3]
	saltPart := parts[4]
	hashPart := parts[5]

	params, err := parseArgon2Params(paramsPart)
	if err != nil {
		return err
	}

	salt, err := base64.RawStdEncoding.DecodeString(saltPart)
	if err != nil {
		return fmt.Errorf("decode argon2 salt: %w", err)
	}
	expectedHash, err := base64.RawStdEncoding.DecodeString(hashPart)
	if err != nil {
		return fmt.Errorf("decode argon2 hash: %w", err)
	}

	derived := argon2.IDKey([]byte(password), salt, params.time, params.memoryKB, params.parallelism, params.keyLen)

	if subtle.ConstantTimeCompare(derived, expectedHash) == 1 {
		return nil
	}
	return bcrypt.ErrMismatchedHashAndPassword
}

type argon2Params struct {
	memoryKB    uint32
	time        uint32
	parallelism uint8
	keyLen      uint32
}

func parseArgon2Params(raw string) (argon2Params, error) {
	// raw example: m=65536,t=1,p=2,l=32
	params := argon2Params{}
	sections := strings.Split(raw, ",")
	for _, section := range sections {
		kv := strings.SplitN(section, "=", 2)
		if len(kv) != 2 {
			return params, fmt.Errorf("invalid argon2 parameter segment: %s", section)
		}
		key := kv[0]
		value := kv[1]
		switch key {
		case "m":
			v, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return params, fmt.Errorf("invalid argon2 memory value: %w", err)
			}
			params.memoryKB = uint32(v)
		case "t":
			v, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return params, fmt.Errorf("invalid argon2 time value: %w", err)
			}
			params.time = uint32(v)
		case "p":
			v, err := strconv.ParseUint(value, 10, 8)
			if err != nil {
				return params, fmt.Errorf("invalid argon2 parallelism value: %w", err)
			}
			params.parallelism = uint8(v)
		case "l":
			v, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return params, fmt.Errorf("invalid argon2 key length value: %w", err)
			}
			params.keyLen = uint32(v)
		}
	}

	if params.memoryKB == 0 {
		params.memoryKB = defaultArgon2MemoryKB
	}
	if params.time == 0 {
		params.time = defaultArgon2Time
	}
	if params.parallelism == 0 {
		params.parallelism = defaultArgon2Parallelism
	}
	if params.keyLen == 0 {
		params.keyLen = defaultArgon2KeyLength
	}
	return params, nil
}
