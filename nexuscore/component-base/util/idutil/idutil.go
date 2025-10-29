/*
这个包 idutil 提供了生成各种唯一标识符和密钥的工具函数。它结合了 Sonyflake（分布式 ID 生成算法）、Hashids（生成短哈希）和密码学安全的随机数生成器。

包摘要
函数名	功能描述	返回值类型	关键特性
GetIntID	生成一个全局唯一的 64 位整数 ID。	uint64	基于 Sonyflake，结合机器 IP 地址，保证分布式系统下的唯一性。
GetInstanceID	生成一个带指定前缀的、可读的短实例 ID（如 secret-2v69o5）。	string	使用 Hashids 将整数 ID 编码为短字符串，并进行反转，增加不可预测性。前缀用于标识类型。
GetUUID36	生成一个更短的、36 进制字母数字的 UUID（如 300m50zn91nwz5）。	string	使用 Hashids 和 Alphabet36 编码整数 ID，生成紧凑的字符串。同样会进行字符串反转。可指定前缀。
NewSecretID	生成一个长度为 36 的密码学安全的随机字符串（Secret ID）。	string	从 Alphabet62 (a-z, A-Z, 0-9) 中随机选择字符。适用于 API 密钥、访问密钥 ID 等。
NewSecretKey	生成一个长度为 32 的密码学安全的随机字符串（Secret Key 或密码）。	string	从 Alphabet62 (a-z, A-Z, 0-9) 中随机选择字符。适用于更敏感的密钥或密码。
常量:

Alphabet62: 包含 62 个字符（大小写字母和数字）的字符串。

Alphabet36: 包含 36 个字符（小写字母和数字）的字符串。

函数使用方式及示例
1. 获取唯一整数 ID (GetIntID)
用于需要高性能、全局唯一整数 ID 的场景，如作为数据库主键。

go
package main

import (
    "fmt"
    "github.com/maxiaolu1981/cretem/nexuscore/component-base/util/idutil"
)

func main() {
    uniqueIntID := idutil.GetIntID()
    fmt.Printf("Unique Integer ID: %d\n", uniqueIntID)
    // 输出: Unique Integer ID: 1234567890123456789 (一个很大的数字)
}
2. 获取实例 ID (GetInstanceID)
用于生成对人类友好且带类型的资源标识符，如 user-abc123, secret-def456。

go
func main() {
    // 假设我们有一个从数据库或 GetIntID() 获取的整数 ID
    uid := uint64(1234567890)

    instanceID := idutil.GetInstanceID(uid, "user-")
    fmt.Printf("Instance ID: %s\n", instanceID)
    // 输出可能类似于: Instance ID: user-5xw4v3
}
3. 获取短 UUID (GetUUID36)
用于生成紧凑的、类似 UUID 的字符串标识符，比传统的 UUID 更短。

go
func main() {
    shortUUID := idutil.GetUUID36("")
    fmt.Printf("Short UUID: %s\n", shortUUID)
    // 输出可能类似于: Short UUID: 2zkx5m39n1y0
    // 也可以加前缀
    prefixedUUID := idutil.GetUUID36("key_")
    fmt.Printf("Prefixed Short UUID: %s\n", prefixedUUID)
    // 输出可能类似于: Prefixed Short UUID: key_2zkx5m39n1y0
}
4. 生成随机密钥 (NewSecretID, NewSecretKey)
用于生成密码学安全的随机令牌、密钥、密码等。

go
func main() {
    // 生成一个 Secret ID (较长，36字符)，适合做访问密钥 ID
    secretID := idutil.NewSecretID()
    fmt.Printf("Secret ID: %s\n", secretID)
    // 输出可能类似于: Secret ID: A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8S

    // 生成一个 Secret Key (稍短，32字符)，适合做更敏感的访问密钥或密码
    secretKey := idutil.NewSecretKey()
    fmt.Printf("Secret Key: %s\n", secretKey)
    // 输出可能类似于: Secret Key: ZaX7cV8kLp0oM3nJhB2vGfRdQtYw1sE
    // 注意: 此函数生成的密钥不应原样存储，应进行加盐哈希处理。
}
关键注意事项
机器标识 (MachineID): GetIntID 使用 IP 地址的最后两段来生成机器 ID。在容器化环境（如 Docker, Kubernetes）中，这可能导致不同机器的机器 ID 冲突，因为 IP 可能动态分配或落在同一子网。在生产环境中，可能需要重写 st.MachineID 函数以使用更稳定的标识（如从环境变量读取或使用云提供商元数据）。

Hashids 配置: GetInstanceID 和 GetUUID36 使用了固定的盐（Salt: "x20k5x"）和字母表。如果希望生成的 ID 不可猜测或与其它系统兼容，应考虑将其改为自定义的值。

随机性来源: NewSecretID 和 NewSecretKey 使用 crypto/rand，这是密码学安全的，适合生成密钥。

反转操作: GetInstanceID 和 GetUUID36 中对 Hashids 结果进行了反转 (stringutil.Reverse)，这主要是为了打乱顺序，让生成的字符串看起来更随机。

错误处理: 当前实现中，大部分函数在出错时会直接 panic。在实际应用中，你可能希望根据具体情况调整错误处理策略（例如返回错误而不是 panic）。

package main

import (
	"fmt"
	"log"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/idutil"
)

func main() {
	// 示例1: 生成密钥并使用 bcrypt 哈希
	secretKey := idutil.NewSecretKey()
	fmt.Printf("原始密钥: %s\n", secretKey)

	hashedBcrypt, err := idutil.HashWithBcrypt(secretKey)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Bcrypt 哈希: %s\n", hashedBcrypt)

	// 验证
	isValid := idutil.VerifyWithBcrypt(secretKey, hashedBcrypt)
	fmt.Printf("Bcrypt 验证结果: %v\n", isValid)

	// 示例2: 使用 Argon2 哈希
	hashedArgon2, err := idutil.HashWithArgon2(secretKey, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Argon2 哈希: %s\n", hashedArgon2)

	// 验证
	isValidArgon2, err := idutil.VerifyWithArgon2(secretKey, hashedArgon2)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Argon2 验证结果: %v\n", isValidArgon2)

	// 示例3: 一次性生成并哈希
	original, hashed, err := idutil.NewHashedSecretKey("bcrypt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("新生成的密钥: %s\n", original)
	fmt.Printf("直接哈希后的密钥: %s\n", hashed)
}
*/

package idutil

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/iputil"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/stringutil"
	"github.com/sony/sonyflake"
	hashids "github.com/speps/go-hashids"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
)

// Defiens alphabet.
const (
	Alphabet62 = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	Alphabet36 = "abcdefghijklmnopqrstuvwxyz1234567890"
)

// Argon2Params 包含 Argon2 哈希算法的参数
type Argon2Params struct {
	Memory      uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

// DefaultArgon2Params 提供默认的 Argon2 参数
var DefaultArgon2Params = &Argon2Params{
	Memory:      64 * 1024, // 64MB
	Iterations:  3,
	Parallelism: 2,
	SaltLength:  16,
	KeyLength:   32,
}

var sf *sonyflake.Sonyflake

func init() {
	var st sonyflake.Settings
	st.MachineID = func() (uint16, error) {
		ip := iputil.GetLocalIP()

		return uint16([]byte(ip)[2])<<8 + uint16([]byte(ip)[3]), nil
	}

	sf = sonyflake.NewSonyflake(st)
}

// GetIntID returns uint64 uniq id.
func GetIntID() uint64 {
	id, err := sf.NextID()
	if err != nil {
		panic(err)
	}

	return id
}

// GetInstanceID returns id format like: secret-2v69o5
func GetInstanceID(uid uint64, prefix string) string {
	hd := hashids.NewData()
	hd.Alphabet = Alphabet36
	hd.MinLength = 6
	hd.Salt = "x20k5x"

	h, err := hashids.NewWithData(hd)
	if err != nil {
		panic(err)
	}

	i, err := h.Encode([]int{int(uid)})
	if err != nil {
		panic(err)
	}

	return prefix + stringutil.Reverse(i)
}

// GetUUID36 returns id format like: 300m50zn91nwz5.
func GetUUID36(prefix string) string {
	id := GetIntID()
	hd := hashids.NewData()
	hd.Alphabet = Alphabet36

	h, err := hashids.NewWithData(hd)
	if err != nil {
		panic(err)
	}

	i, err := h.Encode([]int{int(id)})
	if err != nil {
		panic(err)
	}

	return prefix + stringutil.Reverse(i)
}

func randString(letters string, n int) string {
	output := make([]byte, n)

	// We will take n bytes, one byte for each character of output.
	randomness := make([]byte, n)

	// read all random
	_, err := rand.Read(randomness)
	if err != nil {
		panic(err)
	}

	l := len(letters)
	// fill output
	for pos := range output {
		// get random item
		random := randomness[pos]

		// random % 64
		randomPos := random % uint8(l)

		// put into output
		output[pos] = letters[randomPos]
	}

	return string(output)
}

// NewSecretID returns a secretID.
func NewSecretID() string {
	return randString(Alphabet62, 36)
}

// NewSecretKey returns a secretKey or password.
func NewSecretKey() string {
	return randString(Alphabet62, 32)
}

// HashWithBcrypt 使用 bcrypt 对密钥进行加盐哈希处理
func HashWithBcrypt(secretKey string) (string, error) {
	hashedBytes, err := bcrypt.GenerateFromPassword([]byte(secretKey), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashedBytes), nil
}

// VerifyWithBcrypt 验证密钥是否与 bcrypt 哈希匹配
func VerifyWithBcrypt(secretKey, hashedKey string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hashedKey), []byte(secretKey))
	return err == nil
}

// HashWithArgon2 使用 Argon2 对密钥进行加盐哈希处理
func HashWithArgon2(secretKey string, params *Argon2Params) (string, error) {
	if params == nil {
		params = DefaultArgon2Params
	}

	// 生成随机盐
	salt := make([]byte, params.SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}

	// 使用 Argon2id 生成哈希
	hash := argon2.IDKey(
		[]byte(secretKey),
		salt,
		params.Iterations,
		params.Memory,
		params.Parallelism,
		params.KeyLength,
	)

	// 将参数、盐和哈希编码为字符串
	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)

	// 格式: $argon2id$v=19$m=65536,t=3,p=2$salt$hash
	encoded := fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s",
		params.Memory, params.Iterations, params.Parallelism,
		b64Salt, b64Hash)

	return encoded, nil
}

// VerifyWithArgon2 验证密钥是否与 Argon2 哈希匹配
func VerifyWithArgon2(secretKey, encoded string) (bool, error) {
	// 解析编码的字符串
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 {
		return false, fmt.Errorf("invalid encoded hash format")
	}

	// 解析参数
	var params Argon2Params
	_, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d",
		&params.Memory, &params.Iterations, &params.Parallelism)
	if err != nil {
		return false, err
	}

	// 解码盐和哈希
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, err
	}

	expectedHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, err
	}

	// 使用相同的参数重新计算哈希
	actualHash := argon2.IDKey(
		[]byte(secretKey),
		salt,
		params.Iterations,
		params.Memory,
		params.Parallelism,
		uint32(len(expectedHash)),
	)

	// 使用恒定时间比较来防止时序攻击
	return subtle.ConstantTimeCompare(actualHash, expectedHash) == 1, nil
}

// NewHashedSecretKey 生成新的密钥并立即进行哈希处理
func NewHashedSecretKey(algorithm string) (originalKey, hashedKey string, err error) {
	originalKey = NewSecretKey()

	switch algorithm {
	case "bcrypt":
		hashedKey, err = HashWithBcrypt(originalKey)
	case "argon2":
		hashedKey, err = HashWithArgon2(originalKey, DefaultArgon2Params)
	default:
		hashedKey, err = HashWithBcrypt(originalKey) // 默认使用 bcrypt
	}

	return originalKey, hashedKey, err
}

// 使用你已有的Sonyflake实例
// GenerateSecureSessionID 使用密码学安全的UUID
func GenerateSecureSessionID(prefix string) string {
	if prefix == "" {
		prefix = "sess_"
	}

	// 生成16字节随机数
	bytes := make([]byte, 16)
	_, err := rand.Read(bytes)
	if err != nil {
		// 如果crypto/rand失败，使用fallback（但添加时间戳）
		nanos := time.Now().UnixNano()
		bytes = []byte{
			byte(nanos), byte(nanos >> 8), byte(nanos >> 16), byte(nanos >> 24),
			byte(nanos >> 32), byte(nanos >> 40), byte(nanos >> 48), byte(nanos >> 56),
		}
	}

	// 转换为hex字符串
	return prefix + hex.EncodeToString(bytes)[:16]
}
