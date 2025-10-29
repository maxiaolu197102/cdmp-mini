package dbscan

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

type RowScanner interface {
	Scan(dest ...any) error
}

type userScanMode int

const (
	scanModeFull userScanMode = iota
	scanModeAuth
	scanModeLite
)

type userRowBuffer struct {
	id         uint64
	instanceID sql.RawBytes
	name       sql.RawBytes
	nickname   sql.RawBytes
	password   sql.RawBytes
	email      sql.RawBytes
	phone      sql.RawBytes
	status     int
	isAdmin    int
	extend     sql.RawBytes
	createdAt  time.Time
	updatedAt  time.Time
	loginedAt  sql.NullTime
	version    uint64

	fullDest [14]any
	authDest [13]any
	liteDest [12]any
}

func newUserRowBuffer() *userRowBuffer {
	buf := &userRowBuffer{}
	buf.fullDest = [14]any{
		&buf.id,
		&buf.instanceID,
		&buf.name,
		&buf.nickname,
		&buf.password,
		&buf.email,
		&buf.phone,
		&buf.status,
		&buf.isAdmin,
		&buf.extend,
		&buf.createdAt,
		&buf.updatedAt,
		&buf.loginedAt,
		&buf.version,
	}
	buf.authDest = [13]any{
		&buf.id,
		&buf.instanceID,
		&buf.name,
		&buf.nickname,
		&buf.password,
		&buf.email,
		&buf.phone,
		&buf.status,
		&buf.isAdmin,
		&buf.createdAt,
		&buf.updatedAt,
		&buf.loginedAt,
		&buf.version,
	}
	buf.liteDest = [12]any{
		&buf.id,
		&buf.instanceID,
		&buf.name,
		&buf.nickname,
		&buf.email,
		&buf.phone,
		&buf.status,
		&buf.isAdmin,
		&buf.createdAt,
		&buf.updatedAt,
		&buf.loginedAt,
		&buf.version,
	}
	return buf
}

func (b *userRowBuffer) reset() {
	b.id = 0
	b.instanceID = nil
	b.name = nil
	b.nickname = nil
	b.password = nil
	b.email = nil
	b.phone = nil
	b.status = 0
	b.isAdmin = 0
	b.extend = nil
	b.createdAt = time.Time{}
	b.updatedAt = time.Time{}
	b.loginedAt = sql.NullTime{}
	b.version = 0
}

func (b *userRowBuffer) dest(mode userScanMode) []any {
	switch mode {
	case scanModeAuth:
		return b.authDest[:]
	case scanModeLite:
		return b.liteDest[:]
	default:
		return b.fullDest[:]
	}
}

var userRowBufferPool = sync.Pool{
	New: func() any {
		return newUserRowBuffer()
	},
}

func rawToString(v []byte) string {
	if len(v) == 0 {
		return ""
	}
	return string(v)
}

func materializeUser(dst *v1.User, buf *userRowBuffer, mode userScanMode) {
	dst.ObjectMeta = metav1.ObjectMeta{
		ID:         buf.id,
		InstanceID: rawToString(buf.instanceID),
		Name:       rawToString(buf.name),
		CreatedAt:  buf.createdAt,
		UpdatedAt:  buf.updatedAt,
		Version:    buf.version,
	}
	dst.Status = buf.status
	dst.IsAdmin = buf.isAdmin
	dst.Nickname = rawToString(buf.nickname)
	dst.Email = rawToString(buf.email)
	dst.Phone = rawToString(buf.phone)
	if buf.loginedAt.Valid {
		dst.LoginedAt = buf.loginedAt.Time
	} else {
		dst.LoginedAt = time.Time{}
	}

	if mode != scanModeLite {
		dst.Password = rawToString(buf.password)
	}
	if mode == scanModeFull {
		shadow := rawToString(buf.extend)
		dst.ExtendShadow = shadow
		shadow = strings.TrimSpace(shadow)
		if shadow != "" {
			var ext metav1.Extend
			if err := json.Unmarshal([]byte(shadow), &ext); err == nil {
				dst.Extend = ext
			}
		}
	}
}

func scanUser(scanner RowScanner, dst *v1.User, mode userScanMode) (*v1.User, error) {
	buf := userRowBufferPool.Get().(*userRowBuffer)
	buf.reset()

	if err := scanner.Scan(buf.dest(mode)...); err != nil {
		userRowBufferPool.Put(buf)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, err
	}

	if dst == nil {
		dst = &v1.User{}
	} else {
		*dst = v1.User{}
	}

	materializeUser(dst, buf, mode)

	userRowBufferPool.Put(buf)
	return dst, nil
}

// ScanUser builds a v1.User from the provided row scanner using all columns (including extendShadow).
func ScanUser(scanner RowScanner) (*v1.User, error) {
	return scanUser(scanner, nil, scanModeFull)
}

// ScanUserFullInto scans a row into the provided destination using the full column set.
func ScanUserFullInto(scanner RowScanner, dst *v1.User) (*v1.User, error) {
	return scanUser(scanner, dst, scanModeFull)
}

// ScanUserAuth scans the reduced column set used for authentication flows.
func ScanUserAuth(scanner RowScanner) (*v1.User, error) {
	return scanUser(scanner, nil, scanModeAuth)
}

// ScanUserAuthInto scans the reduced column set into the provided destination.
func ScanUserAuthInto(scanner RowScanner, dst *v1.User) (*v1.User, error) {
	return scanUser(scanner, dst, scanModeAuth)
}

// ScanUserLite scans the minimal column set needed for list-style read paths.
func ScanUserLite(scanner RowScanner) (*v1.User, error) {
	return scanUser(scanner, nil, scanModeLite)
}

// ScanUserLiteInto scans the minimal column set into the provided destination.
func ScanUserLiteInto(scanner RowScanner, dst *v1.User) (*v1.User, error) {
	return scanUser(scanner, dst, scanModeLite)
}

type userContactBuffer struct {
	id    uint64
	name  sql.RawBytes
	email sql.RawBytes
	phone sql.RawBytes
	dest  [4]any
}

func newUserContactBuffer() *userContactBuffer {
	buf := &userContactBuffer{}
	buf.dest = [4]any{
		&buf.id,
		&buf.name,
		&buf.email,
		&buf.phone,
	}
	return buf
}

func (b *userContactBuffer) reset() {
	b.id = 0
	b.name = nil
	b.email = nil
	b.phone = nil
}

var userContactBufferPool = sync.Pool{
	New: func() any {
		return newUserContactBuffer()
	},
}

// ScanUserContact extracts minimal user contact info.
func ScanUserContact(scanner RowScanner) (uint64, string, string, string, error) {
	buf := userContactBufferPool.Get().(*userContactBuffer)
	buf.reset()
	if err := scanner.Scan(buf.dest[:]...); err != nil {
		userContactBufferPool.Put(buf)
		return 0, "", "", "", err
	}
	id := buf.id
	name := string(buf.name)
	email := ""
	phone := ""
	if len(buf.email) > 0 {
		email = usercache.NormalizeEmail(string(buf.email))
	}
	if len(buf.phone) > 0 {
		phone = usercache.NormalizePhone(string(buf.phone))
	}
	userContactBufferPool.Put(buf)
	return id, name, email, phone, nil
}
