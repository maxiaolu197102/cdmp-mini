package user

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/store/interfaces"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

type fakeFactory struct {
	userStore interfaces.UserStore
}

func (f *fakeFactory) Users() interfaces.UserStore               { return f.userStore }
func (f *fakeFactory) Secrets() interfaces.SecretStore           { return nil }
func (f *fakeFactory) Polices() interfaces.PolicyStore           { return nil }
func (f *fakeFactory) PolicyAudits() interfaces.PolicyAuditStore { return nil }
func (f *fakeFactory) Close() error                              { return nil }

type fakeUserStore struct {
	preflight map[string]*v1.User
	calls     int
	lastArgs  []string
}

func (f *fakeUserStore) Create(context.Context, *v1.User, metav1.CreateOptions, *options.Options) error {
	panic("unexpected call to Create")
}
func (f *fakeUserStore) Update(context.Context, *v1.User, metav1.UpdateOptions, *options.Options) error {
	panic("unexpected call to Update")
}
func (f *fakeUserStore) UpdatePasswordWithVersion(context.Context, uint64, string, string, time.Time, time.Time) error {
	panic("unexpected call to UpdatePasswordWithVersion")
}
func (f *fakeUserStore) Delete(context.Context, string, metav1.DeleteOptions, *options.Options) error {
	panic("unexpected call to Delete")
}
func (f *fakeUserStore) DeleteForce(context.Context, string, metav1.DeleteOptions, *options.Options) error {
	panic("unexpected call to DeleteForce")
}
func (f *fakeUserStore) DeleteCollection(context.Context, []string, metav1.DeleteOptions, *options.Options) error {
	panic("unexpected call to DeleteCollection")
}
func (f *fakeUserStore) Get(context.Context, string, metav1.GetOptions, *options.Options) (*v1.User, error) {
	panic("unexpected call to Get")
}
func (f *fakeUserStore) GetByEmail(context.Context, string, *options.Options) (*v1.User, error) {
	panic("unexpected call to GetByEmail")
}
func (f *fakeUserStore) GetByPhone(context.Context, string, *options.Options) (*v1.User, error) {
	panic("unexpected call to GetByPhone")
}
func (f *fakeUserStore) List(context.Context, string, metav1.ListOptions, *options.Options) (*v1.UserList, error) {
	panic("unexpected call to List")
}
func (f *fakeUserStore) ListAllUsernames(context.Context) ([]string, error) {
	panic("unexpected call to ListAllUsernames")
}
func (f *fakeUserStore) ListAll(context.Context, string) (*v1.UserList, error) {
	panic("unexpected call to ListAll")
}
func (f *fakeUserStore) PreflightConflicts(ctx context.Context, username, email, phone string, _ *options.Options) (map[string]*v1.User, error) {
	f.calls++
	f.lastArgs = []string{username, email, phone}
	if f.preflight == nil {
		return map[string]*v1.User{}, nil
	}
	copyMap := make(map[string]*v1.User, len(f.preflight))
	for k, v := range f.preflight {
		copyMap[k] = v
	}
	return copyMap, nil
}

func TestEnsureContactUniquenessUsesPreflight(t *testing.T) {
	store := &fakeUserStore{}
	opts := options.NewOptions()
	svc := NewUserService(&fakeFactory{userStore: store}, nil, opts, nil, nil)
	svc.Options.RedisOptions.MaxRetries = 1

	user := &v1.User{ObjectMeta: metav1.ObjectMeta{Name: "Alice"}, Email: "Alice@Example.com", Phone: " 13900000000 "}
	if _, usernameChecked, err := svc.ensureContactUniqueness(context.Background(), user); err != nil {
		t.Fatalf("ensureContactUniqueness returned error: %v", err)
	} else if !usernameChecked {
		t.Fatalf("expected username to be preflighted")
	}
	if store.calls != 1 {
		t.Fatalf("expected PreflightConflicts to be called once, got %d", store.calls)
	}
	want := []string{"Alice", "alice@example.com", "13900000000"}
	if !reflect.DeepEqual(store.lastArgs, want) {
		t.Fatalf("expected normalized args %v, got %v", want, store.lastArgs)
	}
}

func TestEnsureContactUniquenessReturnsErrorOnEmailConflict(t *testing.T) {
	store := &fakeUserStore{preflight: map[string]*v1.User{
		"email": {ObjectMeta: metav1.ObjectMeta{Name: "bob"}},
	}}
	opts := options.NewOptions()
	svc := NewUserService(&fakeFactory{userStore: store}, nil, opts, nil, nil)
	svc.Options.RedisOptions.MaxRetries = 1

	user := &v1.User{ObjectMeta: metav1.ObjectMeta{Name: "Alice"}, Email: "alice@example.com"}
	_, _, err := svc.ensureContactUniqueness(context.Background(), user)
	if err == nil {
		t.Fatalf("expected validation error, got nil")
	}
	if errors.GetCode(err) != code.ErrValidation {
		t.Fatalf("expected validation code, got %d", errors.GetCode(err))
	}
}

func TestEnsureContactUniquenessAllowsSameOwner(t *testing.T) {
	store := &fakeUserStore{preflight: map[string]*v1.User{
		"email": {ObjectMeta: metav1.ObjectMeta{Name: "alice"}},
	}}
	opts := options.NewOptions()
	svc := NewUserService(&fakeFactory{userStore: store}, nil, opts, nil, nil)
	svc.Options.RedisOptions.MaxRetries = 1

	user := &v1.User{ObjectMeta: metav1.ObjectMeta{Name: "alice"}, Email: "alice@example.com"}
	if _, _, err := svc.ensureContactUniqueness(context.Background(), user); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
