package interfaces

var client Factory

type Factory interface {
	Users() UserStore
	Secrets() SecretStore
	Polices() PolicyStore
	PolicyAudits() PolicyAuditStore
	Close() error
}

func Client() Factory {
	return client
}
func SetClient(factory Factory) {
	client = factory
}

// UserStore defines the user storage interface.
