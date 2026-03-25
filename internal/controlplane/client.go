package controlplane

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/synadia-io/control-plane-sdk-go/syncp"
)

var ErrAccountNotFound = errors.New("account not found in control plane")

// AccountSelectors groups the account resolution fields shared across resource inputs.
type AccountSelectors struct {
	AccountID         string
	AccountPublicNKey string
	SystemID          string
}

// Client abstracts the Control Plane backend from reconcilers.
type Client interface {
	EnsureStream(ctx context.Context, in StreamInput) (StreamResult, error)
	DeleteStream(ctx context.Context, in StreamInput) error
	ReadStreamState(ctx context.Context, in StreamInput) ([]byte, bool, error)

	EnsureAccount(ctx context.Context, in AccountInput) (AccountResult, error)
	DeleteAccount(ctx context.Context, in AccountInput) error
	ReadAccountState(ctx context.Context, in AccountInput) ([]byte, bool, error)

	EnsureKeyValue(ctx context.Context, in KeyValueInput) (KeyValueResult, error)
	DeleteKeyValue(ctx context.Context, in KeyValueInput) error
	ReadKeyValueState(ctx context.Context, in KeyValueInput) ([]byte, bool, error)

	EnsureObjectStore(ctx context.Context, in ObjectStoreInput) (ObjectStoreResult, error)
	DeleteObjectStore(ctx context.Context, in ObjectStoreInput) error
	ReadObjectStoreState(ctx context.Context, in ObjectStoreInput) ([]byte, bool, error)

	EnsureConsumer(ctx context.Context, in ConsumerInput) (ConsumerResult, error)
	DeleteConsumer(ctx context.Context, in ConsumerInput) error
	ReadConsumerState(ctx context.Context, in ConsumerInput) ([]byte, bool, error)

	EnsureNatsUser(ctx context.Context, in NatsUserInput) (NatsUserResult, error)
	DeleteNatsUser(ctx context.Context, in NatsUserInput) error
	ReadNatsUserState(ctx context.Context, in NatsUserInput) ([]byte, bool, error)
}

type Options struct {
	BaseURL  string
	TokenEnv string
	Timeout  time.Duration
}

type client struct {
	api      *syncp.APIClient
	tokenEnv string
}

func NewClient(opts Options) (Client, error) {
	cfg := syncp.NewConfiguration()
	cfg.UserAgent = "synack/0.1.0"
	cfg.HTTPClient = &http.Client{Timeout: opts.Timeout}

	if opts.BaseURL != "" {
		u, err := url.Parse(opts.BaseURL)
		if err != nil {
			return nil, err
		}
		cfg.Scheme = u.Scheme
		cfg.Host = u.Host
	}

	return &client{
		api:      syncp.NewAPIClient(cfg),
		tokenEnv: opts.TokenEnv,
	}, nil
}

func (c *client) resolveAccountID(ctx context.Context, sel AccountSelectors) (string, error) {
	if sel.AccountID != "" {
		return sel.AccountID, nil
	}

	if sel.AccountPublicNKey == "" {
		return "", errors.New("must set accountId, account, or accountPublicNKey")
	}

	if sel.SystemID == "" {
		return "", errors.New("systemId is required when accountPublicNKey is set")
	}

	list, _, err := c.api.SystemAPI.ListAccounts(ctx, sel.SystemID).Execute()
	if err != nil {
		return "", fmt.Errorf("list accounts for nkey resolution: %w", err)
	}

	for _, a := range list.Items {
		if a.AccountPublicKey == sel.AccountPublicNKey {
			return a.Id, nil
		}
	}

	return "", fmt.Errorf("%w: no account with public nkey %q in system %q", ErrAccountNotFound, sel.AccountPublicNKey, sel.SystemID)
}

func (c *client) authContext(ctx context.Context) (context.Context, error) {
	token, err := tokenFromEnv(c.tokenEnv)
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, syncp.ContextAccessToken, token), nil
}

func tokenFromEnv(name string) (string, error) {
	v := os.Getenv(name)
	if v == "" {
		return "", errors.New("control plane token not set")
	}
	return v, nil
}

func isStatusCode(err error, code int) bool {
	if err == nil {
		return false
	}
	var withCode interface {
		Code() int
	}
	if errors.As(err, &withCode) {
		return withCode.Code() == code
	}
	return false
}
