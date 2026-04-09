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
	"github.com/tidwall/gjson"
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

	ResolveSigningKeyGroupID(ctx context.Context, accountID, skGroupID string) (string, error)
	EnsureNatsUser(ctx context.Context, in NatsUserInput) (NatsUserResult, error)
	DeleteNatsUser(ctx context.Context, in NatsUserInput) error
	ReadNatsUserState(ctx context.Context, in NatsUserInput) ([]byte, bool, error)
	DownloadNatsUserCreds(ctx context.Context, natsUserID string) (string, error)

	EnsureTeam(ctx context.Context, in TeamInput) (TeamResult, error)
	DeleteTeam(ctx context.Context, in TeamInput) error
	ReadTeamState(ctx context.Context, in TeamInput) ([]byte, bool, error)

	EnsureTeamServiceAccount(ctx context.Context, in TeamServiceAccountInput) (TeamServiceAccountResult, error)
	DeleteTeamServiceAccount(ctx context.Context, in TeamServiceAccountInput) error
	ReadTeamServiceAccountState(ctx context.Context, in TeamServiceAccountInput) ([]byte, bool, error)

	EnsureAppUserRoleBinding(ctx context.Context, in AppUserRoleBindingInput) (AppUserRoleBindingResult, error)
	DeleteAppUserRoleBinding(ctx context.Context, in AppUserRoleBindingInput) error
	ReadAppUserRoleBindingState(ctx context.Context, in AppUserRoleBindingInput) ([]byte, bool, error)
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

type apiError struct {
	err  error
	code int
	body string
}

func withAPIError(err error) error {
	if err == nil {
		return nil
	}

	if apiErr, ok := err.(*apiError); ok {
		return apiErr
	}

	openAPIErr, value := openAPIError(err)
	if openAPIErr == nil {
		return err
	}

	return &apiError{
		err:  err,
		code: openAPIErr.Code(),
		body: value,
	}
}

func (e *apiError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	if e.body == "" {
		return e.err.Error()
	}
	return fmt.Sprintf("%s (error: %s)", e.err.Error(), e.body)
}

func (e *apiError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *apiError) Code() int {
	if e == nil {
		return 0
	}
	return e.code
}

func openAPIError(err error) (*syncp.GenericOpenAPIError, string) {
	var openAPIError *syncp.GenericOpenAPIError
	if !errors.As(err, &openAPIError) || openAPIError == nil {
		return nil, ""
	}

	field := gjson.GetBytes(openAPIError.Body(), "error")
	if !field.Exists() {
		return openAPIError, ""
	}

	return openAPIError, field.String()
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
		err = withAPIError(err)
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
	if apiErr, ok := err.(*apiError); ok {
		return apiErr.Code() == code
	}

	return false
}
