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

// StreamInput captures what the reconciler needs to apply a Stream.
type StreamInput struct {
	// AccountID is the canonical account identifier for stream APIs.
	AccountID string

	// AccountPublicNKey may be used instead of AccountID when SystemID is provided.
	AccountPublicNKey string

	// SystemID scopes AccountPublicNKey resolution.
	SystemID string

	// Account is a legacy alias for AccountID.
	Account string

	// StreamID optionally targets a known stream ID directly.
	StreamID string

	Name     string
	Subjects []string
}

// StreamResult captures the stable identifiers we surface back to status.
type StreamResult struct {
	AccountID string
	StreamID  string
}

// AccountInput captures what the reconciler needs to apply an Account.
type AccountInput struct {
	SystemID string
	Name     string
}

// AccountResult captures the stable identifiers we surface back to status.
type AccountResult struct {
	AccountID string
}

// Client abstracts the Control Plane backend from reconcilers.
type Client interface {
	EnsureStream(ctx context.Context, in StreamInput) (StreamResult, error)
	EnsureAccount(ctx context.Context, in AccountInput) (AccountResult, error)
}

// Options configures the Control Plane client.
type Options struct {
	BaseURL             string
	TokenEnv            string
	EnableAccountImport bool
}

type client struct {
	api                 *syncp.APIClient
	tokenEnv            string
	enableAccountImport bool
}

// NewClient creates a Control Plane client wrapper backed by control-plane-sdk-go.
func NewClient(opts Options) (Client, error) {
	if opts.TokenEnv == "" {
		opts.TokenEnv = "SYNACK_CONTROL_PLANE_TOKEN"
	}

	cfg := syncp.NewConfiguration()
	cfg.UserAgent = "synack/0.1.0"
	cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}

	if opts.BaseURL != "" {
		u, err := url.Parse(opts.BaseURL)
		if err != nil {
			return nil, err
		}
		cfg.Scheme = u.Scheme
		cfg.Host = u.Host
	}

	return &client{
		api:                 syncp.NewAPIClient(cfg),
		tokenEnv:            opts.TokenEnv,
		enableAccountImport: opts.EnableAccountImport,
	}, nil
}

// EnsureStream reconciles a stream by name under an account.
func (c *client) EnsureStream(ctx context.Context, in StreamInput) (StreamResult, error) {
	if in.Name == "" {
		return StreamResult{}, errors.New("stream name is required")
	}

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return StreamResult{}, err
	}

	desired := desiredStreamConfig(in)

	if in.StreamID != "" {
		updated, _, err := c.api.StreamAPI.UpdateStream(authCtx, in.StreamID).JSStreamConfigRequest(desired).Execute()
		if err == nil {
			return StreamResult{AccountID: in.AccountID, StreamID: updated.Id}, nil
		}
		if !isStatusCode(err, http.StatusNotFound) {
			return StreamResult{}, fmt.Errorf("update stream by stream id %q: %w", in.StreamID, err)
		}
	}

	accountID, err := c.resolveAccountID(authCtx, in)
	if err != nil {
		return StreamResult{}, err
	}

	list, _, err := c.api.AccountAPI.ListStreams(authCtx, accountID).Execute()
	if err != nil {
		return StreamResult{}, fmt.Errorf("list streams: %w", err)
	}

	for _, s := range list.Items {
		if s.Config.Name != in.Name {
			continue
		}
		updated, _, err := c.api.StreamAPI.UpdateStream(authCtx, s.Id).JSStreamConfigRequest(desired).Execute()
		if err != nil {
			return StreamResult{}, fmt.Errorf("update stream %q: %w", in.Name, err)
		}
		return StreamResult{AccountID: accountID, StreamID: updated.Id}, nil
	}

	created, _, err := c.api.AccountAPI.CreateStream(authCtx, accountID).JSStreamConfigRequest(desired).Execute()
	if err != nil {
		return StreamResult{}, fmt.Errorf("create stream %q: %w", in.Name, err)
	}

	return StreamResult{AccountID: accountID, StreamID: created.Id}, nil
}

// EnsureAccount reconciles an account by name under a system.
func (c *client) EnsureAccount(ctx context.Context, in AccountInput) (AccountResult, error) {
	if in.SystemID == "" {
		return AccountResult{}, errors.New("account systemId is required")
	}
	if in.Name == "" {
		return AccountResult{}, errors.New("account name is required")
	}

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return AccountResult{}, err
	}

	list, _, err := c.api.SystemAPI.ListAccounts(authCtx, in.SystemID).Execute()
	if err != nil {
		return AccountResult{}, fmt.Errorf("list accounts: %w", err)
	}

	for _, a := range list.Items {
		if a.Name == in.Name {
			return AccountResult{AccountID: a.Id}, nil
		}
	}

	createReq := syncp.AccountCreateRequest{Name: in.Name}
	created, _, err := c.api.SystemAPI.CreateAccount(authCtx, in.SystemID).AccountCreateRequest(createReq).Execute()
	if err != nil {
		return AccountResult{}, fmt.Errorf("create account %q: %w", in.Name, err)
	}

	return AccountResult{AccountID: created.Id}, nil
}

func desiredStreamConfig(in StreamInput) syncp.JSStreamConfigRequest {
	return syncp.JSStreamConfigRequest{
		Subjects: in.Subjects,
		JSCommonStreamConfig: syncp.JSCommonStreamConfig{
			AllowDirect:       false,
			AllowRollupHdrs:   false,
			DenyDelete:        false,
			DenyPurge:         false,
			Discard:           syncp.DISCARDPOLICY_OLD,
			MaxAge:            0,
			MaxBytes:          -1,
			MaxConsumers:      -1,
			MaxMsgs:           -1,
			MaxMsgsPerSubject: -1,
			Name:              in.Name,
			NumReplicas:       1,
			Retention:         syncp.RETENTIONPOLICY_LIMITS,
			Sealed:            false,
			Storage:           syncp.STORAGETYPE_FILE,
		},
	}
}

func (c *client) resolveAccountID(ctx context.Context, in StreamInput) (string, error) {
	if in.AccountID != "" {
		return in.AccountID, nil
	}

	if in.Account != "" {
		return in.Account, nil
	}

	if in.AccountPublicNKey == "" {
		return "", errors.New("stream must set accountId, account (legacy), or accountPublicNKey")
	}

	if in.SystemID == "" {
		return "", errors.New("systemId is required when accountPublicNKey is set") // Control Plane SDK Limitation
	}

	list, _, err := c.api.SystemAPI.ListAccounts(ctx, in.SystemID).Execute()
	if err != nil {
		return "", fmt.Errorf("list accounts for nkey resolution: %w", err)
	}

	for _, a := range list.Items {
		if a.AccountPublicKey == in.AccountPublicNKey {
			return a.Id, nil
		}
	}

	if c.enableAccountImport {
		// Import requires credentials/JWT material not modeled in StreamSpec yet.
		return "", fmt.Errorf("%w: auto-import hook enabled but no import material is configured", ErrAccountNotFound)
	}

	return "", fmt.Errorf("%w: no account with public nkey %q in system %q", ErrAccountNotFound, in.AccountPublicNKey, in.SystemID)
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
