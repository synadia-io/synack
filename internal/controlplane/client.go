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

	Description       string
	Retention         string
	MaxConsumers      int
	MaxMsgsPerSubject int
	MaxMsgs           int
	MaxBytes          int
	MaxAge            string
	MaxMsgSize        int
	Storage           string
	Discard           string
	Replicas          int
	NoAck             bool
	DuplicateWindow   string
	Placement         *syncp.Placement
	Sources           []syncp.StreamSource
	Compression       string
	SubjectTransform  *syncp.SubjectTransformConfig
	RePublish         *syncp.RePublish
	Sealed            bool
	DenyDelete        bool
	DenyPurge         bool
	AllowDirect       bool
	AllowRollup       bool
	DiscardPerSubject bool
	FirstSequence     uint64
	Metadata          map[string]string
}

// StreamResult captures the stable identifiers we surface back to status.
type StreamResult struct {
	AccountID string
	StreamID  string
}

// AccountInput captures what the reconciler needs to apply an Account.
type AccountInput struct {
	AccountID string
	SystemID  string
	Name      string
}

// AccountResult captures the stable identifiers we surface back to status.
type AccountResult struct {
	AccountID string
}

// Client abstracts the Control Plane backend from reconcilers.
type Client interface {
	EnsureStream(ctx context.Context, in StreamInput) (StreamResult, error)
	EnsureAccount(ctx context.Context, in AccountInput) (AccountResult, error)
	DeleteStream(ctx context.Context, in StreamInput) error
	DeleteAccount(ctx context.Context, in AccountInput) error
}

// Options configures the Control Plane client.
type Options struct {
	BaseURL  string
	TokenEnv string
}

type client struct {
	api      *syncp.APIClient
	tokenEnv string
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
		api:      syncp.NewAPIClient(cfg),
		tokenEnv: opts.TokenEnv,
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

func (c *client) DeleteStream(ctx context.Context, in StreamInput) error {
	if in.Name == "" {
		return errors.New("stream name is required")
	}

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	if in.StreamID != "" {
		_, err := c.api.StreamAPI.DeleteStream(authCtx, in.StreamID).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			return nil
		}
		return fmt.Errorf("delete stream by stream id %q: %w", in.StreamID, err)
	}

	accountID, err := c.resolveAccountID(authCtx, in)
	if err != nil {
		if errors.Is(err, ErrAccountNotFound) {
			return nil
		}
		return err
	}

	list, _, err := c.api.AccountAPI.ListStreams(authCtx, accountID).Execute()
	if err != nil {
		return fmt.Errorf("list streams for delete: %w", err)
	}

	for _, s := range list.Items {
		if s.Config.Name != in.Name {
			continue
		}
		_, err := c.api.StreamAPI.DeleteStream(authCtx, s.Id).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			return nil
		}
		return fmt.Errorf("delete stream %q: %w", in.Name, err)
	}

	return nil
}

func (c *client) DeleteAccount(ctx context.Context, in AccountInput) error {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	accountID := in.AccountID
	if accountID == "" {
		if in.SystemID == "" {
			return errors.New("account systemId is required when accountId is not set")
		}
		if in.Name == "" {
			return errors.New("account name is required when accountId is not set")
		}

		list, _, err := c.api.SystemAPI.ListAccounts(authCtx, in.SystemID).Execute()
		if err != nil {
			return fmt.Errorf("list accounts for delete: %w", err)
		}

		for _, a := range list.Items {
			if a.Name == in.Name {
				accountID = a.Id
				break
			}
		}

		if accountID == "" {
			return nil
		}
	}

	_, err = c.api.AccountAPI.DeleteAccount(authCtx, accountID).Execute()
	if err == nil || isStatusCode(err, http.StatusNotFound) {
		return nil
	}
	return fmt.Errorf("delete account %q: %w", accountID, err)
}

func desiredStreamConfig(in StreamInput) syncp.JSStreamConfigRequest {
	maxAge := int64(0)
	if in.MaxAge != "" {
		if d, err := time.ParseDuration(in.MaxAge); err == nil {
			maxAge = int64(d)
		}
	}

	duplicateWindow := int64(0)
	if in.DuplicateWindow != "" {
		if d, err := time.ParseDuration(in.DuplicateWindow); err == nil {
			duplicateWindow = int64(d)
		}
	}

	retention := syncp.RETENTIONPOLICY_LIMITS
	switch in.Retention {
	case string(syncp.RETENTIONPOLICY_INTEREST):
		retention = syncp.RETENTIONPOLICY_INTEREST
	case string(syncp.RETENTIONPOLICY_WORKQUEUE):
		retention = syncp.RETENTIONPOLICY_WORKQUEUE
	}

	storage := syncp.STORAGETYPE_MEMORY
	if in.Storage == string(syncp.STORAGETYPE_FILE) {
		storage = syncp.STORAGETYPE_FILE
	}

	discard := syncp.DISCARDPOLICY_OLD
	if in.Discard == string(syncp.DISCARDPOLICY_NEW) {
		discard = syncp.DISCARDPOLICY_NEW
	}

	replicas := int64(in.Replicas)
	if replicas < 1 {
		replicas = 1
	}

	description := in.Description
	noAck := in.NoAck
	discardPerSubject := in.DiscardPerSubject
	maxMsgSize := int64(in.MaxMsgSize)
	firstSeq := in.FirstSequence
	compression := mapCompression(in.Compression)

	return syncp.JSStreamConfigRequest{
		Subjects: in.Subjects,
		JSCommonStreamConfig: syncp.JSCommonStreamConfig{
			AllowDirect:          in.AllowDirect,
			AllowRollupHdrs:      in.AllowRollup,
			Compression:          compression,
			DenyDelete:           in.DenyDelete,
			DenyPurge:            in.DenyPurge,
			Description:          &description,
			Discard:              discard,
			DiscardNewPerSubject: &discardPerSubject,
			DuplicateWindow:      &duplicateWindow,
			FirstSeq:             streamFirstSeqPtr(firstSeq),
			MaxAge:               maxAge,
			MaxBytes:             int64(in.MaxBytes),
			MaxConsumers:         int64(in.MaxConsumers),
			MaxMsgSize:           &maxMsgSize,
			MaxMsgs:              int64(in.MaxMsgs),
			MaxMsgsPerSubject:    int64(in.MaxMsgsPerSubject),
			Metadata:             in.Metadata,
			Name:                 in.Name,
			NoAck:                &noAck,
			NumReplicas:          replicas,
			Placement:            in.Placement,
			Republish:            in.RePublish,
			Retention:            retention,
			Sealed:               in.Sealed,
			Sources:              in.Sources,
			Storage:              storage,
			SubjectTransform:     in.SubjectTransform,
		},
	}
}

func streamFirstSeqPtr(v uint64) *uint64 {
	if v == 0 {
		return nil
	}
	return &v
}

func mapCompression(v string) *syncp.S2Compression {
	if v != string(syncp.S2COMPRESSION_S2) {
		return nil
	}
	c := syncp.S2COMPRESSION_S2
	return &c
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
