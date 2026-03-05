package controlplane

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
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

type StreamInput struct {
	AccountSelectors

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

type StreamResult struct {
	AccountID string
	StreamID  string
}

type KeyValueInput struct {
	AccountSelectors

	KeyValueID string

	Bucket       string
	Description  string
	History      int
	TTL          string
	MaxBytes     int
	MaxValueSize int
	Storage      string
	Replicas     int
	Compression  bool
	Placement    *syncp.Placement
	RePublish    *syncp.RePublish
	Mirror       *syncp.StreamSource
	Sources      []syncp.StreamSource
}

type KeyValueResult struct {
	AccountID  string
	KeyValueID string
}

type ObjectStoreInput struct {
	AccountSelectors

	ObjectStoreID string

	Bucket      string
	Description string
	TTL         string
	MaxBytes    int
	Storage     string
	Replicas    int
	Compression bool
	Placement   *syncp.Placement
	Metadata    map[string]string
}

type ObjectStoreResult struct {
	AccountID     string
	ObjectStoreID string
}

type ConsumerInput struct {
	StreamID   string
	ConsumerID string
	IsPush     bool

	Name        string
	Description string

	AckPolicy         string
	AckWait           string
	DeliverPolicy     string
	DurableName       string
	FilterSubjects    []string
	InactiveThreshold string
	MaxAckPending     int
	MaxDeliver        int
	MemStorage        bool
	Replicas          int
	OptStartSeq       uint64
	OptStartTime      string
	ReplayPolicy      string
	SampleFreq        string
	Backoff           []string
	Direct            bool
	Metadata          map[string]string

	// Pull-only fields
	MaxRequestBatch    int
	MaxRequestMaxBytes int
	MaxRequestExpires  string
	MaxWaiting         int

	// Push-only fields
	DeliverSubject    string
	DeliverGroup      string
	FlowControl       bool
	HeadersOnly       bool
	HeartbeatInterval string
	RateLimitBps      uint64
}

type ConsumerResult struct {
	ConsumerID string
	StreamID   string
	IsPush     bool
}

type AccountInput struct {
	AccountID string
	SystemID  string
	Name      string
}

type AccountResult struct {
	AccountID string
}

// Client abstracts the Control Plane backend from reconcilers.
type Client interface {
	EnsureStream(ctx context.Context, in StreamInput) (StreamResult, error)
	DeleteStream(ctx context.Context, in StreamInput) error

	EnsureAccount(ctx context.Context, in AccountInput) (AccountResult, error)
	DeleteAccount(ctx context.Context, in AccountInput) error

	EnsureKeyValue(ctx context.Context, in KeyValueInput) (KeyValueResult, error)
	DeleteKeyValue(ctx context.Context, in KeyValueInput) error

	EnsureObjectStore(ctx context.Context, in ObjectStoreInput) (ObjectStoreResult, error)
	DeleteObjectStore(ctx context.Context, in ObjectStoreInput) error

	EnsureConsumer(ctx context.Context, in ConsumerInput) (ConsumerResult, error)
	DeleteConsumer(ctx context.Context, in ConsumerInput) error
}

type Options struct {
	BaseURL  string
	TokenEnv string
}

type client struct {
	api      *syncp.APIClient
	tokenEnv string
}

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

// --- Stream ---

func (c *client) EnsureStream(ctx context.Context, in StreamInput) (StreamResult, error) {
	if in.Name == "" {
		return StreamResult{}, errors.New("stream name is required")
	}

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return StreamResult{}, err
	}

	desired := inputToStreamConfig(in)

	if in.StreamID != "" {
		updated, _, err := c.api.StreamAPI.UpdateStream(authCtx, in.StreamID).JSStreamConfigRequest(desired).Execute()
		if err == nil {
			return StreamResult{AccountID: in.AccountID, StreamID: updated.Id}, nil
		}
		if !isStatusCode(err, http.StatusNotFound) {
			return StreamResult{}, fmt.Errorf("update stream by stream id %q: %w", in.StreamID, err)
		}
	}

	accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
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

	accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
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

// --- Account ---

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

		found := make([]string, 0)
		for _, a := range list.Items {
			if a.Name == in.Name {
				found = append(found, a.Id)
			}
		}

		if len(found) == 0 {
			return nil
		}

		if len(found) > 1 {
			return fmt.Errorf("multiple accounts found with name %q in system %q: %s", in.Name, in.SystemID, strings.Join(found, ", "))
		}

		accountID = found[0]
	}

	_, err = c.api.AccountAPI.DeleteAccount(authCtx, accountID).Execute()
	if err == nil || isStatusCode(err, http.StatusNotFound) {
		return nil
	}
	return fmt.Errorf("delete account %q: %w", accountID, err)
}

// --- KV Bucket ---

func (c *client) EnsureKeyValue(ctx context.Context, in KeyValueInput) (KeyValueResult, error) {
	if in.Bucket == "" {
		return KeyValueResult{}, errors.New("kv bucket name is required")
	}

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return KeyValueResult{}, err
	}

	if in.KeyValueID != "" {
		updateReq := inputToKVUpdateConfig(in)
		updated, _, err := c.api.KvBucketAPI.UpdateKvBucket(authCtx, in.KeyValueID).JSKVBucketUpdateRequest(updateReq).Execute()
		if err == nil {
			return KeyValueResult{AccountID: in.AccountID, KeyValueID: updated.Id}, nil
		}
		if !isStatusCode(err, http.StatusNotFound) {
			return KeyValueResult{}, fmt.Errorf("update kv bucket by id %q: %w", in.KeyValueID, err)
		}
	}

	accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
	if err != nil {
		return KeyValueResult{}, err
	}

	list, _, err := c.api.AccountAPI.ListKvBuckets(authCtx, accountID).Execute()
	if err != nil {
		return KeyValueResult{}, fmt.Errorf("list kv buckets: %w", err)
	}

	for _, kv := range list.Items {
		if kv.Config.Bucket != in.Bucket {
			continue
		}
		updateReq := inputToKVUpdateConfig(in)
		updated, _, err := c.api.KvBucketAPI.UpdateKvBucket(authCtx, kv.Id).JSKVBucketUpdateRequest(updateReq).Execute()
		if err != nil {
			return KeyValueResult{}, fmt.Errorf("update kv bucket %q: %w", in.Bucket, err)
		}
		return KeyValueResult{AccountID: accountID, KeyValueID: updated.Id}, nil
	}

	desired := inputToKVConfig(in)
	created, _, err := c.api.AccountAPI.CreateKvBucket(authCtx, accountID).JSKVBucketConfig(desired).Execute()
	if err != nil {
		return KeyValueResult{}, fmt.Errorf("create kv bucket %q: %w", in.Bucket, err)
	}

	return KeyValueResult{AccountID: accountID, KeyValueID: created.Id}, nil
}

func (c *client) DeleteKeyValue(ctx context.Context, in KeyValueInput) error {
	if in.Bucket == "" {
		return errors.New("kv bucket name is required")
	}

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	if in.KeyValueID != "" {
		_, err := c.api.KvBucketAPI.DeleteKvBucket(authCtx, in.KeyValueID).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			return nil
		}
		return fmt.Errorf("delete kv bucket by id %q: %w", in.KeyValueID, err)
	}

	accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
	if err != nil {
		if errors.Is(err, ErrAccountNotFound) {
			return nil
		}
		return err
	}

	list, _, err := c.api.AccountAPI.ListKvBuckets(authCtx, accountID).Execute()
	if err != nil {
		return fmt.Errorf("list kv buckets for delete: %w", err)
	}

	for _, kv := range list.Items {
		if kv.Config.Bucket != in.Bucket {
			continue
		}
		_, err := c.api.KvBucketAPI.DeleteKvBucket(authCtx, kv.Id).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			return nil
		}
		return fmt.Errorf("delete kv bucket %q: %w", in.Bucket, err)
	}

	return nil
}

// --- Object Bucket ---

func (c *client) EnsureObjectStore(ctx context.Context, in ObjectStoreInput) (ObjectStoreResult, error) {
	if in.Bucket == "" {
		return ObjectStoreResult{}, errors.New("object bucket name is required")
	}

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return ObjectStoreResult{}, err
	}

	if in.ObjectStoreID != "" {
		updateReq := inputToObjectStoreUpdateConfig(in)
		updated, _, err := c.api.ObjectBucketAPI.UpdateObjectBucket(authCtx, in.ObjectStoreID).JSObjectBucketUpdateRequest(updateReq).Execute()
		if err == nil {
			return ObjectStoreResult{AccountID: in.AccountID, ObjectStoreID: updated.Id}, nil
		}
		if !isStatusCode(err, http.StatusNotFound) {
			return ObjectStoreResult{}, fmt.Errorf("update object bucket by id %q: %w", in.ObjectStoreID, err)
		}
	}

	accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
	if err != nil {
		return ObjectStoreResult{}, err
	}

	list, _, err := c.api.AccountAPI.ListObjectBuckets(authCtx, accountID).Execute()
	if err != nil {
		return ObjectStoreResult{}, fmt.Errorf("list object buckets: %w", err)
	}

	for _, obj := range list.Items {
		if obj.Config.Bucket != in.Bucket {
			continue
		}
		updateReq := inputToObjectStoreUpdateConfig(in)
		updated, _, err := c.api.ObjectBucketAPI.UpdateObjectBucket(authCtx, obj.Id).JSObjectBucketUpdateRequest(updateReq).Execute()
		if err != nil {
			return ObjectStoreResult{}, fmt.Errorf("update object bucket %q: %w", in.Bucket, err)
		}
		return ObjectStoreResult{AccountID: accountID, ObjectStoreID: updated.Id}, nil
	}

	desired := inputToObjectStoreConfig(in)
	created, _, err := c.api.AccountAPI.CreateObjectBucket(authCtx, accountID).JSObjectBucketConfig(desired).Execute()
	if err != nil {
		return ObjectStoreResult{}, fmt.Errorf("create object bucket %q: %w", in.Bucket, err)
	}

	return ObjectStoreResult{AccountID: accountID, ObjectStoreID: created.Id}, nil
}

func (c *client) DeleteObjectStore(ctx context.Context, in ObjectStoreInput) error {
	if in.Bucket == "" {
		return errors.New("object bucket name is required")
	}

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	if in.ObjectStoreID != "" {
		_, err := c.api.ObjectBucketAPI.DeleteObjectBucket(authCtx, in.ObjectStoreID).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			return nil
		}
		return fmt.Errorf("delete object bucket by id %q: %w", in.ObjectStoreID, err)
	}

	accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
	if err != nil {
		if errors.Is(err, ErrAccountNotFound) {
			return nil
		}
		return err
	}

	list, _, err := c.api.AccountAPI.ListObjectBuckets(authCtx, accountID).Execute()
	if err != nil {
		return fmt.Errorf("list object buckets for delete: %w", err)
	}

	for _, obj := range list.Items {
		if obj.Config.Bucket != in.Bucket {
			continue
		}
		_, err := c.api.ObjectBucketAPI.DeleteObjectBucket(authCtx, obj.Id).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			return nil
		}
		return fmt.Errorf("delete object bucket %q: %w", in.Bucket, err)
	}

	return nil
}

// --- Consumer ---

func (c *client) EnsureConsumer(ctx context.Context, in ConsumerInput) (ConsumerResult, error) {
	if in.Name == "" {
		return ConsumerResult{}, errors.New("consumer name is required")
	}

	if in.StreamID == "" {
		return ConsumerResult{}, errors.New("consumer streamId is required")
	}

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return ConsumerResult{}, err
	}

	isPush := in.DeliverSubject != ""

	if in.ConsumerID != "" {
		if isPush {
			desired := pushConsumerConfig(in)
			updated, _, err := c.api.PushConsumerAPI.UpdatePushConsumer(authCtx, in.ConsumerID).JSPushConsumerUpdateRequest(desiredPushConsumerUpdateConfig(in)).Execute()
			if err == nil {
				_ = desired // used for create path
				return ConsumerResult{ConsumerID: updated.Id, StreamID: in.StreamID, IsPush: true}, nil
			}
			if !isStatusCode(err, http.StatusNotFound) {
				return ConsumerResult{}, fmt.Errorf("update push consumer by id %q: %w", in.ConsumerID, err)
			}
		} else {
			updated, _, err := c.api.PullConsumerAPI.UpdatePullConsumer(authCtx, in.ConsumerID).JSPullConsumerUpdateRequest(pullConsumerUpdateConfig(in)).Execute()
			if err == nil {
				return ConsumerResult{ConsumerID: updated.Id, StreamID: in.StreamID, IsPush: false}, nil
			}
			if !isStatusCode(err, http.StatusNotFound) {
				return ConsumerResult{}, fmt.Errorf("update pull consumer by id %q: %w", in.ConsumerID, err)
			}
		}
	}

	list, _, err := c.api.StreamAPI.ListConsumers(authCtx, in.StreamID).Execute()
	if err != nil {
		return ConsumerResult{}, fmt.Errorf("list consumers: %w", err)
	}

	for _, cons := range list.Items {
		if cons.Name != in.Name {
			continue
		}
		if isPush {
			updated, _, err := c.api.PushConsumerAPI.UpdatePushConsumer(authCtx, cons.Id).JSPushConsumerUpdateRequest(desiredPushConsumerUpdateConfig(in)).Execute()
			if err != nil {
				return ConsumerResult{}, fmt.Errorf("update push consumer %q: %w", in.Name, err)
			}
			return ConsumerResult{ConsumerID: updated.Id, StreamID: in.StreamID, IsPush: true}, nil
		}
		updated, _, err := c.api.PullConsumerAPI.UpdatePullConsumer(authCtx, cons.Id).JSPullConsumerUpdateRequest(pullConsumerUpdateConfig(in)).Execute()
		if err != nil {
			return ConsumerResult{}, fmt.Errorf("update pull consumer %q: %w", in.Name, err)
		}
		return ConsumerResult{ConsumerID: updated.Id, StreamID: in.StreamID, IsPush: false}, nil
	}

	if isPush {
		desired := pushConsumerConfig(in)
		created, _, err := c.api.StreamAPI.CreatePushConsumer(authCtx, in.StreamID).JSPushConsumerConfigRequest(desired).Execute()
		if err != nil {
			return ConsumerResult{}, fmt.Errorf("create push consumer %q: %w", in.Name, err)
		}
		return ConsumerResult{ConsumerID: created.Id, StreamID: in.StreamID, IsPush: true}, nil
	}

	desired := pullConsumerConfig(in)

	created, _, err := c.api.StreamAPI.CreatePullConsumer(authCtx, in.StreamID).JSPullConsumerConfigRequest(desired).Execute()
	if err != nil {
		return ConsumerResult{}, fmt.Errorf("create pull consumer %q: %w", in.Name, err)
	}
	return ConsumerResult{ConsumerID: created.Id, StreamID: in.StreamID, IsPush: false}, nil
}

func (c *client) DeleteConsumer(ctx context.Context, in ConsumerInput) error {
	if in.Name == "" {
		return errors.New("consumer name is required")
	}

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	if in.ConsumerID != "" {
		if in.IsPush {
			_, err := c.api.PushConsumerAPI.DeletePushConsumer(authCtx, in.ConsumerID).Execute()
			if err == nil || isStatusCode(err, http.StatusNotFound) {
				return nil
			}
			return fmt.Errorf("delete push consumer by id %q: %w", in.ConsumerID, err)
		}
		_, err := c.api.PullConsumerAPI.DeletePullConsumer(authCtx, in.ConsumerID).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			return nil
		}
		return fmt.Errorf("delete pull consumer by id %q: %w", in.ConsumerID, err)
	}

	if in.StreamID == "" {
		return nil
	}

	list, _, err := c.api.StreamAPI.ListConsumers(authCtx, in.StreamID).Execute()
	if err != nil {
		if isStatusCode(err, http.StatusNotFound) {
			return nil
		}
		return fmt.Errorf("list consumers for delete: %w", err)
	}

	for _, cons := range list.Items {
		if cons.Name != in.Name {
			continue
		}

		_, err := c.api.PullConsumerAPI.DeletePullConsumer(authCtx, cons.Id).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			return nil
		}

		_, err = c.api.PushConsumerAPI.DeletePushConsumer(authCtx, cons.Id).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			return nil
		}
		return fmt.Errorf("delete consumer %q: %w", in.Name, err)
	}

	return nil
}

// --- Config builders ---

func inputToStreamConfig(in StreamInput) syncp.JSStreamConfigRequest {
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

	replicas := max(int64(in.Replicas), 1)

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

func inputToKVConfig(in KeyValueInput) syncp.JSKVBucketConfig {
	maxAge := int64(0)
	if in.TTL != "" {
		if d, err := time.ParseDuration(in.TTL); err == nil {
			maxAge = int64(d)
		}
	}

	storage := syncp.STORAGETYPE_FILE
	if in.Storage == string(syncp.STORAGETYPE_MEMORY) {
		storage = syncp.STORAGETYPE_MEMORY
	}

	replicas := int64(in.Replicas)
	if replicas < 1 {
		replicas = 1
	}

	history := int64(in.History)
	if history < 1 {
		history = 1
	}

	description := in.Description
	maxValueSize := int64(in.MaxValueSize)
	maxBytes := int64(in.MaxBytes)

	cfg := syncp.JSKVBucketConfig{
		Bucket:       in.Bucket,
		Description:  &description,
		History:      &history,
		MaxAge:       &maxAge,
		MaxBytes:     &maxBytes,
		MaxValueSize: &maxValueSize,
		NumReplicas:  &replicas,
		Storage:      storage,
		Placement:    in.Placement,
		Republish:    in.RePublish,
		Mirror:       in.Mirror,
		Sources:      in.Sources,
	}
	if in.Compression {
		cfg.Compression = &in.Compression
	}

	return cfg
}

func inputToKVUpdateConfig(in KeyValueInput) syncp.JSKVBucketUpdateRequest {
	maxAge := int64(0)
	if in.TTL != "" {
		if d, err := time.ParseDuration(in.TTL); err == nil {
			maxAge = int64(d)
		}
	}

	replicas := int64(in.Replicas)
	if replicas < 1 {
		replicas = 1
	}

	history := int64(in.History)
	if history < 1 {
		history = 1
	}

	description := in.Description
	maxValueSize := int64(in.MaxValueSize)
	maxBytes := int64(in.MaxBytes)

	cfg := syncp.UpdatableKVBucketConfig{
		Description:  &description,
		History:      &history,
		MaxAge:       &maxAge,
		MaxBytes:     &maxBytes,
		MaxValueSize: &maxValueSize,
		NumReplicas:  &replicas,
		Placement:    in.Placement,
		Republish:    in.RePublish,
		Mirror:       in.Mirror,
		Sources:      in.Sources,
	}
	if in.Compression {
		cfg.Compression = &in.Compression
	}

	return syncp.JSKVBucketUpdateRequest{Config: cfg}
}

func inputToObjectStoreConfig(in ObjectStoreInput) syncp.JSObjectBucketConfig {
	maxAge := int64(0)
	if in.TTL != "" {
		if d, err := time.ParseDuration(in.TTL); err == nil {
			maxAge = int64(d)
		}
	}

	storage := syncp.STORAGETYPE_FILE
	if in.Storage == string(syncp.STORAGETYPE_MEMORY) {
		storage = syncp.STORAGETYPE_MEMORY
	}

	replicas := int64(in.Replicas)
	if replicas < 1 {
		replicas = 1
	}

	description := in.Description
	maxBytes := int64(in.MaxBytes)

	cfg := syncp.JSObjectBucketConfig{
		ObjectStoreConfig: syncp.ObjectStoreConfig{
			Bucket:      in.Bucket,
			Description: &description,
			MaxAge:      &maxAge,
			MaxBytes:    &maxBytes,
			NumReplicas: &replicas,
			Placement:   in.Placement,
			Metadata:    in.Metadata,
		},
		Storage: &storage,
	}
	if in.Compression {
		cfg.Compression = &in.Compression
	}

	return cfg
}

func inputToObjectStoreUpdateConfig(in ObjectStoreInput) syncp.JSObjectBucketUpdateRequest {
	maxAge := int64(0)
	if in.TTL != "" {
		if d, err := time.ParseDuration(in.TTL); err == nil {
			maxAge = int64(d)
		}
	}

	replicas := int64(in.Replicas)
	if replicas < 1 {
		replicas = 1
	}

	description := in.Description
	maxBytes := int64(in.MaxBytes)

	cfg := syncp.UpdatableObjectBucketConfig{
		Description: &description,
		MaxAge:      &maxAge,
		MaxBytes:    &maxBytes,
		NumReplicas: &replicas,
		Placement:   in.Placement,
		Metadata:    in.Metadata,
	}
	if in.Compression {
		cfg.Compression = &in.Compression
	}

	return syncp.JSObjectBucketUpdateRequest{Config: cfg}
}

func pullConsumerConfig(in ConsumerInput) syncp.JSPullConsumerConfigRequest {
	ackPolicy := mapAckPolicy(in.AckPolicy)
	deliverPolicy := mapDeliverPolicy(in.DeliverPolicy)
	replayPolicy := mapReplayPolicy(in.ReplayPolicy)

	replicas := int64(in.Replicas)
	description := in.Description
	name := in.Name

	cfg := syncp.JSPullConsumerConfigRequest{
		JSCommonConsumerConfigRequest: syncp.JSCommonConsumerConfigRequest{
			Name:          &name,
			Description:   &description,
			AckPolicy:     ackPolicy,
			DeliverPolicy: deliverPolicy,
			ReplayPolicy:  replayPolicy,
			NumReplicas:   replicas,
		},
	}

	if in.MaxAckPending > 0 {
		v := int64(in.MaxAckPending)
		cfg.MaxAckPending = &v
	}
	if in.MaxDeliver > 0 {
		v := int64(in.MaxDeliver)
		cfg.MaxDeliver = &v
	}
	if in.MemStorage {
		cfg.MemStorage = &in.MemStorage
	}
	if in.Direct {
		cfg.Direct = &in.Direct
	}
	if in.DurableName != "" {
		cfg.DurableName = &in.DurableName
	}
	if in.AckWait != "" {
		if d, err := time.ParseDuration(in.AckWait); err == nil {
			ns := int64(d)
			cfg.AckWait = &ns
		}
	}
	if len(in.FilterSubjects) > 0 {
		cfg.FilterSubjects = in.FilterSubjects
	}
	if in.InactiveThreshold != "" {
		if d, err := time.ParseDuration(in.InactiveThreshold); err == nil {
			ns := int64(d)
			cfg.InactiveThreshold = &ns
		}
	}
	if in.OptStartSeq > 0 {
		cfg.OptStartSeq = &in.OptStartSeq
	}
	if in.OptStartTime != "" {
		if t, err := time.Parse(time.RFC3339, in.OptStartTime); err == nil {
			cfg.OptStartTime = &t
		}
	}
	if in.SampleFreq != "" {
		cfg.SampleFreq = &in.SampleFreq
	}
	if len(in.Backoff) > 0 {
		cfg.Backoff = parseDurations(in.Backoff)
	}
	if in.Metadata != nil {
		// Metadata not directly on JSCommonConsumerConfigRequest; skip for now
	}

	// Pull-specific
	if in.MaxRequestBatch > 0 {
		v := int64(in.MaxRequestBatch)
		cfg.MaxBatch = &v
	}
	if in.MaxRequestMaxBytes > 0 {
		v := int64(in.MaxRequestMaxBytes)
		cfg.MaxBytes = &v
	}
	if in.MaxRequestExpires != "" {
		if d, err := time.ParseDuration(in.MaxRequestExpires); err == nil {
			ns := int64(d)
			cfg.MaxExpires = &ns
		}
	}
	if in.MaxWaiting > 0 {
		v := int64(in.MaxWaiting)
		cfg.MaxWaiting = &v
	}

	return cfg
}

func pushConsumerConfig(in ConsumerInput) syncp.JSPushConsumerConfigRequest {
	ackPolicy := mapAckPolicy(in.AckPolicy)
	deliverPolicy := mapDeliverPolicy(in.DeliverPolicy)
	replayPolicy := mapReplayPolicy(in.ReplayPolicy)

	replicas := int64(in.Replicas)
	description := in.Description
	name := in.Name

	cfg := syncp.JSPushConsumerConfigRequest{
		JSCommonConsumerConfigRequest: syncp.JSCommonConsumerConfigRequest{
			Name:          &name,
			Description:   &description,
			AckPolicy:     ackPolicy,
			DeliverPolicy: deliverPolicy,
			ReplayPolicy:  replayPolicy,
			NumReplicas:   replicas,
		},
		DeliverSubject: &in.DeliverSubject,
	}

	if in.MaxAckPending > 0 {
		v := int64(in.MaxAckPending)
		cfg.MaxAckPending = &v
	}
	if in.MaxDeliver > 0 {
		v := int64(in.MaxDeliver)
		cfg.MaxDeliver = &v
	}
	if in.MemStorage {
		cfg.MemStorage = &in.MemStorage
	}
	if in.Direct {
		cfg.Direct = &in.Direct
	}
	if in.DurableName != "" {
		cfg.DurableName = &in.DurableName
	}
	if in.DeliverGroup != "" {
		cfg.DeliverGroup = &in.DeliverGroup
	}
	if in.AckWait != "" {
		if d, err := time.ParseDuration(in.AckWait); err == nil {
			ns := int64(d)
			cfg.AckWait = &ns
		}
	}
	if len(in.FilterSubjects) > 0 {
		cfg.FilterSubjects = in.FilterSubjects
	}
	if in.InactiveThreshold != "" {
		if d, err := time.ParseDuration(in.InactiveThreshold); err == nil {
			ns := int64(d)
			cfg.InactiveThreshold = &ns
		}
	}
	if in.OptStartSeq > 0 {
		cfg.OptStartSeq = &in.OptStartSeq
	}
	if in.OptStartTime != "" {
		if t, err := time.Parse(time.RFC3339, in.OptStartTime); err == nil {
			cfg.OptStartTime = &t
		}
	}
	if in.SampleFreq != "" {
		cfg.SampleFreq = &in.SampleFreq
	}
	if len(in.Backoff) > 0 {
		cfg.Backoff = parseDurations(in.Backoff)
	}

	// Push-specific

	if in.FlowControl {
		cfg.FlowControl = &in.FlowControl
	}
	if in.HeadersOnly {
		cfg.HeadersOnly = &in.HeadersOnly
	}
	if in.HeartbeatInterval != "" {
		if d, err := time.ParseDuration(in.HeartbeatInterval); err == nil {
			ns := int64(d)
			cfg.IdleHeartbeat = &ns
		}
	}
	if in.RateLimitBps > 0 {
		cfg.RateLimitBps = &in.RateLimitBps
	}

	return cfg
}

func pullConsumerUpdateConfig(in ConsumerInput) syncp.JSPullConsumerUpdateRequest {
	description := in.Description
	cfg := syncp.JSPullConsumerUpdateRequest{
		Description: &description,
	}
	if in.AckWait != "" {
		if d, err := time.ParseDuration(in.AckWait); err == nil {
			ns := int64(d)
			cfg.AckWait = &ns
		}
	}
	if in.MaxAckPending > 0 {
		v := int64(in.MaxAckPending)
		cfg.MaxAckPending = &v
	}
	if in.MaxDeliver > 0 {
		v := int64(in.MaxDeliver)
		cfg.MaxDeliver = &v
	}
	if in.SampleFreq != "" {
		cfg.SampleFreq = &in.SampleFreq
	}
	return cfg
}

func desiredPushConsumerUpdateConfig(in ConsumerInput) syncp.JSPushConsumerUpdateRequest {
	description := in.Description
	cfg := syncp.JSPushConsumerUpdateRequest{
		Description: &description,
	}
	if in.AckWait != "" {
		if d, err := time.ParseDuration(in.AckWait); err == nil {
			ns := int64(d)
			cfg.AckWait = &ns
		}
	}
	if in.MaxAckPending > 0 {
		v := int64(in.MaxAckPending)
		cfg.MaxAckPending = &v
	}
	if in.MaxDeliver > 0 {
		v := int64(in.MaxDeliver)
		cfg.MaxDeliver = &v
	}
	if in.SampleFreq != "" {
		cfg.SampleFreq = &in.SampleFreq
	}
	if in.HeadersOnly {
		cfg.HeadersOnly = &in.HeadersOnly
	}
	return cfg
}

// --- Helpers ---

func mapAckPolicy(v string) syncp.AckPolicy {
	switch v {
	case string(syncp.ACKPOLICY_ALL):
		return syncp.ACKPOLICY_ALL
	case string(syncp.ACKPOLICY_NONE):
		return syncp.ACKPOLICY_NONE
	case string(syncp.ACKPOLICY_EXPLICIT):
		return syncp.ACKPOLICY_EXPLICIT
	default:
		return syncp.ACKPOLICY_EXPLICIT
	}
}

func mapDeliverPolicy(v string) syncp.DeliverPolicy {
	switch v {
	case string(syncp.DELIVERPOLICY_ALL):
		return syncp.DELIVERPOLICY_ALL
	case string(syncp.DELIVERPOLICY_LAST):
		return syncp.DELIVERPOLICY_LAST
	case string(syncp.DELIVERPOLICY_LAST_PER_SUBJECT):
		return syncp.DELIVERPOLICY_LAST_PER_SUBJECT
	case string(syncp.DELIVERPOLICY_NEW):
		return syncp.DELIVERPOLICY_NEW
	case string(syncp.DELIVERPOLICY_BY_START_SEQUENCE):
		return syncp.DELIVERPOLICY_BY_START_SEQUENCE
	case string(syncp.DELIVERPOLICY_BY_START_TIME):
		return syncp.DELIVERPOLICY_BY_START_TIME
	default:
		return syncp.DELIVERPOLICY_ALL
	}
}

func mapReplayPolicy(v string) syncp.ReplayPolicy {
	switch v {
	case string(syncp.REPLAYPOLICY_ORIGINAL):
		return syncp.REPLAYPOLICY_ORIGINAL
	default:
		return syncp.REPLAYPOLICY_INSTANT
	}
}

func parseDurations(in []string) []int64 {
	out := make([]int64, 0, len(in))
	for _, s := range in {
		if d, err := time.ParseDuration(s); err == nil {
			out = append(out, int64(d))
		}
	}
	return out
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
