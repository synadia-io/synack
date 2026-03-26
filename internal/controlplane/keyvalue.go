package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/synadia-io/control-plane-sdk-go/syncp"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

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

func (c *client) EnsureKeyValue(ctx context.Context, in KeyValueInput) (KeyValueResult, error) {
	l := log.FromContext(ctx).WithValues("resourceType", "kvBucket", "resourceName", in.Bucket)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return KeyValueResult{}, err
	}

	// If we already know the ID, use it directly — never fall through to create.
	if in.KeyValueID != "" {
		updateReq := inputToKVUpdateConfig(in)
		updated, _, err := c.api.KvBucketAPI.UpdateKvBucket(authCtx, in.KeyValueID).JSKVBucketUpdateRequest(updateReq).Execute()
		if err != nil {
			return KeyValueResult{}, fmt.Errorf("update kv bucket by id %q: %w", in.KeyValueID, err)
		}
		l.Info("keyvalue updated", "resourceID", updated.Id, "accountID", in.AccountID)
		return KeyValueResult{AccountID: in.AccountID, KeyValueID: updated.Id}, nil
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
		l.Info("keyvalue updated", "resourceID", updated.Id, "accountID", accountID)
		return KeyValueResult{AccountID: accountID, KeyValueID: updated.Id}, nil
	}

	desired := inputToKVConfig(in)
	created, _, err := c.api.AccountAPI.CreateKvBucket(authCtx, accountID).JSKVBucketConfig(desired).Execute()
	if err != nil {
		return KeyValueResult{}, fmt.Errorf("create kv bucket %q: %w", in.Bucket, err)
	}

	l.Info("keyvalue created", "resourceID", created.Id, "accountID", accountID)

	return KeyValueResult{AccountID: accountID, KeyValueID: created.Id}, nil
}

func (c *client) DeleteKeyValue(ctx context.Context, in KeyValueInput) error {
	l := log.FromContext(ctx).WithValues("resourceType", "kvBucket", "resourceName", in.Bucket)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	if in.KeyValueID != "" {
		_, err := c.api.KvBucketAPI.DeleteKvBucket(authCtx, in.KeyValueID).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			l.Info("keyvalue deleted", "resourceID", in.KeyValueID)
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
			l.Info("keyvalue deleted", "resourceID", kv.Id, "accountID", accountID)
			return nil
		}
		return fmt.Errorf("delete kv bucket %q: %w", in.Bucket, err)
	}

	return nil
}

func (c *client) ReadKeyValueState(ctx context.Context, in KeyValueInput) ([]byte, bool, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, false, err
	}

	if in.KeyValueID != "" {
		kv, _, err := c.api.KvBucketAPI.GetKvBucket(authCtx, in.KeyValueID).Execute()
		if err != nil {
			if isStatusCode(err, http.StatusNotFound) {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("get kv bucket by id %q: %w", in.KeyValueID, err)
		}
		state, err := json.Marshal(kv.Config)
		if err != nil {
			return nil, false, err
		}
		return state, true, nil
	}

	accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
	if err != nil {
		if errors.Is(err, ErrAccountNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	list, _, err := c.api.AccountAPI.ListKvBuckets(authCtx, accountID).Execute()
	if err != nil {
		return nil, false, fmt.Errorf("list kv buckets: %w", err)
	}

	for _, kv := range list.Items {
		if kv.Config.Bucket != in.Bucket {
			continue
		}
		state, err := json.Marshal(kv.Config)
		if err != nil {
			return nil, false, err
		}
		return state, true, nil
	}

	return nil, false, nil
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
