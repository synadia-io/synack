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

func (c *client) EnsureObjectStore(ctx context.Context, in ObjectStoreInput) (ObjectStoreResult, error) {
	l := log.FromContext(ctx).WithValues("resourceType", "objectStore", "resourceName", in.Bucket)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return ObjectStoreResult{}, err
	}

	// If we already know the ID use it directly, don't fall through to create.
	if in.ObjectStoreID != "" {
		updateReq := inputToObjectStoreUpdateConfig(in)
		updated, _, err := c.api.ObjectBucketAPI.UpdateObjectBucket(authCtx, in.ObjectStoreID).JSObjectBucketUpdateRequest(updateReq).Execute()
		if err != nil {
			return ObjectStoreResult{}, fmt.Errorf("update object bucket by id %q: %w", in.ObjectStoreID, err)
		}
		l.Info("object store updated", "resourceID", updated.Id, "accountID", in.AccountID)
		return ObjectStoreResult{AccountID: in.AccountID, ObjectStoreID: updated.Id}, nil
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
		l.Info("object store updated", "resourceID", updated.Id, "accountID", accountID)
		return ObjectStoreResult{AccountID: accountID, ObjectStoreID: updated.Id}, nil
	}

	desired := inputToObjectStoreConfig(in)
	created, _, err := c.api.AccountAPI.CreateObjectBucket(authCtx, accountID).JSObjectBucketConfig(desired).Execute()
	if err != nil {
		return ObjectStoreResult{}, fmt.Errorf("create object bucket %q: %w", in.Bucket, err)
	}
	l.Info("object store created", "resourceID", created.Id, "accountID", accountID)

	return ObjectStoreResult{AccountID: accountID, ObjectStoreID: created.Id}, nil
}

func (c *client) DeleteObjectStore(ctx context.Context, in ObjectStoreInput) error {
	l := log.FromContext(ctx).WithValues("resourceType", "objectStore", "resourceName", in.Bucket)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	if in.ObjectStoreID != "" {
		_, err := c.api.ObjectBucketAPI.DeleteObjectBucket(authCtx, in.ObjectStoreID).Execute()
		if err == nil || isStatusCode(err, http.StatusNotFound) {
			l.Info("object store deleted", "resourceID", in.ObjectStoreID)
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
			l.Info("object store deleted", "resourceID", obj.Id, "accountID", accountID)
			return nil
		}
		return fmt.Errorf("delete object bucket %q: %w", in.Bucket, err)
	}

	return nil
}

func (c *client) ReadObjectStoreState(ctx context.Context, in ObjectStoreInput) ([]byte, bool, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, false, err
	}

	if in.ObjectStoreID != "" {
		obj, _, err := c.api.ObjectBucketAPI.GetObjectBucket(authCtx, in.ObjectStoreID).Execute()
		if err != nil {
			if isStatusCode(err, http.StatusNotFound) {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("get object bucket by id %q: %w", in.ObjectStoreID, err)
		}
		state, err := json.Marshal(obj.Config)
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

	list, _, err := c.api.AccountAPI.ListObjectBuckets(authCtx, accountID).Execute()
	if err != nil {
		return nil, false, fmt.Errorf("list object buckets: %w", err)
	}

	for _, obj := range list.Items {
		if obj.Config.Bucket != in.Bucket {
			continue
		}
		state, err := json.Marshal(obj.Config)
		if err != nil {
			return nil, false, err
		}
		return state, true, nil
	}

	return nil, false, nil
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
