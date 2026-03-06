package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	natsv1 "github.com/synadia-io/synack/api/v1alpha1"
)

const (
	conflictRetryDelay = 2 * time.Second

	accountAppliedStateAnnotation     = "synack.synadia.io/last-applied-account-input"
	streamAppliedStateAnnotation      = "synack.synadia.io/last-applied-stream-input"
	keyValueAppliedStateAnnotation    = "synack.synadia.io/last-applied-keyvalue-input"
	objectStoreAppliedStateAnnotation = "synack.synadia.io/last-applied-objectstore-input"
	consumerAppliedStateAnnotation    = "synack.synadia.io/last-applied-consumer-input"

	accountServerStateAnnotation     = "synack.synadia.io/last-server-account-state"
	streamServerStateAnnotation      = "synack.synadia.io/last-server-stream-state"
	keyValueServerStateAnnotation    = "synack.synadia.io/last-server-keyvalue-state"
	objectStoreServerStateAnnotation = "synack.synadia.io/last-server-objectstore-state"
	consumerServerStateAnnotation    = "synack.synadia.io/last-server-consumer-state"
)

func requeueOnConflict(err error) (ctrl.Result, error) {
	if apierrors.IsConflict(err) {
		return ctrl.Result{RequeueAfter: conflictRetryDelay}, nil
	}

	return ctrl.Result{}, err
}

func diffState(appliedState, desiredState []byte) (string, error) {
	if len(appliedState) == 0 || len(desiredState) == 0 {
		return "", nil
	}

	var applied, desired any

	if err := json.Unmarshal(appliedState, &applied); err != nil {
		return "", err
	}
	if err := json.Unmarshal(desiredState, &desired); err != nil {
		return "", err
	}

	return cmp.Diff(applied, desired), nil

}

func logStateDiff(log logr.Logger, resource string, diff string) {
	if diff == "" {
		return
	}

	log.Info(resource + " desired state changed\n" + diff)
}

func loadAnnotation(obj client.Object, key string) []byte {
	if obj.GetAnnotations() == nil {
		return nil
	}
	return []byte(obj.GetAnnotations()[key])
}

// hasDependentStreams returns true if any Stream in the same namespace references the given account by name.
func hasDependentStreams(ctx context.Context, c client.Client, namespace, accountName string) (bool, error) {
	var streams natsv1.StreamList
	if err := c.List(ctx, &streams, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for _, s := range streams.Items {
		if s.Spec.AccountRef != nil && s.Spec.AccountRef.Name == accountName {
			return true, nil
		}
	}
	return false, nil
}

// hasDependentKeyValues returns true if any KeyValue in the same namespace references the given account by name.
func hasDependentKeyValues(ctx context.Context, c client.Client, namespace, accountName string) (bool, error) {
	var kvs natsv1.KeyValueList
	if err := c.List(ctx, &kvs, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for _, kv := range kvs.Items {
		if kv.Spec.AccountRef != nil && kv.Spec.AccountRef.Name == accountName {
			return true, nil
		}
	}
	return false, nil
}

// hasDependentObjectStores returns true if any ObjectStore in the same namespace references the given account by name.
func hasDependentObjectStores(ctx context.Context, c client.Client, namespace, accountName string) (bool, error) {
	var objs natsv1.ObjectStoreList
	if err := c.List(ctx, &objs, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for _, o := range objs.Items {
		if o.Spec.AccountRef != nil && o.Spec.AccountRef.Name == accountName {
			return true, nil
		}
	}
	return false, nil
}

// hasDependentConsumers returns true if any Consumer in the same namespace references the given stream by name.
func hasDependentConsumers(ctx context.Context, c client.Client, namespace, streamName string) (bool, error) {
	var consumers natsv1.ConsumerList
	if err := c.List(ctx, &consumers, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for _, c := range consumers.Items {
		if c.Spec.StreamRef != nil && c.Spec.StreamRef.Name == streamName {
			return true, nil
		}
	}
	return false, nil
}

// checkAccountDependents returns an error describing any resources that still reference the account.
func checkAccountDependents(ctx context.Context, c client.Client, namespace, accountName string) error {
	hasStreams, err := hasDependentStreams(ctx, c, namespace, accountName)
	if err != nil {
		return fmt.Errorf("failed to check dependent streams: %w", err)
	}
	hasKVs, err := hasDependentKeyValues(ctx, c, namespace, accountName)
	if err != nil {
		return fmt.Errorf("failed to check dependent key values: %w", err)
	}
	hasObjs, err := hasDependentObjectStores(ctx, c, namespace, accountName)
	if err != nil {
		return fmt.Errorf("failed to check dependent object stores: %w", err)
	}

	if hasStreams || hasKVs || hasObjs {
		return fmt.Errorf("waiting for dependent resources to be deleted before removing Account %q", accountName)
	}
	return nil
}

func setAnnotations(obj client.Object, key string, value []byte) bool {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	if annotations[key] == string(value) {
		return false
	}
	annotations[key] = string(value)
	obj.SetAnnotations(annotations)
	return true
}
