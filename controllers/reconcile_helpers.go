package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	natsv1 "github.com/synadia-io/synack/api/v1alpha1"
)

const (
	conflictRetryDelay = 2 * time.Second

	appliedStateAnnotation = "synack.synadia.io/last-applied-state"
	serverStateAnnotation  = "synack.synadia.io/last-server-state"

	messageApplied = "applied"
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

	patch, err := jsonpatch.CreateMergePatch(appliedState, desiredState)
	if err != nil {
		return "", err
	}

	compactPatch := &bytes.Buffer{}
	if err := json.Compact(compactPatch, patch); err != nil {
		return "", err
	}

	if compactPatch.String() == "{}" {
		return "", nil
	}

	return compactPatch.String(), nil
}

func logStateDiff(log logr.Logger, resource string, diff string) {
	if diff == "" {
		return
	}

	var diffObject any
	if err := json.Unmarshal([]byte(diff), &diffObject); err == nil {
		log.Info(resource+" desired state changed", "mergePatch", diffObject)
		return
	}

	// Fallback for non-JSON diffs so we still emit something useful.
	log.Info(resource+" desired state changed", "diff", diff)
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

// hasDependentNatsUsers returns true if any NatsUser in the same namespace references the given account by name.
func hasDependentNatsUsers(ctx context.Context, c client.Client, namespace, accountName string) (bool, error) {
	var users natsv1.NatsUserList
	if err := c.List(ctx, &users, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for _, u := range users.Items {
		if u.Spec.AccountRef != nil && u.Spec.AccountRef.Name == accountName {
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
	hasUsers, err := hasDependentNatsUsers(ctx, c, namespace, accountName)
	if err != nil {
		return fmt.Errorf("failed to check dependent nats users: %w", err)
	}

	if hasStreams || hasKVs || hasObjs || hasUsers {
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
