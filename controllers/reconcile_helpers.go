package controllers

import (
	"encoding/json"
	"time"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	conflictRetryDelay = 2 * time.Second

	accountAppliedStateAnnotation     = "synack.synadia.io/last-applied-account-input"
	streamAppliedStateAnnotation      = "synack.synadia.io/last-applied-stream-input"
	keyValueAppliedStateAnnotation    = "synack.synadia.io/last-applied-keyvalue-input"
	objectStoreAppliedStateAnnotation = "synack.synadia.io/last-applied-objectstore-input"
	consumerAppliedStateAnnotation    = "synack.synadia.io/last-applied-consumer-input"
)

func requeueOnConflict(err error) (ctrl.Result, error) {
	if apierrors.IsConflict(err) {
		return ctrl.Result{RequeueAfter: conflictRetryDelay}, nil
	}

	return ctrl.Result{}, err
}

func diffState(appliedState, desiredState []byte) (string, error) {
	var applied, desired any

	if err := json.Unmarshal(appliedState, &applied); err != nil {
		return "", err
	}
	if err := json.Unmarshal(desiredState, &desired); err != nil {
		return "", err
	}

	return cmp.Diff(applied, desired), nil

}

func loadAnnotation(obj client.Object, key string) []byte {
	if obj.GetAnnotations() == nil {
		return nil
	}
	return []byte(obj.GetAnnotations()[key])
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
