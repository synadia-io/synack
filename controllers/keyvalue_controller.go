package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	natsv1 "github.com/synadia-io/synack/api/v1alpha1"
	"github.com/synadia-io/synack/internal/controlplane"
)

// KeyValueReconciler reconciles a KeyValue object.
type KeyValueReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ControlPlane    controlplane.Client
	RequeueInterval time.Duration
}

const keyValueFinalizer = "synack.synadia.io/keyvalue-finalizer"

// +kubebuilder:rbac:groups=synack.synadia.io,resources=keyvalues,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=synack.synadia.io,resources=keyvalues/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=keyvalues/finalizers,verbs=update

func (r *KeyValueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var kv natsv1.KeyValue
	if err := r.Get(ctx, req.NamespacedName, &kv); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	knownID := kv.Status.KeyValueID
	if kv.Spec.KeyValueID != "" {
		knownID = kv.Spec.KeyValueID
	}

	if !kv.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&kv, keyValueFinalizer) {

			// If we never had a KV ID, this resource was never fully reconciled
			if kv.Status.KeyValueID == "" {
				if ok := controllerutil.RemoveFinalizer(&kv, keyValueFinalizer); !ok {
					return ctrl.Result{}, nil
				}
				if err := r.Update(ctx, &kv); err != nil {
					return requeueOnConflict(err)
				}
				return ctrl.Result{}, nil
			}

			if err := r.ControlPlane.DeleteKeyValue(ctx, controlplane.KeyValueInput{
				AccountSelectors: controlplane.AccountSelectors{
					AccountID:         kv.Spec.AccountID,
					AccountPublicNKey: kv.Spec.AccountPublicNKey,
					SystemID:          kv.Spec.SystemID,
				},
				KeyValueID: knownID,
				Bucket:     kv.Spec.Bucket,
			}); err != nil {
				l.Error(err, "keyvalue delete failed")
				kv.Status.Message = err.Error()
				if err := r.Status().Update(ctx, &kv); err != nil {
					l.Error(err, "failed to update keyvalue status")
				}
				return requeueReconcileErr, nil
			}

			if ok := controllerutil.RemoveFinalizer(&kv, keyValueFinalizer); !ok {
				return ctrl.Result{}, nil
			}

			if err := r.Update(ctx, &kv); err != nil {
				return requeueOnConflict(err)
			}
		}

		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&kv, keyValueFinalizer) {
		if ok := controllerutil.AddFinalizer(&kv, keyValueFinalizer); !ok {
			return ctrl.Result{}, nil
		}

		if err := r.Update(ctx, &kv); err != nil {
			return requeueOnConflict(err)
		}

		return ctrl.Result{Requeue: true}, nil
	}

	if err := validateAccountSelectors(kv.Spec.AccountSelector); err != nil {
		kv.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &kv); err != nil {
			l.Error(err, "failed to update keyvalue status")
		}
		return ctrl.Result{}, nil
	}

	accountID, err := resolveAccountRef(ctx, r.Client, kv.Namespace, kv.Spec.AccountSelector)
	if err != nil {
		if errors.Is(err, errWaitingForAccount) {
			kv.Status.Message = err.Error()
			if err := r.Status().Update(ctx, &kv); err != nil {
				l.Error(err, "failed to update keyvalue status")
			}
			return requeueWaitingForResource, nil
		}

		l.Error(err, "failed to resolve account for kv bucket")
		kv.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &kv); err != nil {
			l.Error(err, "failed to update keyvalue status")
		}
		return requeueReconcileErr, nil
	}

	in := controlplane.KeyValueInput{
		AccountSelectors: controlplane.AccountSelectors{
			AccountID:         accountID,
			AccountPublicNKey: kv.Spec.AccountPublicNKey,
			SystemID:          kv.Spec.SystemID,
		},
		KeyValueID:   knownID,
		Bucket:       kv.Spec.Bucket,
		Description:  kv.Spec.Description,
		History:      kv.Spec.History,
		TTL:          kv.Spec.TTL,
		MaxBytes:     kv.Spec.MaxBytes,
		MaxValueSize: kv.Spec.MaxValueSize,
		Storage:      kv.Spec.Storage,
		Replicas:     kv.Spec.Replicas,
		Compression:  kv.Spec.Compression,
		Placement:    toSCPPlacement(kv.Spec.Placement),
		RePublish:    toSCPRePublish(kv.Spec.RePublish),
		Mirror:       toSCPStreamSourcePtr(kv.Spec.Mirror),
		Sources:      toSCPStreamSources(kv.Spec.Sources),
	}

	appliedState := loadAnnotation(&kv, keyValueAppliedStateAnnotation)
	desiredState, err := json.Marshal(in)
	if err != nil {
		kv.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &kv); err != nil {
			l.Error(err, "failed to update keyvalue status")
		}
		return requeueReconcileErr, nil
	}

	specChanged := false
	if diff, err := diffState(appliedState, desiredState); err != nil {
		l.Error(err, "failed to diff keyvalue state")
		kv.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &kv); err != nil {
			l.Error(err, "failed to update keyvalue status")
		}
		return requeueReconcileErr, nil
	} else if diff != "" {
		logStateDiff(l, "keyvalue", diff)
		specChanged = true
	}

	if !specChanged && kv.Status.KeyValueID != "" {
		serverState, found, err := r.ControlPlane.ReadKeyValueState(ctx, in)
		if err != nil {
			l.Error(err, "failed to read keyvalue server state")
			return requeueReconcileErr, nil
		}

		lastServerState := loadAnnotation(&kv, keyValueServerStateAnnotation)
		if found && lastServerState != nil {
			diff, err := diffState(serverState, lastServerState)
			if err != nil {
				l.Error(err, "failed to diff server state")
			} else if diff == "" {
				return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
			} else {
				l.Info("server-side drift detected for keyvalue. Reverting...\n" + diff)
			}
		} else if !found {
			l.Info("keyvalue not found on server, will re-create")
		}
	}

	out, err := r.ControlPlane.EnsureKeyValue(ctx, in)
	if err != nil {
		l.Error(err, "keyvalue update failed")
		kv.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &kv); err != nil {
			l.Error(err, "failed to update keyvalue status")
		}
		return requeueReconcileErr, nil
	}

	desiredStatus := kv.Status
	desiredStatus.KeyValueID = out.KeyValueID
	desiredStatus.Message = "applied"

	if desiredStatus != kv.Status {
		desiredStatus.LastSynced = time.Now().UTC().Format(time.RFC3339)
		kv.Status = desiredStatus
		if err := r.Status().Update(ctx, &kv); err != nil {
			return requeueOnConflict(err)
		}
	}

	readIn := in
	readIn.KeyValueID = out.KeyValueID
	newServerState, _, _ := r.ControlPlane.ReadKeyValueState(ctx, readIn)

	annotationsChanged := setAnnotations(&kv, keyValueAppliedStateAnnotation, desiredState)
	if newServerState != nil {
		if setAnnotations(&kv, keyValueServerStateAnnotation, newServerState) {
			annotationsChanged = true
		}
	}
	if annotationsChanged {
		if err := r.Update(ctx, &kv); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
}

func (r *KeyValueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1.KeyValue{}).
		Watches(&natsv1.Account{}, handler.EnqueueRequestsFromMapFunc(r.enqueueKVBucketsForAccount)).
		Complete(r)
}

func (r *KeyValueReconciler) enqueueKVBucketsForAccount(ctx context.Context, obj client.Object) []reconcile.Request {
	account, ok := obj.(*natsv1.Account)
	if !ok {
		return nil
	}

	var buckets natsv1.KeyValueList
	if err := r.List(ctx, &buckets, client.InNamespace(account.Namespace)); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for _, bucket := range buckets.Items {
		if bucket.Spec.AccountRef == nil {
			continue
		}
		if bucket.Spec.AccountRef.Name != account.Name {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: bucket.Namespace,
				Name:      bucket.Name,
			},
		})
	}
	return requests
}
