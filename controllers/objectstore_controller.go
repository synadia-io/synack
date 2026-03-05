package controllers

import (
	"context"
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

	natsv1alpha1 "github.com/synadia-io/synack/api/v1alpha1"
	"github.com/synadia-io/synack/internal/controlplane"
)

// ObjectStoreReconciler reconciles an ObjectStore object.
type ObjectStoreReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	ControlPlane controlplane.Client
}

const ObjectStoreFinalizer = "synack.synadia.io/ObjectStore-finalizer"

// +kubebuilder:rbac:groups=synack.synadia.io,resources=ObjectStores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=synack.synadia.io,resources=ObjectStores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=ObjectStores/finalizers,verbs=update

func (r *ObjectStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var obj natsv1alpha1.ObjectStore
	if err := r.Get(ctx, req.NamespacedName, &obj); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	knownID := obj.Status.ObjectStoreID
	if obj.Spec.ObjectStoreID != "" {
		knownID = obj.Spec.ObjectStoreID
	}

	if !obj.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&obj, ObjectStoreFinalizer) {
			if obj.Status.ObjectStoreID == "" {
				if ok := controllerutil.RemoveFinalizer(&obj, ObjectStoreFinalizer); !ok {
					return ctrl.Result{}, nil
				}
				if err := r.Update(ctx, &obj); err != nil {
					return requeueOnConflict(err)
				}
				return ctrl.Result{}, nil
			}

			if err := r.ControlPlane.DeleteObjectStore(ctx, controlplane.ObjectStoreInput{
				AccountSelectors: controlplane.AccountSelectors{
					AccountID:         obj.Spec.AccountID,
					AccountPublicNKey: obj.Spec.AccountPublicNKey,
					SystemID:          obj.Spec.SystemID,
				},
				ObjectStoreID: knownID,
				Bucket:        obj.Spec.Bucket,
			}); err != nil {
				l.Error(err, "control plane delete failed")
				obj.Status.Message = err.Error()
				_ = r.Status().Update(ctx, &obj)
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}

			if ok := controllerutil.RemoveFinalizer(&obj, ObjectStoreFinalizer); !ok {
				return ctrl.Result{}, nil
			}
			if err := r.Update(ctx, &obj); err != nil {
				return requeueOnConflict(err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&obj, ObjectStoreFinalizer) {
		if ok := controllerutil.AddFinalizer(&obj, ObjectStoreFinalizer); !ok {
			return ctrl.Result{}, nil
		}
		if err := r.Update(ctx, &obj); err != nil {
			return requeueOnConflict(err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if err := validateAccountSelectors(obj.Spec.AccountSelector); err != nil {
		obj.Status.Message = err.Error()
		_ = r.Status().Update(ctx, &obj)
		return ctrl.Result{}, nil
	}

	accountID, err := resolveAccountRef(ctx, r.Client, obj.Namespace, obj.Spec.AccountSelector)
	if err != nil {
		if errors.Is(err, errWaitingForAccount) {
			obj.Status.Message = err.Error()
			_ = r.Status().Update(ctx, &obj)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		l.Error(err, "failed to resolve account for object bucket")
		obj.Status.Message = err.Error()
		_ = r.Status().Update(ctx, &obj)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	out, err := r.ControlPlane.EnsureObjectStore(ctx, controlplane.ObjectStoreInput{
		AccountSelectors: controlplane.AccountSelectors{
			AccountID:         accountID,
			AccountPublicNKey: obj.Spec.AccountPublicNKey,
			SystemID:          obj.Spec.SystemID,
		},
		ObjectStoreID: knownID,
		Bucket:        obj.Spec.Bucket,
		Description:   obj.Spec.Description,
		TTL:           obj.Spec.TTL,
		MaxBytes:      obj.Spec.MaxBytes,
		Storage:       obj.Spec.Storage,
		Replicas:      obj.Spec.Replicas,
		Compression:   obj.Spec.Compression,
		Placement:     toSCPPlacement(obj.Spec.Placement),
		Metadata:      obj.Spec.Metadata,
	})
	if err != nil {
		l.Error(err, "control plane apply failed")
		obj.Status.Message = err.Error()
		_ = r.Status().Update(ctx, &obj)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	desiredStatus := obj.Status
	desiredStatus.ObservedGeneration = obj.Generation
	desiredStatus.ObjectStoreID = out.ObjectStoreID
	desiredStatus.Message = "applied"
	if desiredStatus != obj.Status {
		desiredStatus.LastSyncedAt = time.Now().UTC().Format(time.RFC3339)
		obj.Status = desiredStatus
		if err := r.Status().Update(ctx, &obj); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *ObjectStoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1alpha1.ObjectStore{}).
		Watches(&natsv1alpha1.Account{}, handler.EnqueueRequestsFromMapFunc(r.enqueueObjectStoresForAccount)).
		Complete(r)
}

func (r *ObjectStoreReconciler) enqueueObjectStoresForAccount(ctx context.Context, obj client.Object) []reconcile.Request {
	account, ok := obj.(*natsv1alpha1.Account)
	if !ok {
		return nil
	}

	var buckets natsv1alpha1.ObjectStoreList
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
