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

// ObjectStoreReconciler reconciles an ObjectStore object.
type ObjectStoreReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ControlPlane    controlplane.Client
	RequeueInterval time.Duration
}

const objectStoreFinalizer = "synack.synadia.io/objectstore-finalizer"

// +kubebuilder:rbac:groups=synack.synadia.io,resources=objectstores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=synack.synadia.io,resources=objectstores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=objectstores/finalizers,verbs=update

func (r *ObjectStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var obj natsv1.ObjectStore
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
		if controllerutil.ContainsFinalizer(&obj, objectStoreFinalizer) {

			// If we never had an object store ID, this resource was never fully reconciled
			if obj.Status.ObjectStoreID == "" {
				if ok := controllerutil.RemoveFinalizer(&obj, objectStoreFinalizer); !ok {
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
				l.Error(err, "object store delete failed")
				obj.Status.Message = err.Error()
				if err := r.Status().Update(ctx, &obj); err != nil {
					l.Error(err, "failed to update objectstore status")
				}
				return requeueReconcileErr, nil
			}

			if ok := controllerutil.RemoveFinalizer(&obj, objectStoreFinalizer); !ok {
				return ctrl.Result{}, nil
			}

			if err := r.Update(ctx, &obj); err != nil {
				return requeueOnConflict(err)
			}
		}

		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&obj, objectStoreFinalizer) {
		if ok := controllerutil.AddFinalizer(&obj, objectStoreFinalizer); !ok {
			return ctrl.Result{}, nil
		}

		if err := r.Update(ctx, &obj); err != nil {
			return requeueOnConflict(err)
		}

		return ctrl.Result{Requeue: true}, nil
	}

	if err := validateAccountSelectors(obj.Spec.AccountSelector); err != nil {
		obj.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &obj); err != nil {
			l.Error(err, "failed to update object store status")
		}
		return ctrl.Result{}, nil
	}

	accountID, err := resolveAccountRef(ctx, r.Client, obj.Namespace, obj.Spec.AccountSelector)
	if err != nil {
		if errors.Is(err, errWaitingForAccount) {
			obj.Status.Message = err.Error()
			if err := r.Status().Update(ctx, &obj); err != nil {
				l.Error(err, "failed to update object store status")
			}
			return requeueWaitingForResource, nil
		}

		l.Error(err, "failed to resolve account for object bucket")
		obj.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &obj); err != nil {
			l.Error(err, "failed to update object store status")
		}
		return requeueReconcileErr, nil
	}

	in := controlplane.ObjectStoreInput{
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
	}

	appliedState := loadAnnotation(&obj, appliedStateAnnotation)
	desiredState, err := json.Marshal(in)
	if err != nil {
		obj.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &obj); err != nil {
			l.Error(err, "failed to update object store status")
		}
		return requeueReconcileErr, nil
	}

	specChanged := false
	if diff, err := diffState(appliedState, desiredState); err != nil {
		l.Error(err, "failed to diff object store state")
		obj.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &obj); err != nil {
			l.Error(err, "failed to update object store status")
		}
		return requeueReconcileErr, nil
	} else if diff != "" {
		logStateDiff(l, "object store", diff)
		specChanged = true
	}

	if !specChanged && obj.Status.ObjectStoreID != "" {
		serverState, found, err := r.ControlPlane.ReadObjectStoreState(ctx, in)
		if err != nil {
			l.Error(err, "failed to read object store server state")
			return requeueReconcileErr, nil
		}

		lastServerState := loadAnnotation(&obj, serverStateAnnotation)
		if found && lastServerState != nil {
			diff, err := diffState(serverState, lastServerState)
			if err != nil {
				l.Error(err, "failed to diff server state")
			} else if diff == "" {
				return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
			} else {
				l.Info("server-side drift detected for object store. Reverting...\n" + diff)
			}
		} else if !found {
			l.Info("object store not found on server, will re-create")
		}
	}

	out, err := r.ControlPlane.EnsureObjectStore(ctx, in)
	if err != nil {
		l.Error(err, "object store update failed")
		obj.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &obj); err != nil {
			l.Error(err, "failed to update object store status")
		}
		return requeueReconcileErr, nil
	}

	desiredStatus := obj.Status
	desiredStatus.ObjectStoreID = out.ObjectStoreID
	desiredStatus.Message = messageApplied

	if desiredStatus != obj.Status {
		desiredStatus.LastSynced = time.Now().UTC().Format(time.RFC3339)
		obj.Status = desiredStatus
		if err := r.Status().Update(ctx, &obj); err != nil {
			return requeueOnConflict(err)
		}
	}

	readIn := in
	readIn.ObjectStoreID = out.ObjectStoreID
	newServerState, _, _ := r.ControlPlane.ReadObjectStoreState(ctx, readIn)

	annotationsChanged := setAnnotations(&obj, appliedStateAnnotation, desiredState)
	if newServerState != nil {
		if setAnnotations(&obj, serverStateAnnotation, newServerState) {
			annotationsChanged = true
		}
	}
	if annotationsChanged {
		if err := r.Update(ctx, &obj); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
}

func (r *ObjectStoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1.ObjectStore{}).
		Watches(&natsv1.Account{}, handler.EnqueueRequestsFromMapFunc(r.enqueueObjectStoresForAccount)).
		Complete(r)
}

func (r *ObjectStoreReconciler) enqueueObjectStoresForAccount(ctx context.Context, obj client.Object) []reconcile.Request {
	account, ok := obj.(*natsv1.Account)
	if !ok {
		return nil
	}

	var buckets natsv1.ObjectStoreList
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
