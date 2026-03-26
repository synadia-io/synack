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

// NatsUserReconciler reconciles a NatsUser object.
type NatsUserReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ControlPlane    controlplane.Client
	RequeueInterval time.Duration
}

const natsUserFinalizer = "synack.synadia.io/natsuser-finalizer"

// +kubebuilder:rbac:groups=synack.synadia.io,resources=natsusers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=synack.synadia.io,resources=natsusers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=natsusers/finalizers,verbs=update
// +kubebuilder:rbac:groups=synack.synadia.io,resources=accounts,verbs=get;list;watch

func (r *NatsUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var natsUser natsv1.NatsUser
	if err := r.Get(ctx, req.NamespacedName, &natsUser); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	knownUserID := natsUser.Status.NatsUserID
	if natsUser.Spec.NatsUserID != "" {
		knownUserID = natsUser.Spec.NatsUserID
	}

	if !natsUser.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&natsUser, natsUserFinalizer) {

			// If we never had a user ID, this resource was never fully reconciled
			if natsUser.Status.NatsUserID == "" {
				if ok := controllerutil.RemoveFinalizer(&natsUser, natsUserFinalizer); !ok {
					return ctrl.Result{}, nil
				}
				if err := r.Update(ctx, &natsUser); err != nil {
					return requeueOnConflict(err)
				}
				return ctrl.Result{}, nil
			}

			in := controlplane.NatsUserInput{
				AccountSelectors: controlplane.AccountSelectors{
					AccountID:         natsUser.Spec.AccountID,
					AccountPublicNKey: natsUser.Spec.AccountPublicNKey,
					SystemID:          natsUser.Spec.SystemID,
				},
				NatsUserID: knownUserID,
				Name:       natsUser.Spec.Name,
			}

			if err := r.ControlPlane.DeleteNatsUser(ctx, in); err != nil {
				l.Error(err, "nats user delete failed")
				natsUser.Status.Message = err.Error()
				if err := r.Status().Update(ctx, &natsUser); err != nil {
					l.Error(err, "failed to update nats user status")
				}
				return requeueReconcileErr, nil
			}

			if ok := controllerutil.RemoveFinalizer(&natsUser, natsUserFinalizer); !ok {
				return ctrl.Result{}, nil
			}

			if err := r.Update(ctx, &natsUser); err != nil {
				return requeueOnConflict(err)
			}
		}

		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&natsUser, natsUserFinalizer) {
		if ok := controllerutil.AddFinalizer(&natsUser, natsUserFinalizer); !ok {
			return ctrl.Result{}, nil
		}

		if err := r.Update(ctx, &natsUser); err != nil {
			return requeueOnConflict(err)
		}

		return ctrl.Result{Requeue: true}, nil
	}

	if err := validateAccountSelectors(natsUser.Spec.AccountSelector); err != nil {
		natsUser.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &natsUser); err != nil {
			l.Error(err, "failed to update nats user status")
		}
		return ctrl.Result{}, nil
	}

	accountID, err := resolveAccountRef(ctx, r.Client, natsUser.Namespace, natsUser.Spec.AccountSelector)
	if err != nil {
		if errors.Is(err, errWaitingForAccount) {
			natsUser.Status.Message = err.Error()
			if err := r.Status().Update(ctx, &natsUser); err != nil {
				l.Error(err, "failed to update nats user status")
			}
			return requeueWaitingForResource, nil
		}

		l.Error(err, "failed to resolve account for nats user")
		natsUser.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &natsUser); err != nil {
			l.Error(err, "failed to update nats user status")
		}
		return requeueReconcileErr, nil
	}

	in := controlplane.NatsUserInput{
		AccountSelectors: controlplane.AccountSelectors{
			AccountID:         accountID,
			AccountPublicNKey: natsUser.Spec.AccountPublicNKey,
			SystemID:          natsUser.Spec.SystemID,
		},
		NatsUserID:        knownUserID,
		Name:              natsUser.Spec.Name,
		SigningKeyGroupID: natsUser.Spec.SigningKeyGroupID,
		Spec:              natsUser.Spec,
	}

	appliedState := loadAnnotation(&natsUser, appliedStateAnnotation)
	desiredState, err := json.Marshal(in)
	if err != nil {
		natsUser.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &natsUser); err != nil {
			l.Error(err, "failed to update nats user status")
		}
		return requeueReconcileErr, nil
	}

	specChanged := false
	if diff, err := diffState(appliedState, desiredState); err != nil {
		l.Error(err, "failed to diff nats user state")
		natsUser.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &natsUser); err != nil {
			l.Error(err, "failed to update nats user status")
		}
		return requeueReconcileErr, nil
	} else if diff != "" {
		logStateDiff(l, "natsUser", diff)
		specChanged = true
	}

	if !specChanged && natsUser.Status.NatsUserID != "" {
		serverState, found, err := r.ControlPlane.ReadNatsUserState(ctx, in)
		if err != nil {
			l.Error(err, "failed to read nats user server state")
			return requeueReconcileErr, nil
		}

		lastServerState := loadAnnotation(&natsUser, serverStateAnnotation)
		if found && lastServerState != nil {
			diff, err := diffState(serverState, lastServerState)
			if err != nil {
				l.Error(err, "failed to diff server state")
			} else if diff == "" {
				return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
			} else {
				l.Info("server-side drift detected for nats user. Reverting...\n" + diff)
			}
		} else if !found {
			l.Info("nats user not found on server, will re-create")
		}
	}

	out, err := r.ControlPlane.EnsureNatsUser(ctx, in)
	if err != nil {
		l.Error(err, "nats user update failed")
		natsUser.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &natsUser); err != nil {
			l.Error(err, "failed to update nats user status")
		}
		return requeueReconcileErr, nil
	}

	desiredStatus := natsUser.Status
	desiredStatus.NatsUserID = out.NatsUserID
	desiredStatus.AccountID = out.AccountID
	desiredStatus.UserPublicKey = out.UserPublicKey
	desiredStatus.Message = "applied"

	if desiredStatus != natsUser.Status {
		desiredStatus.LastSynced = time.Now().UTC().Format(time.RFC3339)
		natsUser.Status = desiredStatus
		if err := r.Status().Update(ctx, &natsUser); err != nil {
			return requeueOnConflict(err)
		}
	}

	readIn := in
	readIn.NatsUserID = out.NatsUserID
	newServerState, _, _ := r.ControlPlane.ReadNatsUserState(ctx, readIn)

	annotationsChanged := setAnnotations(&natsUser, appliedStateAnnotation, desiredState)
	if newServerState != nil {
		if setAnnotations(&natsUser, serverStateAnnotation, newServerState) {
			annotationsChanged = true
		}
	}
	if annotationsChanged {
		if err := r.Update(ctx, &natsUser); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
}

func (r *NatsUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1.NatsUser{}).
		Watches(&natsv1.Account{}, handler.EnqueueRequestsFromMapFunc(r.enqueueNatsUsersForAccount)).
		Complete(r)
}

func (r *NatsUserReconciler) enqueueNatsUsersForAccount(ctx context.Context, obj client.Object) []reconcile.Request {
	account, ok := obj.(*natsv1.Account)
	if !ok {
		return nil
	}

	var users natsv1.NatsUserList
	if err := r.List(ctx, &users, client.InNamespace(account.Namespace)); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for _, user := range users.Items {
		if user.Spec.AccountRef == nil {
			continue
		}
		if user.Spec.AccountRef.Name != account.Name {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: user.Namespace,
				Name:      user.Name,
			},
		})
	}
	return requests
}
