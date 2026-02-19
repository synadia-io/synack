package controllers

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	synackv1alpha1 "github.com/synadia-io/synack/api/v1alpha1"
	"github.com/synadia-io/synack/internal/controlplane"
)

// AccountReconciler reconciles an Account object.
type AccountReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	ControlPlane controlplane.Client
}

const accountFinalizer = "synack.synadia.io/account-finalizer"

// +kubebuilder:rbac:groups=synack.synadia.io,resources=accounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=synack.synadia.io,resources=accounts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=accounts/finalizers,verbs=update

func (r *AccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var account synackv1alpha1.Account
	if err := r.Get(ctx, req.NamespacedName, &account); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !account.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&account, accountFinalizer) {
			// If we never recorded a backend ID, this resource was never successfully
			// reconciled/applied, so finalize locally without remote delete.
			if account.Status.AccountID == "" {
				if ok := controllerutil.RemoveFinalizer(&account, accountFinalizer); !ok {
					return ctrl.Result{}, nil
				}
				if err := r.Update(ctx, &account); err != nil {
					return requeueOnConflict(err)
				}
				return ctrl.Result{}, nil
			}

			if err := r.ControlPlane.DeleteAccount(ctx, controlplane.AccountInput{
				AccountID: account.Status.AccountID,
				SystemID:  account.Spec.SystemID,
				Name:      account.Spec.Name,
			}); err != nil {
				l.Error(err, "control plane delete failed")
				account.Status.Message = err.Error()
				_ = r.Status().Update(ctx, &account)
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}

			if ok := controllerutil.RemoveFinalizer(&account, accountFinalizer); !ok {
				return ctrl.Result{}, nil
			}
			if err := r.Update(ctx, &account); err != nil {
				return requeueOnConflict(err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&account, accountFinalizer) {
		if ok := controllerutil.AddFinalizer(&account, accountFinalizer); !ok {
			return ctrl.Result{}, nil
		}
		if err := r.Update(ctx, &account); err != nil {
			return requeueOnConflict(err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	out, err := r.ControlPlane.EnsureAccount(ctx, controlplane.AccountInput{
		AccountID: account.Status.AccountID,
		SystemID:  account.Spec.SystemID,
		Name:      account.Spec.Name,
	})
	if err != nil {
		l.Error(err, "control plane apply failed")
		account.Status.Message = err.Error()
		_ = r.Status().Update(ctx, &account)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	desiredStatus := account.Status
	desiredStatus.ObservedGeneration = account.Generation
	desiredStatus.AccountID = out.AccountID
	desiredStatus.Message = "applied"
	if desiredStatus != account.Status {
		desiredStatus.LastSyncedAt = time.Now().UTC().Format(time.RFC3339)
		account.Status = desiredStatus
		if err := r.Status().Update(ctx, &account); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *AccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&synackv1alpha1.Account{}).
		Complete(r)
}
