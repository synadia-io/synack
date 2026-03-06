package controllers

import (
	"context"
	"encoding/json"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	natsv1 "github.com/synadia-io/synack/api/v1alpha1"
	"github.com/synadia-io/synack/internal/controlplane"
)

// AccountReconciler reconciles an Account object.
type AccountReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ControlPlane    controlplane.Client
	RequeueInterval time.Duration
}

const accountFinalizer = "synack.synadia.io/account-finalizer"

// +kubebuilder:rbac:groups=synack.synadia.io,resources=accounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=synack.synadia.io,resources=accounts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=accounts/finalizers,verbs=update

func (r *AccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var account natsv1.Account
	if err := r.Get(ctx, req.NamespacedName, &account); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !account.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&account, accountFinalizer) {

			// Block deletion until all dependent resources are removed
			if err := checkAccountDependents(ctx, r.Client, account.Namespace, account.Name); err != nil {
				l.Info(err.Error())
				account.Status.Message = err.Error()
				if err := r.Status().Update(ctx, &account); err != nil {
					l.Error(err, "failed to update account status")
				}
				return requeueWaitingForResource, nil
			}

			// If we never had an account ID, this resource was never fully reconciled
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
				l.Error(err, "account delete failed")
				account.Status.Message = err.Error()
				if err := r.Status().Update(ctx, &account); err != nil {
					l.Error(err, "failed to update account status")
				}
				return requeueReconcileErr, nil
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

	in := controlplane.AccountInput{
		AccountID: account.Status.AccountID,
		SystemID:  account.Spec.SystemID,
		Name:      account.Spec.Name,
	}

	appliedState := loadAnnotation(&account, accountAppliedStateAnnotation)
	desiredState, err := json.Marshal(in)
	if err != nil {
		account.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &account); err != nil {
			l.Error(err, "failed to update account status")
		}
		return requeueReconcileErr, nil
	}

	specChanged := false
	if diff, err := diffState(appliedState, desiredState); err != nil {
		l.Error(err, "failed to diff account state")
		account.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &account); err != nil {
			l.Error(err, "failed to update account status")
		}
		return requeueReconcileErr, nil
	} else if diff != "" {
		logStateDiff(l, "account", diff)
		specChanged = true
	}

	if !specChanged && account.Status.AccountID != "" {
		serverState, found, err := r.ControlPlane.ReadAccountState(ctx, in)
		if err != nil {
			l.Error(err, "failed to read account server state")
			return requeueReconcileErr, nil
		}

		lastServerState := loadAnnotation(&account, accountServerStateAnnotation)
		if found && lastServerState != nil {
			diff, err := diffState(serverState, lastServerState)
			if err != nil {
				l.Error(err, "failed to diff server state")
			} else if diff == "" {
				return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
			} else {
				l.Info("server-side drift detected for account. Reverting...\n" + diff)
			}
		} else if !found {
			l.Info("account not found on server, will re-create")
		}
	}

	out, err := r.ControlPlane.EnsureAccount(ctx, in)
	if err != nil {
		l.Error(err, "account update failed")
		account.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &account); err != nil {
			l.Error(err, "failed to update account status")
		}
		return requeueReconcileErr, nil
	}

	desiredStatus := account.Status
	desiredStatus.AccountID = out.AccountID
	desiredStatus.Message = "applied"

	if desiredStatus != account.Status {
		desiredStatus.LastSynced = time.Now().UTC().Format(time.RFC3339)
		account.Status = desiredStatus
		if err := r.Status().Update(ctx, &account); err != nil {
			return requeueOnConflict(err)
		}
	}

	readIn := in
	readIn.AccountID = out.AccountID
	newServerState, _, _ := r.ControlPlane.ReadAccountState(ctx, readIn)

	annotationsChanged := setAnnotations(&account, accountAppliedStateAnnotation, desiredState)
	if newServerState != nil {
		if setAnnotations(&account, accountServerStateAnnotation, newServerState) {
			annotationsChanged = true
		}
	}
	if annotationsChanged {
		if err := r.Update(ctx, &account); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
}

func (r *AccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1.Account{}).
		Complete(r)
}
