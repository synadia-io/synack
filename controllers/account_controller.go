package controllers

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

	out, err := r.ControlPlane.EnsureAccount(ctx, controlplane.AccountInput{
		SystemID: account.Spec.SystemID,
		Name:     account.Spec.Name,
	})
	if err != nil {
		l.Error(err, "control plane apply failed")
		account.Status.Message = err.Error()
		_ = r.Status().Update(ctx, &account)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	account.Status.ObservedGeneration = account.Generation
	account.Status.AccountID = out.AccountID
	account.Status.LastSyncedAt = time.Now().UTC().Format(time.RFC3339)
	account.Status.Message = "applied"
	if err := r.Status().Update(ctx, &account); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *AccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&synackv1alpha1.Account{}).
		Complete(r)
}
