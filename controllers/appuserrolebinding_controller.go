package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// AppUserRoleBindingReconciler reconciles an AppUserRoleBinding object.
type AppUserRoleBindingReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ControlPlane    controlplane.Client
	RequeueInterval time.Duration
}

const appUserRoleBindingFinalizer = "synack.synadia.io/appuserrolebinding-finalizer"

// +kubebuilder:rbac:groups=synack.synadia.io,resources=appuserrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=synack.synadia.io,resources=appuserrolebindings/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=appuserrolebindings/finalizers,verbs=update
// +kubebuilder:rbac:groups=synack.synadia.io,resources=teamserviceaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=teams,verbs=get;list;watch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=accounts,verbs=get;list;watch

func (r *AppUserRoleBindingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var binding natsv1.AppUserRoleBinding
	if err := r.Get(ctx, req.NamespacedName, &binding); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !binding.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&binding, appUserRoleBindingFinalizer) {
			if !binding.Status.Bound {
				if ok := controllerutil.RemoveFinalizer(&binding, appUserRoleBindingFinalizer); !ok {
					return ctrl.Result{}, nil
				}
				if err := r.Update(ctx, &binding); err != nil {
					return requeueOnConflict(err)
				}
				return ctrl.Result{}, nil
			}

			teamAppUserID := binding.Status.TeamAppUserID
			if teamAppUserID == "" {
				teamAppUserID = binding.Spec.TeamAppUserID
			}
			targetID := binding.Status.TargetID
			if targetID == "" {
				targetID = binding.Spec.TargetID
			}

			if err := r.ControlPlane.DeleteAppUserRoleBinding(ctx, controlplane.AppUserRoleBindingInput{
				TeamAppUserID: teamAppUserID,
				Scope:         controlplane.RoleBindingScope(binding.Spec.Scope),
				TargetID:      targetID,
				RoleID:        binding.Spec.RoleID,
			}); err != nil {
				l.Error(err, "app user role binding delete failed")
				binding.Status.Message = err.Error()
				if err := r.Status().Update(ctx, &binding); err != nil {
					l.Error(err, "failed to update binding status")
				}
				return requeueReconcileErr, nil
			}

			if ok := controllerutil.RemoveFinalizer(&binding, appUserRoleBindingFinalizer); !ok {
				return ctrl.Result{}, nil
			}

			if err := r.Update(ctx, &binding); err != nil {
				return requeueOnConflict(err)
			}
		}

		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&binding, appUserRoleBindingFinalizer) {
		if ok := controllerutil.AddFinalizer(&binding, appUserRoleBindingFinalizer); !ok {
			return ctrl.Result{}, nil
		}

		if err := r.Update(ctx, &binding); err != nil {
			return requeueOnConflict(err)
		}

		return ctrl.Result{Requeue: true}, nil
	}

	if binding.Spec.SubjectRef == nil && binding.Spec.TeamAppUserID == "" {
		binding.Status.Message = "spec.subjectRef or spec.teamAppUserId is required"
		if err := r.Status().Update(ctx, &binding); err != nil {
			l.Error(err, "failed to update binding status")
		}
		return ctrl.Result{}, nil
	}
	if binding.Spec.SubjectRef != nil && binding.Spec.TeamAppUserID != "" {
		binding.Status.Message = "spec.subjectRef and spec.teamAppUserId are mutually exclusive"
		if err := r.Status().Update(ctx, &binding); err != nil {
			l.Error(err, "failed to update binding status")
		}
		return ctrl.Result{}, nil
	}

	if binding.Spec.TargetRef == nil && binding.Spec.TargetID == "" {
		binding.Status.Message = "spec.targetRef or spec.targetId is required"
		if err := r.Status().Update(ctx, &binding); err != nil {
			l.Error(err, "failed to update binding status")
		}
		return ctrl.Result{}, nil
	}
	if binding.Spec.TargetRef != nil && binding.Spec.TargetID != "" {
		binding.Status.Message = "spec.targetRef and spec.targetId are mutually exclusive"
		if err := r.Status().Update(ctx, &binding); err != nil {
			l.Error(err, "failed to update binding status")
		}
		return ctrl.Result{}, nil
	}

	// Resolve subject.
	teamAppUserID, err := r.resolveSubjectRef(ctx, &binding)
	if err != nil {
		if errors.Is(err, errWaitingForServiceAccount) {
			binding.Status.Message = err.Error()
			if err := r.Status().Update(ctx, &binding); err != nil {
				l.Error(err, "failed to update binding status")
			}
			return requeueWaitingForResource, nil
		}
		l.Error(err, "failed to resolve subject for role binding")
		binding.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &binding); err != nil {
			l.Error(err, "failed to update binding status")
		}
		return requeueReconcileErr, nil
	}

	// Resolve target.
	targetID, err := r.resolveTargetRef(ctx, &binding)
	if err != nil {
		if errors.Is(err, errWaitingForTeam) || errors.Is(err, errWaitingForAccount) {
			binding.Status.Message = err.Error()
			if err := r.Status().Update(ctx, &binding); err != nil {
				l.Error(err, "failed to update binding status")
			}
			return requeueWaitingForResource, nil
		}
		l.Error(err, "failed to resolve target for role binding")
		binding.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &binding); err != nil {
			l.Error(err, "failed to update binding status")
		}
		return requeueReconcileErr, nil
	}

	in := controlplane.AppUserRoleBindingInput{
		TeamAppUserID: teamAppUserID,
		Scope:         controlplane.RoleBindingScope(binding.Spec.Scope),
		TargetID:      targetID,
		RoleID:        binding.Spec.RoleID,
	}

	appliedState := loadAnnotation(&binding, appliedStateAnnotation)
	desiredState, err := json.Marshal(in)
	if err != nil {
		binding.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &binding); err != nil {
			l.Error(err, "failed to update binding status")
		}
		return requeueReconcileErr, nil
	}

	specChanged := false
	if diff, err := diffState(appliedState, desiredState); err != nil {
		l.Error(err, "failed to diff binding state")
		binding.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &binding); err != nil {
			l.Error(err, "failed to update binding status")
		}
		return requeueReconcileErr, nil
	} else if diff != "" {
		logStateDiff(l, "appUserRoleBinding", diff)
		specChanged = true
	}

	if !specChanged && binding.Status.Bound {
		serverState, found, err := r.ControlPlane.ReadAppUserRoleBindingState(ctx, in)
		if err != nil {
			l.Error(err, "failed to read binding server state")
			return requeueReconcileErr, nil
		}

		lastServerState := loadAnnotation(&binding, serverStateAnnotation)
		if found && lastServerState != nil {
			diff, err := diffState(serverState, lastServerState)
			if err != nil {
				l.Error(err, "failed to diff server state")
			} else if diff == "" {
				return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
			} else {
				l.Info("server-side drift detected for app user role binding. Reverting...\n" + diff)
			}
		} else if !found {
			l.Info("app user role binding not found on server, will re-create")
		}
	}

	out, err := r.ControlPlane.EnsureAppUserRoleBinding(ctx, in)
	if err != nil {
		l.Error(err, "app user role binding reconcile failed")
		binding.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &binding); err != nil {
			l.Error(err, "failed to update binding status")
		}
		return requeueReconcileErr, nil
	}

	desiredStatus := binding.Status
	desiredStatus.Bound = out.Bound
	desiredStatus.TeamAppUserID = teamAppUserID
	desiredStatus.TargetID = targetID
	desiredStatus.LastAppliedRoleID = binding.Spec.RoleID
	desiredStatus.Message = messageApplied

	if desiredStatus != binding.Status {
		desiredStatus.LastSynced = time.Now().UTC().Format(time.RFC3339)
		binding.Status = desiredStatus
		if err := r.Status().Update(ctx, &binding); err != nil {
			return requeueOnConflict(err)
		}
	}

	newServerState, _, _ := r.ControlPlane.ReadAppUserRoleBindingState(ctx, in)

	annotationsChanged := setAnnotations(&binding, appliedStateAnnotation, desiredState)
	if newServerState != nil {
		if setAnnotations(&binding, serverStateAnnotation, newServerState) {
			annotationsChanged = true
		}
	}
	if annotationsChanged {
		if err := r.Update(ctx, &binding); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
}

// resolveSubjectRef resolves the team app user ID from a TeamServiceAccount ref or direct ID.
func (r *AppUserRoleBindingReconciler) resolveSubjectRef(ctx context.Context, binding *natsv1.AppUserRoleBinding) (string, error) {
	if binding.Spec.SubjectRef == nil {
		return binding.Spec.TeamAppUserID, nil
	}

	var tsa natsv1.TeamServiceAccount
	key := types.NamespacedName{
		Namespace: binding.Namespace,
		Name:      binding.Spec.SubjectRef.Name,
	}

	if err := r.Get(ctx, key, &tsa); err != nil {
		if apierrors.IsNotFound(err) {
			return "", errors.Join(errWaitingForServiceAccount, fmt.Errorf("referenced TeamServiceAccount %q not found", binding.Spec.SubjectRef.Name))
		}
		return "", err
	}

	if tsa.Status.TeamAppUserID == "" {
		return "", errors.Join(errWaitingForServiceAccount, fmt.Errorf("referenced TeamServiceAccount %q is not ready (no teamAppUserId yet)", binding.Spec.SubjectRef.Name))
	}

	return tsa.Status.TeamAppUserID, nil
}

// resolveTargetRef resolves the target resource ID based on scope.
func (r *AppUserRoleBindingReconciler) resolveTargetRef(ctx context.Context, binding *natsv1.AppUserRoleBinding) (string, error) {
	if binding.Spec.TargetRef == nil {
		return binding.Spec.TargetID, nil
	}

	key := types.NamespacedName{
		Namespace: binding.Namespace,
		Name:      binding.Spec.TargetRef.Name,
	}

	switch binding.Spec.Scope {
	case natsv1.ScopeTeam:
		var team natsv1.Team
		if err := r.Get(ctx, key, &team); err != nil {
			if apierrors.IsNotFound(err) {
				return "", errors.Join(errWaitingForTeam, fmt.Errorf("referenced Team %q not found", binding.Spec.TargetRef.Name))
			}
			return "", err
		}
		if team.Status.ID == "" {
			return "", errors.Join(errWaitingForTeam, fmt.Errorf("referenced Team %q is not ready", binding.Spec.TargetRef.Name))
		}
		return team.Status.ID, nil

	case natsv1.ScopeAccount:
		var account natsv1.Account
		if err := r.Get(ctx, key, &account); err != nil {
			if apierrors.IsNotFound(err) {
				return "", errors.Join(errWaitingForAccount, fmt.Errorf("referenced Account %q not found", binding.Spec.TargetRef.Name))
			}
			return "", err
		}
		if account.Status.AccountID == "" {
			return "", errors.Join(errWaitingForAccount, fmt.Errorf("referenced Account %q is not ready", binding.Spec.TargetRef.Name))
		}
		return account.Status.AccountID, nil

	case natsv1.ScopeSystem, natsv1.ScopeNatsUser:
		return "", fmt.Errorf("targetRef is not supported for scope %s; use targetId instead", binding.Spec.Scope)

	default:
		return "", fmt.Errorf("unsupported scope: %s", binding.Spec.Scope)
	}
}

func (r *AppUserRoleBindingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1.AppUserRoleBinding{}).
		Watches(&natsv1.TeamServiceAccount{}, handler.EnqueueRequestsFromMapFunc(r.enqueueBindingsForServiceAccount)).
		Watches(&natsv1.Team{}, handler.EnqueueRequestsFromMapFunc(r.enqueueBindingsForTeam)).
		Watches(&natsv1.Account{}, handler.EnqueueRequestsFromMapFunc(r.enqueueBindingsForAccount)).
		Complete(r)
}

func (r *AppUserRoleBindingReconciler) enqueueBindingsForServiceAccount(ctx context.Context, obj client.Object) []reconcile.Request {
	tsa, ok := obj.(*natsv1.TeamServiceAccount)
	if !ok {
		return nil
	}

	var bindings natsv1.AppUserRoleBindingList
	if err := r.List(ctx, &bindings, client.InNamespace(tsa.Namespace)); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for _, b := range bindings.Items {
		if b.Spec.SubjectRef == nil {
			continue
		}
		if b.Spec.SubjectRef.Name != tsa.Name {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: b.Namespace,
				Name:      b.Name,
			},
		})
	}
	return requests
}

func (r *AppUserRoleBindingReconciler) enqueueBindingsForTeam(ctx context.Context, obj client.Object) []reconcile.Request {
	team, ok := obj.(*natsv1.Team)
	if !ok {
		return nil
	}

	var bindings natsv1.AppUserRoleBindingList
	if err := r.List(ctx, &bindings, client.InNamespace(team.Namespace)); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for _, b := range bindings.Items {
		if b.Spec.Scope != natsv1.ScopeTeam || b.Spec.TargetRef == nil {
			continue
		}
		if b.Spec.TargetRef.Name != team.Name {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: b.Namespace,
				Name:      b.Name,
			},
		})
	}
	return requests
}

func (r *AppUserRoleBindingReconciler) enqueueBindingsForAccount(ctx context.Context, obj client.Object) []reconcile.Request {
	account, ok := obj.(*natsv1.Account)
	if !ok {
		return nil
	}

	var bindings natsv1.AppUserRoleBindingList
	if err := r.List(ctx, &bindings, client.InNamespace(account.Namespace)); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for _, b := range bindings.Items {
		if b.Spec.Scope != natsv1.ScopeAccount || b.Spec.TargetRef == nil {
			continue
		}
		if b.Spec.TargetRef.Name != account.Name {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: b.Namespace,
				Name:      b.Name,
			},
		})
	}
	return requests
}
