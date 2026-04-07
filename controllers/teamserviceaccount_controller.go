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

// TeamServiceAccountReconciler reconciles a TeamServiceAccount object.
type TeamServiceAccountReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ControlPlane    controlplane.Client
	RequeueInterval time.Duration
}

const teamServiceAccountFinalizer = "synack.synadia.io/teamserviceaccount-finalizer"

var (
	errWaitingForTeam           = fmt.Errorf("waiting for referenced Team to be ready")
	errWaitingForServiceAccount = fmt.Errorf("waiting for referenced TeamServiceAccount to be ready")
)

// +kubebuilder:rbac:groups=synack.synadia.io,resources=teamserviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=synack.synadia.io,resources=teamserviceaccounts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=teamserviceaccounts/finalizers,verbs=update
// +kubebuilder:rbac:groups=synack.synadia.io,resources=teams,verbs=get;list;watch

func (r *TeamServiceAccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var tsa natsv1.TeamServiceAccount
	if err := r.Get(ctx, req.NamespacedName, &tsa); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	knownSAID := tsa.Status.ID
	if tsa.Spec.ServiceAccountID != "" {
		knownSAID = tsa.Spec.ServiceAccountID
	}

	if !tsa.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&tsa, teamServiceAccountFinalizer) {
			if tsa.Status.ID == "" {
				if ok := controllerutil.RemoveFinalizer(&tsa, teamServiceAccountFinalizer); !ok {
					return ctrl.Result{}, nil
				}
				if err := r.Update(ctx, &tsa); err != nil {
					return requeueOnConflict(err)
				}
				return ctrl.Result{}, nil
			}

			if err := r.ControlPlane.DeleteTeamServiceAccount(ctx, controlplane.TeamServiceAccountInput{
				ServiceAccountID: knownSAID,
				Name:             tsa.Spec.Name,
			}); err != nil {
				l.Error(err, "team service account delete failed")
				tsa.Status.Message = err.Error()
				if err := r.Status().Update(ctx, &tsa); err != nil {
					l.Error(err, "failed to update team service account status")
				}
				return requeueReconcileErr, nil
			}

			if ok := controllerutil.RemoveFinalizer(&tsa, teamServiceAccountFinalizer); !ok {
				return ctrl.Result{}, nil
			}

			if err := r.Update(ctx, &tsa); err != nil {
				return requeueOnConflict(err)
			}
		}

		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&tsa, teamServiceAccountFinalizer) {
		if ok := controllerutil.AddFinalizer(&tsa, teamServiceAccountFinalizer); !ok {
			return ctrl.Result{}, nil
		}

		if err := r.Update(ctx, &tsa); err != nil {
			return requeueOnConflict(err)
		}

		return ctrl.Result{Requeue: true}, nil
	}

	if tsa.Spec.TeamRef == nil && tsa.Spec.TeamID == "" {
		tsa.Status.Message = "spec.teamRef or spec.teamId is required"
		if err := r.Status().Update(ctx, &tsa); err != nil {
			l.Error(err, "failed to update team service account status")
		}
		return ctrl.Result{}, nil
	}
	if tsa.Spec.TeamRef != nil && tsa.Spec.TeamID != "" {
		tsa.Status.Message = "spec.teamRef and spec.teamId are mutually exclusive"
		if err := r.Status().Update(ctx, &tsa); err != nil {
			l.Error(err, "failed to update team service account status")
		}
		return ctrl.Result{}, nil
	}

	teamID, err := r.resolveTeamRef(ctx, &tsa)
	if err != nil {
		if errors.Is(err, errWaitingForTeam) {
			tsa.Status.Message = err.Error()
			if err := r.Status().Update(ctx, &tsa); err != nil {
				l.Error(err, "failed to update team service account status")
			}
			return requeueWaitingForResource, nil
		}

		l.Error(err, "failed to resolve team for service account")
		tsa.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &tsa); err != nil {
			l.Error(err, "failed to update team service account status")
		}
		return requeueReconcileErr, nil
	}

	in := controlplane.TeamServiceAccountInput{
		ServiceAccountID: knownSAID,
		TeamID:           teamID,
		Name:             tsa.Spec.Name,
		TeamRoleID:       tsa.Spec.TeamRoleID,
	}

	appliedState := loadAnnotation(&tsa, appliedStateAnnotation)
	desiredState, err := json.Marshal(in)
	if err != nil {
		tsa.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &tsa); err != nil {
			l.Error(err, "failed to update team service account status")
		}
		return requeueReconcileErr, nil
	}

	specChanged := false
	if diff, err := diffState(appliedState, desiredState); err != nil {
		l.Error(err, "failed to diff team service account state")
		tsa.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &tsa); err != nil {
			l.Error(err, "failed to update team service account status")
		}
		return requeueReconcileErr, nil
	} else if diff != "" {
		logStateDiff(l, "teamServiceAccount", diff)
		specChanged = true
	}

	if !specChanged && tsa.Status.ID != "" {
		serverState, found, err := r.ControlPlane.ReadTeamServiceAccountState(ctx, in)
		if err != nil {
			l.Error(err, "failed to read team service account server state")
			return requeueReconcileErr, nil
		}

		lastServerState := loadAnnotation(&tsa, serverStateAnnotation)
		if found && lastServerState != nil {
			diff, err := diffState(serverState, lastServerState)
			if err != nil {
				l.Error(err, "failed to diff server state")
			} else if diff == "" {
				return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
			} else {
				l.Info("server-side drift detected for team service account. Reverting...\n" + diff)
			}
		} else if !found {
			l.Info("team service account not found on server, will re-create")
		}
	}

	out, err := r.ControlPlane.EnsureTeamServiceAccount(ctx, in)
	if err != nil {
		l.Error(err, "team service account reconcile failed")
		tsa.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &tsa); err != nil {
			l.Error(err, "failed to update team service account status")
		}
		return requeueReconcileErr, nil
	}

	desiredStatus := tsa.Status
	desiredStatus.ID = out.ServiceAccountID
	desiredStatus.TeamID = teamID
	desiredStatus.Message = messageApplied

	if desiredStatus != tsa.Status {
		desiredStatus.LastSynced = time.Now().UTC().Format(time.RFC3339)
		tsa.Status = desiredStatus
		if err := r.Status().Update(ctx, &tsa); err != nil {
			return requeueOnConflict(err)
		}
	}

	readIn := in
	readIn.ServiceAccountID = out.ServiceAccountID
	newServerState, _, _ := r.ControlPlane.ReadTeamServiceAccountState(ctx, readIn)

	annotationsChanged := setAnnotations(&tsa, appliedStateAnnotation, desiredState)
	if newServerState != nil {
		if setAnnotations(&tsa, serverStateAnnotation, newServerState) {
			annotationsChanged = true
		}
	}
	if annotationsChanged {
		if err := r.Update(ctx, &tsa); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
}

func (r *TeamServiceAccountReconciler) resolveTeamRef(ctx context.Context, tsa *natsv1.TeamServiceAccount) (string, error) {
	if tsa.Spec.TeamRef == nil {
		return tsa.Spec.TeamID, nil
	}

	var team natsv1.Team
	key := types.NamespacedName{
		Namespace: tsa.Namespace,
		Name:      tsa.Spec.TeamRef.Name,
	}

	if err := r.Get(ctx, key, &team); err != nil {
		if apierrors.IsNotFound(err) {
			return "", errors.Join(errWaitingForTeam, fmt.Errorf("referenced Team %q not found", tsa.Spec.TeamRef.Name))
		}
		return "", err
	}

	if team.Status.ID == "" {
		return "", errors.Join(errWaitingForTeam, fmt.Errorf("referenced Team %q is not ready", tsa.Spec.TeamRef.Name))
	}

	return team.Status.ID, nil
}

func (r *TeamServiceAccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1.TeamServiceAccount{}).
		Watches(&natsv1.Team{}, handler.EnqueueRequestsFromMapFunc(r.enqueueServiceAccountsForTeam)).
		Complete(r)
}

func (r *TeamServiceAccountReconciler) enqueueServiceAccountsForTeam(ctx context.Context, obj client.Object) []reconcile.Request {
	team, ok := obj.(*natsv1.Team)
	if !ok {
		return nil
	}

	var tsas natsv1.TeamServiceAccountList
	if err := r.List(ctx, &tsas, client.InNamespace(team.Namespace)); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for _, sa := range tsas.Items {
		if sa.Spec.TeamRef == nil {
			continue
		}
		if sa.Spec.TeamRef.Name != team.Name {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: sa.Namespace,
				Name:      sa.Name,
			},
		})
	}
	return requests
}
