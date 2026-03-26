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

// TeamReconciler reconciles a Team object.
type TeamReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ControlPlane    controlplane.Client
	RequeueInterval time.Duration
}

const teamFinalizer = "synack.synadia.io/team-finalizer"

// +kubebuilder:rbac:groups=synack.synadia.io,resources=teams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=synack.synadia.io,resources=teams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=teams/finalizers,verbs=update

func (r *TeamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var team natsv1.Team
	if err := r.Get(ctx, req.NamespacedName, &team); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	knownTeamID := team.Status.ID
	if team.Spec.TeamID != "" {
		knownTeamID = team.Spec.TeamID
	}

	if !team.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&team, teamFinalizer) {
			// If we never had a team ID, this resource was never fully reconciled.
			if team.Status.ID == "" {
				if ok := controllerutil.RemoveFinalizer(&team, teamFinalizer); !ok {
					return ctrl.Result{}, nil
				}
				if err := r.Update(ctx, &team); err != nil {
					return requeueOnConflict(err)
				}
				return ctrl.Result{}, nil
			}

			if err := r.ControlPlane.DeleteTeam(ctx, controlplane.TeamInput{
				TeamID: knownTeamID,
				Name:   team.Spec.Name,
			}); err != nil {
				l.Error(err, "team delete failed")
				team.Status.Message = err.Error()
				if err := r.Status().Update(ctx, &team); err != nil {
					l.Error(err, "failed to update team status")
				}
				return requeueReconcileErr, nil
			}

			if ok := controllerutil.RemoveFinalizer(&team, teamFinalizer); !ok {
				return ctrl.Result{}, nil
			}

			if err := r.Update(ctx, &team); err != nil {
				return requeueOnConflict(err)
			}
		}

		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&team, teamFinalizer) {
		if ok := controllerutil.AddFinalizer(&team, teamFinalizer); !ok {
			return ctrl.Result{}, nil
		}

		if err := r.Update(ctx, &team); err != nil {
			return requeueOnConflict(err)
		}

		return ctrl.Result{Requeue: true}, nil
	}

	in := controlplane.TeamInput{
		TeamID: knownTeamID,
		Name:   team.Spec.Name,
	}

	appliedState := loadAnnotation(&team, appliedStateAnnotation)
	desiredState, err := json.Marshal(in)
	if err != nil {
		team.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &team); err != nil {
			l.Error(err, "failed to update team status")
		}
		return requeueReconcileErr, nil
	}

	specChanged := false
	if diff, err := diffState(appliedState, desiredState); err != nil {
		l.Error(err, "failed to diff team state")
		team.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &team); err != nil {
			l.Error(err, "failed to update team status")
		}
		return requeueReconcileErr, nil
	} else if diff != "" {
		logStateDiff(l, "team", diff)
		specChanged = true
	}

	if !specChanged && team.Status.ID != "" {
		serverState, found, err := r.ControlPlane.ReadTeamState(ctx, in)
		if err != nil {
			l.Error(err, "failed to read team server state")
			return requeueReconcileErr, nil
		}

		lastServerState := loadAnnotation(&team, serverStateAnnotation)
		if found && lastServerState != nil {
			diff, err := diffState(serverState, lastServerState)
			if err != nil {
				l.Error(err, "failed to diff server state")
			} else if diff == "" {
				return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
			} else {
				l.Info("server-side drift detected for team. Reverting...\n" + diff)
			}
		} else if !found {
			l.Info("team not found on server, will re-create")
		}
	}

	out, err := r.ControlPlane.EnsureTeam(ctx, in)
	if err != nil {
		l.Error(err, "team reconcile failed")
		team.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &team); err != nil {
			l.Error(err, "failed to update team status")
		}
		return requeueReconcileErr, nil
	}

	desiredStatus := team.Status
	desiredStatus.ID = out.TeamID
	desiredStatus.ObservedName = team.Spec.Name
	desiredStatus.Message = messageApplied

	if desiredStatus != team.Status {
		desiredStatus.LastSynced = time.Now().UTC().Format(time.RFC3339)
		team.Status = desiredStatus
		if err := r.Status().Update(ctx, &team); err != nil {
			return requeueOnConflict(err)
		}
	}

	readIn := in
	readIn.TeamID = out.TeamID
	newServerState, _, _ := r.ControlPlane.ReadTeamState(ctx, readIn)

	annotationsChanged := setAnnotations(&team, appliedStateAnnotation, desiredState)
	if newServerState != nil {
		if setAnnotations(&team, serverStateAnnotation, newServerState) {
			annotationsChanged = true
		}
	}
	if annotationsChanged {
		if err := r.Update(ctx, &team); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
}

func (r *TeamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1.Team{}).
		Complete(r)
}
