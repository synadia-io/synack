package controllers

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	natsv1alpha1 "github.com/synadia-io/synack/api/v1alpha1"
	"github.com/synadia-io/synack/internal/controlplane"
)

// StreamReconciler reconciles a Stream object.
type StreamReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	ControlPlane controlplane.Client
}

// +kubebuilder:rbac:groups=synack.synadia.io,resources=streams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=synack.synadia.io,resources=streams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=streams/finalizers,verbs=update

func (r *StreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var stream natsv1alpha1.Stream
	if err := r.Get(ctx, req.NamespacedName, &stream); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	knownStreamID := stream.Status.StreamID
	if stream.Spec.StreamID != "" {
		knownStreamID = stream.Spec.StreamID
	}

	out, err := r.ControlPlane.EnsureStream(ctx, controlplane.StreamInput{
		AccountID:         stream.Spec.AccountID,
		AccountPublicNKey: stream.Spec.AccountPublicNKey,
		SystemID:          stream.Spec.SystemID,
		Account:           stream.Spec.Account,
		StreamID:          knownStreamID,
		Name:              stream.Spec.Name,
		Subjects:          stream.Spec.Subjects,
	})
	if err != nil {
		l.Error(err, "control plane apply failed")
		stream.Status.Message = err.Error()
		_ = r.Status().Update(ctx, &stream)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	stream.Status.ObservedGeneration = stream.Generation
	stream.Status.StreamID = out.StreamID
	stream.Status.LastSyncedAt = time.Now().UTC().Format(time.RFC3339)
	stream.Status.Message = "applied"
	if err := r.Status().Update(ctx, &stream); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *StreamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1alpha1.Stream{}).
		Complete(r)
}
