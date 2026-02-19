package controllers

import (
	"context"
	"time"

	"github.com/synadia-io/control-plane-sdk-go/syncp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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

const streamFinalizer = "synack.synadia.io/stream-finalizer"

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

	if !stream.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&stream, streamFinalizer) {
			// If we never recorded a backend stream ID, this resource was never
			// successfully reconciled/applied, so finalize locally.
			if stream.Status.StreamID == "" {
				if ok := controllerutil.RemoveFinalizer(&stream, streamFinalizer); !ok {
					return ctrl.Result{}, nil
				}
				if err := r.Update(ctx, &stream); err != nil {
					return requeueOnConflict(err)
				}
				return ctrl.Result{}, nil
			}

			if err := r.ControlPlane.DeleteStream(ctx, controlplane.StreamInput{
				AccountID:         stream.Spec.AccountID,
				AccountPublicNKey: stream.Spec.AccountPublicNKey,
				SystemID:          stream.Spec.SystemID,
				Account:           stream.Spec.Account,
				StreamID:          knownStreamID,
				Name:              stream.Spec.Name,
			}); err != nil {
				l.Error(err, "control plane delete failed")
				stream.Status.Message = err.Error()
				_ = r.Status().Update(ctx, &stream)
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}

			if ok := controllerutil.RemoveFinalizer(&stream, streamFinalizer); !ok {
				return ctrl.Result{}, nil
			}
			if err := r.Update(ctx, &stream); err != nil {
				return requeueOnConflict(err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&stream, streamFinalizer) {
		if ok := controllerutil.AddFinalizer(&stream, streamFinalizer); !ok {
			return ctrl.Result{}, nil
		}
		if err := r.Update(ctx, &stream); err != nil {
			return requeueOnConflict(err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	out, err := r.ControlPlane.EnsureStream(ctx, controlplane.StreamInput{
		AccountID:         stream.Spec.AccountID,
		AccountPublicNKey: stream.Spec.AccountPublicNKey,
		SystemID:          stream.Spec.SystemID,
		Account:           stream.Spec.Account,
		StreamID:          knownStreamID,
		Name:              stream.Spec.Name,
		Subjects:          stream.Spec.Subjects,
		Description:       stream.Spec.Description,
		Retention:         stream.Spec.Retention,
		MaxConsumers:      stream.Spec.MaxConsumers,
		MaxMsgsPerSubject: stream.Spec.MaxMsgsPerSubject,
		MaxMsgs:           stream.Spec.MaxMsgs,
		MaxBytes:          stream.Spec.MaxBytes,
		MaxAge:            stream.Spec.MaxAge,
		MaxMsgSize:        stream.Spec.MaxMsgSize,
		Storage:           stream.Spec.Storage,
		Discard:           stream.Spec.Discard,
		Replicas:          stream.Spec.Replicas,
		NoAck:             stream.Spec.NoAck,
		DuplicateWindow:   stream.Spec.DuplicateWindow,
		Placement:         toControlPlanePlacement(stream.Spec.Placement),
		Sources:           toControlPlaneSources(stream.Spec.Sources),
		Compression:       stream.Spec.Compression,
		SubjectTransform:  toControlPlaneSubjectTransform(stream.Spec.SubjectTransform),
		RePublish:         toControlPlaneRePublish(stream.Spec.RePublish),
		Sealed:            stream.Spec.Sealed,
		DenyDelete:        stream.Spec.DenyDelete,
		DenyPurge:         stream.Spec.DenyPurge,
		AllowDirect:       stream.Spec.AllowDirect,
		AllowRollup:       stream.Spec.AllowRollup,
		DiscardPerSubject: stream.Spec.DiscardPerSubject,
		FirstSequence:     stream.Spec.FirstSequence,
		Metadata:          stream.Spec.Metadata,
	})
	if err != nil {
		l.Error(err, "control plane apply failed")
		stream.Status.Message = err.Error()
		_ = r.Status().Update(ctx, &stream)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	desiredStatus := stream.Status
	desiredStatus.ObservedGeneration = stream.Generation
	desiredStatus.StreamID = out.StreamID
	desiredStatus.Message = "applied"
	if desiredStatus != stream.Status {
		desiredStatus.LastSyncedAt = time.Now().UTC().Format(time.RFC3339)
		stream.Status = desiredStatus
		if err := r.Status().Update(ctx, &stream); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *StreamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1alpha1.Stream{}).
		Complete(r)
}

func toControlPlanePlacement(in *natsv1alpha1.StreamPlacement) *syncp.Placement {
	if in == nil {
		return nil
	}
	return &syncp.Placement{
		Cluster: in.Cluster,
		Tags:    in.Tags,
	}
}

func toControlPlaneSubjectTransform(in *natsv1alpha1.SubjectTransform) *syncp.SubjectTransformConfig {
	if in == nil {
		return nil
	}
	return &syncp.SubjectTransformConfig{
		Src:  in.Source,
		Dest: in.Dest,
	}
}

func toControlPlaneRePublish(in *natsv1alpha1.RePublish) *syncp.RePublish {
	if in == nil {
		return nil
	}
	src := in.Source
	headersOnly := in.HeadersOnly
	return &syncp.RePublish{
		Src:         &src,
		Dest:        in.Destination,
		HeadersOnly: &headersOnly,
	}
}

func toControlPlaneSources(in []natsv1alpha1.StreamSource) []syncp.StreamSource {
	if len(in) == 0 {
		return nil
	}

	out := make([]syncp.StreamSource, 0, len(in))
	for _, src := range in {
		cpSource := syncp.StreamSource{
			Name: src.Name,
		}

		if src.FilterSubject != "" {
			cpSource.FilterSubject = &src.FilterSubject
		}
		if src.OptStartSeq > 0 {
			optStartSeq := uint64(src.OptStartSeq)
			cpSource.OptStartSeq = &optStartSeq
		}
		if src.OptStartTime != "" {
			optStartTime := syncp.NewNullable(src.OptStartTime)
			cpSource.OptStartTime = &optStartTime
		}

		if src.ExternalAPIPrefix != "" || src.ExternalDeliverPrefix != "" {
			external := syncp.NewNullable(syncp.ExternalStream{
				Api:     src.ExternalAPIPrefix,
				Deliver: src.ExternalDeliverPrefix,
			})
			cpSource.External = &external
		}

		if len(src.SubjectTransforms) > 0 {
			cpSource.SubjectTransforms = make([]syncp.SubjectTransformConfig, 0, len(src.SubjectTransforms))
			for _, transform := range src.SubjectTransforms {
				cpSource.SubjectTransforms = append(cpSource.SubjectTransforms, syncp.SubjectTransformConfig{
					Src:  transform.Source,
					Dest: transform.Dest,
				})
			}
		}

		out = append(out, cpSource)
	}

	return out
}
