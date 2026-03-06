package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/synadia-io/control-plane-sdk-go/syncp"
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
// +kubebuilder:rbac:groups=synack.synadia.io,resources=accounts,verbs=get;list;watch

func (r *StreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var stream natsv1.Stream
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

			// Block deletion until all dependent consumers are removed
			hasDeps, err := hasDependentConsumers(ctx, r.Client, stream.Namespace, stream.Name)
			if err != nil {
				l.Error(err, "failed to check dependent consumers")
				return requeueReconcileErr, nil
			}

			if hasDeps {
				msg := fmt.Sprintf("waiting for dependent Consumers to be deleted before removing Stream %q", stream.Name)
				l.Info(msg)
				stream.Status.Message = msg
				if err := r.Status().Update(ctx, &stream); err != nil {
					l.Error(err, "failed to update stream status")
				}
				return requeueWaitingForResource, nil
			}

			// If we never had a stream ID, this resource was never fully reconciled
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
				AccountSelectors: controlplane.AccountSelectors{
					AccountID:         stream.Spec.AccountID,
					AccountPublicNKey: stream.Spec.AccountPublicNKey,
					SystemID:          stream.Spec.SystemID,
				},
				StreamID: knownStreamID,
				Name:     stream.Spec.Name,
			}); err != nil {
				l.Error(err, "stream delete failed")
				stream.Status.Message = err.Error()
				if err := r.Status().Update(ctx, &stream); err != nil {
					l.Error(err, "failed to update stream status")
				}
				return requeueReconcileErr, nil
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

	if err := validateAccountSelectors(stream.Spec.AccountSelector); err != nil {
		stream.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &stream); err != nil {
			l.Error(err, "failed to update stream status")
		}
		return ctrl.Result{}, nil
	}

	accountID, err := resolveAccountRef(ctx, r.Client, stream.Namespace, stream.Spec.AccountSelector)
	if err != nil {
		if errors.Is(err, errWaitingForAccount) {
			stream.Status.Message = err.Error()
			if err := r.Status().Update(ctx, &stream); err != nil {
				l.Error(err, "failed to update stream status")
			}
			return requeueWaitingForResource, nil
		}

		l.Error(err, "failed to resolve account for stream")
		stream.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &stream); err != nil {
			l.Error(err, "failed to update stream status")
		}
		return requeueReconcileErr, nil
	}

	in := controlplane.StreamInput{
		AccountSelectors: controlplane.AccountSelectors{
			AccountID:         accountID,
			AccountPublicNKey: stream.Spec.AccountPublicNKey,
			SystemID:          stream.Spec.SystemID,
		},
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
		Placement:         toSCPPlacement(stream.Spec.Placement),
		Sources:           toSCPStreamSources(stream.Spec.Sources),
		Compression:       stream.Spec.Compression,
		SubjectTransform:  toSCPSubjectTransform(stream.Spec.SubjectTransform),
		RePublish:         toSCPRePublish(stream.Spec.RePublish),
		Sealed:            stream.Spec.Sealed,
		DenyDelete:        stream.Spec.DenyDelete,
		DenyPurge:         stream.Spec.DenyPurge,
		AllowDirect:       stream.Spec.AllowDirect,
		AllowRollup:       stream.Spec.AllowRollup,
		DiscardPerSubject: stream.Spec.DiscardPerSubject,
		FirstSequence:     stream.Spec.FirstSequence,
		Metadata:          stream.Spec.Metadata,
	}

	appliedState := loadAnnotation(&stream, streamAppliedStateAnnotation)
	desiredState, err := json.Marshal(in)
	if err != nil {
		stream.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &stream); err != nil {
			l.Error(err, "failed to update stream status")
		}
		return requeueReconcileErr, nil
	}

	if diff, err := diffState(appliedState, desiredState); err != nil {
		l.Error(err, "failed to diff stream state")
		stream.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &stream); err != nil {
			l.Error(err, "failed to update stream status")
		}
		return requeueReconcileErr, nil
	} else if diff != "" {
		logStateDiff(l, "stream", diff)
	}

	out, err := r.ControlPlane.EnsureStream(ctx, in)
	if err != nil {
		l.Error(err, "stream update failed")
		stream.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &stream); err != nil {
			l.Error(err, "failed to update stream status")
		}
		return requeueReconcileErr, nil
	}

	desiredStatus := stream.Status
	desiredStatus.StreamID = out.StreamID
	desiredStatus.Message = "applied"

	if desiredStatus != stream.Status {
		desiredStatus.LastSynced = time.Now().UTC().Format(time.RFC3339)
		stream.Status = desiredStatus
		if err := r.Status().Update(ctx, &stream); err != nil {
			return requeueOnConflict(err)
		}
	}

	if setAnnotations(&stream, streamAppliedStateAnnotation, desiredState) {
		if err := r.Update(ctx, &stream); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *StreamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1.Stream{}).
		Watches(&natsv1.Account{}, handler.EnqueueRequestsFromMapFunc(r.enqueueStreamsForAccount)).
		Complete(r)
}

func (r *StreamReconciler) enqueueStreamsForAccount(ctx context.Context, obj client.Object) []reconcile.Request {
	account, ok := obj.(*natsv1.Account)
	if !ok {
		return nil
	}

	var streams natsv1.StreamList
	if err := r.List(ctx, &streams, client.InNamespace(account.Namespace)); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for _, stream := range streams.Items {
		if stream.Spec.AccountRef == nil {
			continue
		}
		if stream.Spec.AccountRef.Name != account.Name {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: stream.Namespace,
				Name:      stream.Name,
			},
		})
	}
	return requests
}

func toSCPPlacement(in *natsv1.Placement) *syncp.Placement {
	if in == nil {
		return nil
	}

	return &syncp.Placement{
		Cluster: in.Cluster,
		Tags:    in.Tags,
	}
}

func toSCPSubjectTransform(in *natsv1.SubjectTransform) *syncp.SubjectTransformConfig {
	if in == nil {
		return nil
	}

	return &syncp.SubjectTransformConfig{
		Src:  in.Source,
		Dest: in.Dest,
	}
}

func toSCPRePublish(in *natsv1.RePublish) *syncp.RePublish {
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

func toSCPStreamSourcePtr(in *natsv1.StreamSource) *syncp.StreamSource {
	if in == nil {
		return nil
	}

	sources := toSCPStreamSources([]natsv1.StreamSource{*in})
	if len(sources) == 0 {
		return nil
	}

	return &sources[0]
}

func toSCPStreamSources(in []natsv1.StreamSource) []syncp.StreamSource {
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
