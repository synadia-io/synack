package controllers

import (
	"context"
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

	natsv1alpha1 "github.com/synadia-io/synack/api/v1alpha1"
	"github.com/synadia-io/synack/internal/controlplane"
)

// ConsumerReconciler reconciles a Consumer object.
type ConsumerReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	ControlPlane controlplane.Client
}

const consumerFinalizer = "synack.synadia.io/consumer-finalizer"

// +kubebuilder:rbac:groups=synack.synadia.io,resources=consumers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=synack.synadia.io,resources=consumers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=synack.synadia.io,resources=consumers/finalizers,verbs=update
// +kubebuilder:rbac:groups=synack.synadia.io,resources=streams,verbs=get;list;watch

func (r *ConsumerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var consumer natsv1alpha1.Consumer
	if err := r.Get(ctx, req.NamespacedName, &consumer); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	knownConsumerID := consumer.Status.ConsumerID
	if consumer.Spec.ConsumerID != "" {
		knownConsumerID = consumer.Spec.ConsumerID
	}

	if !consumer.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&consumer, consumerFinalizer) {
			if consumer.Status.ConsumerID == "" {
				if ok := controllerutil.RemoveFinalizer(&consumer, consumerFinalizer); !ok {
					return ctrl.Result{}, nil
				}
				if err := r.Update(ctx, &consumer); err != nil {
					return requeueOnConflict(err)
				}
				return ctrl.Result{}, nil
			}

			if err := r.ControlPlane.DeleteConsumer(ctx, controlplane.ConsumerInput{
				StreamID:   consumer.Status.StreamID,
				ConsumerID: knownConsumerID,
				IsPush:     consumer.Status.IsPush,
				Name:       consumer.Spec.Name,
			}); err != nil {
				l.Error(err, "control plane delete failed")
				consumer.Status.Message = err.Error()
				_ = r.Status().Update(ctx, &consumer)
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}

			if ok := controllerutil.RemoveFinalizer(&consumer, consumerFinalizer); !ok {
				return ctrl.Result{}, nil
			}
			if err := r.Update(ctx, &consumer); err != nil {
				return requeueOnConflict(err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&consumer, consumerFinalizer) {
		if ok := controllerutil.AddFinalizer(&consumer, consumerFinalizer); !ok {
			return ctrl.Result{}, nil
		}
		if err := r.Update(ctx, &consumer); err != nil {
			return requeueOnConflict(err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if err := validateStreamSelectors(consumer.Spec); err != nil {
		consumer.Status.Message = err.Error()
		_ = r.Status().Update(ctx, &consumer)
		return ctrl.Result{}, nil
	}

	streamID, waitingForStream, err := r.resolveStreamID(ctx, &consumer)
	if err != nil {
		if waitingForStream {
			consumer.Status.Message = err.Error()
			_ = r.Status().Update(ctx, &consumer)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		l.Error(err, "failed to resolve stream for consumer")
		consumer.Status.Message = err.Error()
		_ = r.Status().Update(ctx, &consumer)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	out, err := r.ControlPlane.EnsureConsumer(ctx, controlplane.ConsumerInput{
		StreamID:          streamID,
		ConsumerID:        knownConsumerID,
		Name:              consumer.Spec.Name,
		Description:       consumer.Spec.Description,
		AckPolicy:         consumer.Spec.AckPolicy,
		AckWait:           consumer.Spec.AckWait,
		DeliverPolicy:     consumer.Spec.DeliverPolicy,
		DurableName:       consumer.Spec.DurableName,
		FilterSubjects:    consumer.Spec.FilterSubjects,
		InactiveThreshold: consumer.Spec.InactiveThreshold,
		MaxAckPending:     consumer.Spec.MaxAckPending,
		MaxDeliver:        consumer.Spec.MaxDeliver,
		MemStorage:        consumer.Spec.MemStorage,
		Replicas:          consumer.Spec.Replicas,
		OptStartSeq:       consumer.Spec.OptStartSeq,
		OptStartTime:      consumer.Spec.OptStartTime,
		ReplayPolicy:      consumer.Spec.ReplayPolicy,
		SampleFreq:        consumer.Spec.SampleFreq,
		Backoff:           consumer.Spec.Backoff,
		Direct:            consumer.Spec.Direct,
		Metadata:          consumer.Spec.Metadata,

		MaxRequestBatch:    consumer.Spec.MaxRequestBatch,
		MaxRequestMaxBytes: consumer.Spec.MaxRequestMaxBytes,
		MaxRequestExpires:  consumer.Spec.MaxRequestExpires,
		MaxWaiting:         consumer.Spec.MaxWaiting,

		DeliverSubject:    consumer.Spec.DeliverSubject,
		DeliverGroup:      consumer.Spec.DeliverGroup,
		FlowControl:       consumer.Spec.FlowControl,
		HeadersOnly:       consumer.Spec.HeadersOnly,
		HeartbeatInterval: consumer.Spec.HeartbeatInterval,
		RateLimitBps:      consumer.Spec.RateLimitBps,
	})
	if err != nil {
		l.Error(err, "control plane apply failed")
		consumer.Status.Message = err.Error()
		_ = r.Status().Update(ctx, &consumer)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	desiredStatus := consumer.Status
	desiredStatus.ObservedGeneration = consumer.Generation
	desiredStatus.ConsumerID = out.ConsumerID
	desiredStatus.StreamID = out.StreamID
	desiredStatus.IsPush = out.IsPush
	desiredStatus.Message = "applied"
	if desiredStatus != consumer.Status {
		desiredStatus.LastSyncedAt = time.Now().UTC().Format(time.RFC3339)
		consumer.Status = desiredStatus
		if err := r.Status().Update(ctx, &consumer); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *ConsumerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1alpha1.Consumer{}).
		Watches(&natsv1alpha1.Stream{}, handler.EnqueueRequestsFromMapFunc(r.enqueueConsumersForStream)).
		Complete(r)
}

func validateStreamSelectors(spec natsv1alpha1.ConsumerSpec) error {
	if spec.StreamRef != nil && spec.StreamID != "" {
		return errors.New("spec.streamRef and spec.streamId are mutually exclusive")
	}
	if spec.StreamRef == nil && spec.StreamID == "" {
		return errors.New("one of spec.streamRef or spec.streamId is required")
	}
	if spec.StreamRef != nil && spec.StreamRef.Name == "" {
		return errors.New("spec.streamRef.name is required when streamRef is set")
	}
	return nil
}

func (r *ConsumerReconciler) resolveStreamID(ctx context.Context, consumer *natsv1alpha1.Consumer) (string, bool, error) {
	if consumer.Spec.StreamRef == nil {
		return consumer.Spec.StreamID, false, nil
	}

	var stream natsv1alpha1.Stream
	key := types.NamespacedName{
		Namespace: consumer.Namespace,
		Name:      consumer.Spec.StreamRef.Name,
	}
	if err := r.Get(ctx, key, &stream); err != nil {
		if apierrors.IsNotFound(err) {
			return "", true, fmt.Errorf("waiting for referenced Stream %q to be created", consumer.Spec.StreamRef.Name)
		}
		return "", false, err
	}

	if stream.Status.StreamID == "" {
		return "", true, fmt.Errorf("waiting for referenced Stream %q to reconcile status.streamId", consumer.Spec.StreamRef.Name)
	}

	return stream.Status.StreamID, false, nil
}

func (r *ConsumerReconciler) enqueueConsumersForStream(ctx context.Context, obj client.Object) []reconcile.Request {
	stream, ok := obj.(*natsv1alpha1.Stream)
	if !ok {
		return nil
	}

	var consumers natsv1alpha1.ConsumerList
	if err := r.List(ctx, &consumers, client.InNamespace(stream.Namespace)); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for _, consumer := range consumers.Items {
		if consumer.Spec.StreamRef == nil {
			continue
		}
		if consumer.Spec.StreamRef.Name != stream.Name {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: consumer.Namespace,
				Name:      consumer.Name,
			},
		})
	}
	return requests
}
