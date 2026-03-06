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

var (
	requeueWaitingForResource = ctrl.Result{RequeueAfter: 5 * time.Second}
	requeueReconcileErr       = ctrl.Result{RequeueAfter: 15 * time.Second}

	errWaitingForStream = fmt.Errorf("waiting for referenced Stream to be ready")
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

	var consumer natsv1.Consumer
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

			// If we never had a consumer ID, this resource was never fully reconciled
			if consumer.Status.ConsumerID == "" {
				if ok := controllerutil.RemoveFinalizer(&consumer, consumerFinalizer); !ok {
					return ctrl.Result{}, nil
				}
				if err := r.Update(ctx, &consumer); err != nil {
					return requeueOnConflict(err)
				}
				return ctrl.Result{}, nil
			}

			in := controlplane.ConsumerInput{
				StreamID:   consumer.Status.StreamID,
				ConsumerID: knownConsumerID,
				Spec: natsv1.ConsumerSpec{
					Name: consumer.Spec.Name,
				},
			}

			if err := r.ControlPlane.DeleteConsumer(ctx, in); err != nil {
				l.Error(err, "consumer delete failed")
				consumer.Status.Message = err.Error()
				if err := r.Status().Update(ctx, &consumer); err != nil {
					l.Error(err, "failed to update consumer status")
				}
				return requeueReconcileErr, nil
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
		if err := r.Status().Update(ctx, &consumer); err != nil {
			l.Error(err, "failed to update consumer status")
		}
		return ctrl.Result{}, nil
	}

	streamID, err := r.resolveStreamID(ctx, &consumer)
	if err != nil {
		if errors.Is(err, errWaitingForStream) {
			consumer.Status.Message = err.Error()
			if err := r.Status().Update(ctx, &consumer); err != nil {
				l.Error(err, "failed to update consumer status")
			}
			return requeueWaitingForResource, nil
		}

		l.Error(err, "failed to resolve stream for consumer")
		consumer.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &consumer); err != nil {
			l.Error(err, "failed to update consumer status")
		}
		return requeueReconcileErr, nil
	}

	in := controlplane.ConsumerInput{
		StreamID:   streamID,
		ConsumerID: knownConsumerID,
		Spec:       consumer.Spec,
	}

	appliedState := loadAnnotation(&consumer, consumerAppliedStateAnnotation)
	desiredState, err := json.Marshal(in)
	if err != nil {
		consumer.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &consumer); err != nil {
			l.Error(err, "failed to update consumer status")
		}
		return requeueReconcileErr, nil
	}

	if diff, err := diffState(appliedState, desiredState); err != nil {
		l.Error(err, "failed to diff consumer state")
		consumer.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &consumer); err != nil {
			l.Error(err, "failed to update consumer status")
		}
		return requeueReconcileErr, nil
	} else if diff != "" {
		l.Info("consumer desired state changed", "diff", diff)
	}

	out, err := r.ControlPlane.EnsureConsumer(ctx, in)
	if err != nil {
		l.Error(err, "consumer update failed")
		consumer.Status.Message = err.Error()
		if err := r.Status().Update(ctx, &consumer); err != nil {
			l.Error(err, "failed to update consumer status")
		}
		return requeueReconcileErr, nil
	}

	desiredStatus := consumer.Status
	desiredStatus.ConsumerID = out.ConsumerID
	desiredStatus.StreamID = out.StreamID
	desiredStatus.Message = "applied"

	if desiredStatus != consumer.Status {
		desiredStatus.LastSynced = time.Now().UTC().Format(time.RFC3339)
		consumer.Status = desiredStatus
		if err := r.Status().Update(ctx, &consumer); err != nil {
			return requeueOnConflict(err)
		}
	}

	if setAnnotations(&consumer, consumerAppliedStateAnnotation, desiredState) {
		if err := r.Update(ctx, &consumer); err != nil {
			return requeueOnConflict(err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *ConsumerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1.Consumer{}).
		Watches(&natsv1.Stream{}, handler.EnqueueRequestsFromMapFunc(r.enqueueConsumersForStream)).
		Complete(r)
}

func validateStreamSelectors(spec natsv1.ConsumerSpec) error {
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

func (r *ConsumerReconciler) resolveStreamID(ctx context.Context, consumer *natsv1.Consumer) (string, error) {
	if consumer.Spec.StreamRef == nil {
		return consumer.Spec.StreamID, nil
	}

	var stream natsv1.Stream
	key := types.NamespacedName{
		Namespace: consumer.Namespace,
		Name:      consumer.Spec.StreamRef.Name,
	}

	if err := r.Get(ctx, key, &stream); err != nil {
		if apierrors.IsNotFound(err) {
			return "", errors.Join(errWaitingForStream, fmt.Errorf("referenced Stream %q not found", consumer.Spec.StreamRef.Name))
		}
		return "", err
	}

	if stream.Status.StreamID == "" {
		return "", errors.Join(errWaitingForStream, fmt.Errorf("referenced Stream %q is not ready", consumer.Spec.StreamRef.Name))
	}

	return stream.Status.StreamID, nil
}

func (r *ConsumerReconciler) enqueueConsumersForStream(ctx context.Context, obj client.Object) []reconcile.Request {
	stream, ok := obj.(*natsv1.Stream)
	if !ok {
		return nil
	}

	var consumers natsv1.ConsumerList
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
