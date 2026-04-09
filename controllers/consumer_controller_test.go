package controllers

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	natsv1 "github.com/synadia-io/synack/api/v1alpha1"
	"github.com/synadia-io/synack/internal/controlplane"
)

func setupConsumerReconciler(t *testing.T, objs ...client.Object) (*ConsumerReconciler, *fakeControlPlaneClient) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := natsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	fcp := &fakeControlPlaneClient{}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&natsv1.Consumer{}, &natsv1.Stream{}).
		WithObjects(objs...).
		Build()

	return &ConsumerReconciler{
		Client:       c,
		Scheme:       scheme,
		ControlPlane: fcp,
	}, fcp
}

func TestConsumerReconcileRejectsMixedStreamSelectors(t *testing.T) {
	consumer := &natsv1.Consumer{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-consumer",
			Namespace:  "default",
			Finalizers: []string{consumerFinalizer},
		},
		Spec: natsv1.ConsumerSpec{
			StreamRef: &natsv1.ConsumerStreamRef{
				Name: "orders",
			},
			StreamID: "S-123",
			Name:     "MY_CONSUMER",
		},
	}

	r, fcp := setupConsumerReconciler(t, consumer)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: consumer.Name, Namespace: consumer.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got natsv1.Consumer
	if err := r.Get(context.Background(), types.NamespacedName{Name: consumer.Name, Namespace: consumer.Namespace}, &got); err != nil {
		t.Fatalf("get consumer: %v", err)
	}
	if !strings.Contains(got.Status.Message, "mutually exclusive") {
		t.Fatalf("expected validation message, got %q", got.Status.Message)
	}
	if fcp.ensureConsumerHit != 0 {
		t.Fatalf("expected no ensure call, got %d", fcp.ensureConsumerHit)
	}
}

func TestConsumerReconcileRejectsNoStreamSelector(t *testing.T) {
	consumer := &natsv1.Consumer{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-consumer",
			Namespace:  "default",
			Finalizers: []string{consumerFinalizer},
		},
		Spec: natsv1.ConsumerSpec{
			Name: "MY_CONSUMER",
		},
	}

	r, fcp := setupConsumerReconciler(t, consumer)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: consumer.Name, Namespace: consumer.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got natsv1.Consumer
	if err := r.Get(context.Background(), types.NamespacedName{Name: consumer.Name, Namespace: consumer.Namespace}, &got); err != nil {
		t.Fatalf("get consumer: %v", err)
	}
	if !strings.Contains(got.Status.Message, "required") {
		t.Fatalf("expected validation message, got %q", got.Status.Message)
	}
	if fcp.ensureConsumerHit != 0 {
		t.Fatalf("expected no ensure call, got %d", fcp.ensureConsumerHit)
	}
}

func TestConsumerReconcileWaitsForReferencedStreamID(t *testing.T) {
	stream := &natsv1.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orders",
			Namespace: "default",
		},
	}
	consumer := &natsv1.Consumer{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-consumer",
			Namespace:  "default",
			Finalizers: []string{consumerFinalizer},
		},
		Spec: natsv1.ConsumerSpec{
			StreamRef: &natsv1.ConsumerStreamRef{
				Name: "orders",
			},
			Name: "MY_CONSUMER",
		},
	}

	r, fcp := setupConsumerReconciler(t, stream, consumer)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: consumer.Name, Namespace: consumer.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Fatalf("expected 5s requeue, got %v", result.RequeueAfter)
	}

	var got natsv1.Consumer
	if err := r.Get(context.Background(), types.NamespacedName{Name: consumer.Name, Namespace: consumer.Namespace}, &got); err != nil {
		t.Fatalf("get consumer: %v", err)
	}
	if !strings.Contains(got.Status.Message, "waiting for referenced Stream") {
		t.Fatalf("expected waiting message, got %q", got.Status.Message)
	}
	if fcp.ensureConsumerHit != 0 {
		t.Fatalf("expected no ensure call, got %d", fcp.ensureConsumerHit)
	}
}

func TestConsumerReconcileUsesReferencedStreamID(t *testing.T) {
	stream := &natsv1.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orders",
			Namespace: "default",
		},
		Status: natsv1.StreamStatus{
			StreamID: "S-999",
		},
	}
	consumer := &natsv1.Consumer{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-consumer",
			Namespace:  "default",
			Finalizers: []string{consumerFinalizer},
		},
		Spec: natsv1.ConsumerSpec{
			StreamRef: &natsv1.ConsumerStreamRef{
				Name: "orders",
			},
			Name: "MY_CONSUMER",
		},
	}

	r, fcp := setupConsumerReconciler(t, stream, consumer)
	fcp.ensureConsumerFn = func(_ context.Context, in controlplane.ConsumerInput) (controlplane.ConsumerResult, error) {
		return controlplane.ConsumerResult{
			ConsumerID: "C-999",
			StreamID:   in.StreamID,
		}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: consumer.Name, Namespace: consumer.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureConsumerHit != 1 {
		t.Fatalf("expected ensure call, got %d", fcp.ensureConsumerHit)
	}
	if fcp.ensureConsumerIn[0].StreamID != "S-999" {
		t.Fatalf("expected stream ID S-999, got %q", fcp.ensureConsumerIn[0].StreamID)
	}

	var got natsv1.Consumer
	if err := r.Get(context.Background(), types.NamespacedName{Name: consumer.Name, Namespace: consumer.Namespace}, &got); err != nil {
		t.Fatalf("get consumer: %v", err)
	}
	if got.Status.ConsumerID != "C-999" {
		t.Fatalf("expected consumer id C-999, got %q", got.Status.ConsumerID)
	}
	if got.Status.Message != "applied" {
		t.Fatalf("expected status message applied, got %q", got.Status.Message)
	}
}

func TestConsumerReconcileDirectStreamID(t *testing.T) {
	consumer := &natsv1.Consumer{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-consumer",
			Namespace:  "default",
			Finalizers: []string{consumerFinalizer},
		},
		Spec: natsv1.ConsumerSpec{
			StreamID: "S-100",
			Name:     "MY_CONSUMER",
		},
	}

	r, fcp := setupConsumerReconciler(t, consumer)
	fcp.ensureConsumerFn = func(_ context.Context, in controlplane.ConsumerInput) (controlplane.ConsumerResult, error) {
		return controlplane.ConsumerResult{ConsumerID: "C-100", StreamID: in.StreamID}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: consumer.Name, Namespace: consumer.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureConsumerHit != 1 {
		t.Fatalf("expected ensure call, got %d", fcp.ensureConsumerHit)
	}
	if fcp.ensureConsumerIn[0].StreamID != "S-100" {
		t.Fatalf("expected direct stream ID S-100, got %q", fcp.ensureConsumerIn[0].StreamID)
	}
}

func TestConsumerReconcilePushConsumer(t *testing.T) {
	consumer := &natsv1.Consumer{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-push",
			Namespace:  "default",
			Finalizers: []string{consumerFinalizer},
		},
		Spec: natsv1.ConsumerSpec{
			StreamID:       "S-100",
			Name:           "MY_PUSH",
			DeliverSubject: "deliver.orders",
		},
	}

	r, fcp := setupConsumerReconciler(t, consumer)
	fcp.ensureConsumerFn = func(_ context.Context, in controlplane.ConsumerInput) (controlplane.ConsumerResult, error) {
		return controlplane.ConsumerResult{ConsumerID: "C-200", StreamID: in.StreamID}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: consumer.Name, Namespace: consumer.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureConsumerHit != 1 {
		t.Fatalf("expected ensure call, got %d", fcp.ensureConsumerHit)
	}
	if fcp.ensureConsumerIn[0].Spec.DeliverSubject != "deliver.orders" {
		t.Fatalf("expected deliver subject, got %q", fcp.ensureConsumerIn[0].Spec.DeliverSubject)
	}
}

func TestEnqueueConsumersForStream(t *testing.T) {
	stream := &natsv1.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orders",
			Namespace: "default",
		},
	}
	matching := &natsv1.Consumer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-consumer",
			Namespace: "default",
		},
		Spec: natsv1.ConsumerSpec{
			StreamRef: &natsv1.ConsumerStreamRef{
				Name: "orders",
			},
			Name: "MY_CONSUMER",
		},
	}
	other := &natsv1.Consumer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-consumer",
			Namespace: "default",
		},
		Spec: natsv1.ConsumerSpec{
			StreamRef: &natsv1.ConsumerStreamRef{
				Name: "other-stream",
			},
			Name: "OTHER_CONSUMER",
		},
	}

	r, _ := setupConsumerReconciler(t, stream, matching, other)
	requests := r.enqueueConsumersForStream(context.Background(), stream)

	if len(requests) != 1 {
		t.Fatalf("expected 1 enqueue request, got %d", len(requests))
	}
	if requests[0].Name != "my-consumer" || requests[0].Namespace != "default" {
		t.Fatalf("unexpected enqueue target: %+v", requests[0].NamespacedName)
	}
}
