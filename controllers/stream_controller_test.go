package controllers

import (
	"context"
	"fmt"
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

func setupStreamReconciler(t *testing.T, objs ...client.Object) (*StreamReconciler, *fakeControlPlaneClient) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := natsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	fcp := &fakeControlPlaneClient{}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&natsv1.Stream{}, &natsv1.Account{}).
		WithObjects(objs...).
		Build()

	return &StreamReconciler{
		Client:       c,
		Scheme:       scheme,
		ControlPlane: fcp,
	}, fcp
}

func TestStreamReconcileRejectsMixedAccountSelectors(t *testing.T) {
	stream := &natsv1.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "orders",
			Namespace:  "default",
			Finalizers: []string{streamFinalizer},
		},
		Spec: natsv1.StreamSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-123",
				AccountRef: &natsv1.AccountRef{
					Name: "app-team",
				},
			},
			Name: "ORDERS",
		},
	}

	r, fcp := setupStreamReconciler(t, stream)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: stream.Name, Namespace: stream.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got natsv1.Stream
	if err := r.Get(context.Background(), types.NamespacedName{Name: stream.Name, Namespace: stream.Namespace}, &got); err != nil {
		t.Fatalf("get stream: %v", err)
	}
	if !strings.Contains(got.Status.Message, "cannot be combined") {
		t.Fatalf("expected validation message, got %q", got.Status.Message)
	}
	if fcp.ensureStreamHit != 0 {
		t.Fatalf("expected no ensure stream call, got %d", fcp.ensureStreamHit)
	}
}

func TestStreamReconcileWaitsForReferencedAccountID(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-team",
			Namespace: "default",
		},
	}
	stream := &natsv1.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "orders",
			Namespace:  "default",
			Finalizers: []string{streamFinalizer},
		},
		Spec: natsv1.StreamSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountRef: &natsv1.AccountRef{
					Name: "app-team",
				},
			},
			Name: "ORDERS",
		},
	}

	r, fcp := setupStreamReconciler(t, account, stream)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: stream.Name, Namespace: stream.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Fatalf("expected 5s requeue, got %v", result.RequeueAfter)
	}

	var got natsv1.Stream
	if err := r.Get(context.Background(), types.NamespacedName{Name: stream.Name, Namespace: stream.Namespace}, &got); err != nil {
		t.Fatalf("get stream: %v", err)
	}
	if !strings.Contains(got.Status.Message, "waiting for referenced Account") {
		t.Fatalf("expected waiting message, got %q", got.Status.Message)
	}
	if fcp.ensureStreamHit != 0 {
		t.Fatalf("expected no ensure stream call, got %d", fcp.ensureStreamHit)
	}
}

func TestStreamReconcileUsesReferencedAccountID(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-team",
			Namespace: "default",
		},
		Status: natsv1.AccountStatus{
			AccountID: "A-777",
		},
	}
	stream := &natsv1.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "orders",
			Namespace:  "default",
			Finalizers: []string{streamFinalizer},
		},
		Spec: natsv1.StreamSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountRef: &natsv1.AccountRef{
					Name: "app-team",
				},
			},
			Name: "ORDERS",
		},
	}

	r, fcp := setupStreamReconciler(t, account, stream)
	fcp.ensureStreamFn = func(_ context.Context, in controlplane.StreamInput) (controlplane.StreamResult, error) {
		return controlplane.StreamResult{
			AccountID: in.AccountID,
			StreamID:  "S-999",
		}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: stream.Name, Namespace: stream.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureStreamHit != 1 {
		t.Fatalf("expected ensure stream call, got %d", fcp.ensureStreamHit)
	}
	if fcp.ensureStreamIn[0].AccountID != "A-777" {
		t.Fatalf("expected account ID A-777, got %q", fcp.ensureStreamIn[0].AccountID)
	}

	var got natsv1.Stream
	if err := r.Get(context.Background(), types.NamespacedName{Name: stream.Name, Namespace: stream.Namespace}, &got); err != nil {
		t.Fatalf("get stream: %v", err)
	}
	if got.Status.StreamID != "S-999" {
		t.Fatalf("expected stream id S-999, got %q", got.Status.StreamID)
	}
	if got.Status.Message != "applied" {
		t.Fatalf("expected status message applied, got %q", got.Status.Message)
	}
}

func TestStreamReconcileStillSupportsDirectSelectors(t *testing.T) {
	stream := &natsv1.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "orders",
			Namespace:  "default",
			Finalizers: []string{streamFinalizer},
		},
		Spec: natsv1.StreamSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-100",
			},
			Name: "ORDERS",
		},
	}

	r, fcp := setupStreamReconciler(t, stream)
	fcp.ensureStreamFn = func(_ context.Context, in controlplane.StreamInput) (controlplane.StreamResult, error) {
		return controlplane.StreamResult{AccountID: in.AccountID, StreamID: "S-100"}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: stream.Name, Namespace: stream.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureStreamHit != 1 {
		t.Fatalf("expected ensure stream call, got %d", fcp.ensureStreamHit)
	}
	if fcp.ensureStreamIn[0].AccountID != "A-100" {
		t.Fatalf("expected direct account ID A-100, got %q", fcp.ensureStreamIn[0].AccountID)
	}
}

func TestStreamReconcileEnsureError(t *testing.T) {
	stream := &natsv1.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "orders",
			Namespace:  "default",
			Finalizers: []string{streamFinalizer},
		},
		Spec: natsv1.StreamSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-100",
			},
			Name: "ORDERS",
		},
	}

	r, fcp := setupStreamReconciler(t, stream)
	fcp.ensureStreamFn = func(_ context.Context, _ controlplane.StreamInput) (controlplane.StreamResult, error) {
		return controlplane.StreamResult{}, fmt.Errorf("control plane unavailable")
	}

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: stream.Name, Namespace: stream.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 15*time.Second {
		t.Fatalf("expected 15s error requeue, got %v", result.RequeueAfter)
	}

	var got natsv1.Stream
	if err := r.Get(context.Background(), types.NamespacedName{Name: stream.Name, Namespace: stream.Namespace}, &got); err != nil {
		t.Fatalf("get stream: %v", err)
	}
	if !strings.Contains(got.Status.Message, "control plane unavailable") {
		t.Fatalf("expected error in status message, got %q", got.Status.Message)
	}
}

func TestEnqueueStreamsForAccount(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-team",
			Namespace: "default",
		},
	}
	matching := &natsv1.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orders",
			Namespace: "default",
		},
		Spec: natsv1.StreamSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountRef: &natsv1.AccountRef{
					Name: "app-team",
				},
			},
			Name: "ORDERS",
		},
	}
	other := &natsv1.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "payments",
			Namespace: "default",
		},
		Spec: natsv1.StreamSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountRef: &natsv1.AccountRef{
					Name: "other-team",
				},
			},
			Name: "PAYMENTS",
		},
	}

	r, _ := setupStreamReconciler(t, account, matching, other)
	requests := r.enqueueStreamsForAccount(context.Background(), account)

	if len(requests) != 1 {
		t.Fatalf("expected 1 enqueue request, got %d", len(requests))
	}
	if requests[0].Name != "orders" || requests[0].Namespace != "default" {
		t.Fatalf("unexpected enqueue target: %+v", requests[0].NamespacedName)
	}
}
