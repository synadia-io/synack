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

func setupObjectStoreReconciler(t *testing.T, objs ...client.Object) (*ObjectStoreReconciler, *fakeControlPlaneClient) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := natsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	fcp := &fakeControlPlaneClient{}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&natsv1.ObjectStore{}, &natsv1.Account{}).
		WithObjects(objs...).
		Build()

	return &ObjectStoreReconciler{
		Client:       c,
		Scheme:       scheme,
		ControlPlane: fcp,
	}, fcp
}

func TestObjectStoreReconcileRejectsMixedAccountSelectors(t *testing.T) {
	obj := &natsv1.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-obj",
			Namespace:  "default",
			Finalizers: []string{objectStoreFinalizer},
		},
		Spec: natsv1.ObjectStoreSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-123",
				AccountRef: &natsv1.AccountRef{
					Name: "app-team",
				},
			},
			Bucket: "MY_OBJ",
		},
	}

	r, fcp := setupObjectStoreReconciler(t, obj)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: obj.Name, Namespace: obj.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got natsv1.ObjectStore
	if err := r.Get(context.Background(), types.NamespacedName{Name: obj.Name, Namespace: obj.Namespace}, &got); err != nil {
		t.Fatalf("get object bucket: %v", err)
	}
	if !strings.Contains(got.Status.Message, "cannot be combined") {
		t.Fatalf("expected validation message, got %q", got.Status.Message)
	}
	if fcp.ensureObjectStoreHit != 0 {
		t.Fatalf("expected no ensure call, got %d", fcp.ensureObjectStoreHit)
	}
}

func TestObjectStoreReconcileWaitsForReferencedAccountID(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-team",
			Namespace: "default",
		},
	}
	obj := &natsv1.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-obj",
			Namespace:  "default",
			Finalizers: []string{objectStoreFinalizer},
		},
		Spec: natsv1.ObjectStoreSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountRef: &natsv1.AccountRef{
					Name: "app-team",
				},
			},
			Bucket: "MY_OBJ",
		},
	}

	r, fcp := setupObjectStoreReconciler(t, account, obj)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: obj.Name, Namespace: obj.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Fatalf("expected 5s requeue, got %v", result.RequeueAfter)
	}

	var got natsv1.ObjectStore
	if err := r.Get(context.Background(), types.NamespacedName{Name: obj.Name, Namespace: obj.Namespace}, &got); err != nil {
		t.Fatalf("get object bucket: %v", err)
	}
	if !strings.Contains(got.Status.Message, "waiting for referenced Account") {
		t.Fatalf("expected waiting message, got %q", got.Status.Message)
	}
	if fcp.ensureObjectStoreHit != 0 {
		t.Fatalf("expected no ensure call, got %d", fcp.ensureObjectStoreHit)
	}
}

func TestObjectStoreReconcileUsesReferencedAccountID(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-team",
			Namespace: "default",
		},
		Status: natsv1.AccountStatus{
			AccountID: "A-777",
		},
	}
	obj := &natsv1.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-obj",
			Namespace:  "default",
			Finalizers: []string{objectStoreFinalizer},
		},
		Spec: natsv1.ObjectStoreSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountRef: &natsv1.AccountRef{
					Name: "app-team",
				},
			},
			Bucket: "MY_OBJ",
		},
	}

	r, fcp := setupObjectStoreReconciler(t, account, obj)
	fcp.ensureObjectStoreFn = func(_ context.Context, in controlplane.ObjectStoreInput) (controlplane.ObjectStoreResult, error) {
		return controlplane.ObjectStoreResult{
			AccountID:     in.AccountID,
			ObjectStoreID: "OBJ-999",
		}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: obj.Name, Namespace: obj.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureObjectStoreHit != 1 {
		t.Fatalf("expected ensure call, got %d", fcp.ensureObjectStoreHit)
	}
	if fcp.ensureObjectStoreIn[0].AccountID != "A-777" {
		t.Fatalf("expected account ID A-777, got %q", fcp.ensureObjectStoreIn[0].AccountID)
	}

	var got natsv1.ObjectStore
	if err := r.Get(context.Background(), types.NamespacedName{Name: obj.Name, Namespace: obj.Namespace}, &got); err != nil {
		t.Fatalf("get object bucket: %v", err)
	}
	if got.Status.ObjectStoreID != "OBJ-999" {
		t.Fatalf("expected object bucket id OBJ-999, got %q", got.Status.ObjectStoreID)
	}
	if got.Status.Message != "applied" {
		t.Fatalf("expected status message applied, got %q", got.Status.Message)
	}
}

func TestObjectStoreReconcileDirectAccountID(t *testing.T) {
	obj := &natsv1.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-obj",
			Namespace:  "default",
			Finalizers: []string{objectStoreFinalizer},
		},
		Spec: natsv1.ObjectStoreSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-100",
			},
			Bucket: "MY_OBJ",
		},
	}

	r, fcp := setupObjectStoreReconciler(t, obj)
	fcp.ensureObjectStoreFn = func(_ context.Context, in controlplane.ObjectStoreInput) (controlplane.ObjectStoreResult, error) {
		return controlplane.ObjectStoreResult{AccountID: in.AccountID, ObjectStoreID: "OBJ-100"}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: obj.Name, Namespace: obj.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureObjectStoreHit != 1 {
		t.Fatalf("expected ensure call, got %d", fcp.ensureObjectStoreHit)
	}
	if fcp.ensureObjectStoreIn[0].AccountID != "A-100" {
		t.Fatalf("expected direct account ID A-100, got %q", fcp.ensureObjectStoreIn[0].AccountID)
	}
}

func TestEnqueueObjectStoresForAccount(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-team",
			Namespace: "default",
		},
	}
	matching := &natsv1.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-obj",
			Namespace: "default",
		},
		Spec: natsv1.ObjectStoreSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountRef: &natsv1.AccountRef{
					Name: "app-team",
				},
			},
			Bucket: "MY_OBJ",
		},
	}
	other := &natsv1.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-obj",
			Namespace: "default",
		},
		Spec: natsv1.ObjectStoreSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountRef: &natsv1.AccountRef{
					Name: "other-team",
				},
			},
			Bucket: "OTHER_OBJ",
		},
	}

	r, _ := setupObjectStoreReconciler(t, account, matching, other)
	requests := r.enqueueObjectStoresForAccount(context.Background(), account)

	if len(requests) != 1 {
		t.Fatalf("expected 1 enqueue request, got %d", len(requests))
	}
	if requests[0].Name != "my-obj" || requests[0].Namespace != "default" {
		t.Fatalf("unexpected enqueue target: %+v", requests[0].NamespacedName)
	}
}
