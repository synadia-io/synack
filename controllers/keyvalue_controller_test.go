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

func setupKVBucketReconciler(t *testing.T, objs ...client.Object) (*KeyValueReconciler, *fakeControlPlaneClient) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := natsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	fcp := &fakeControlPlaneClient{}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&natsv1.KeyValue{}, &natsv1.Account{}).
		WithObjects(objs...).
		Build()

	return &KeyValueReconciler{
		Client:       c,
		Scheme:       scheme,
		ControlPlane: fcp,
	}, fcp
}

func TestKVBucketReconcileRejectsMixedAccountSelectors(t *testing.T) {
	kv := &natsv1.KeyValue{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-kv",
			Namespace:  "default",
			Finalizers: []string{keyValueFinalizer},
		},
		Spec: natsv1.KeyValueSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-123",
				AccountRef: &natsv1.AccountRef{
					Name: "app-team",
				},
			},
			Bucket: "MY_KV",
		},
	}

	r, fcp := setupKVBucketReconciler(t, kv)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: kv.Name, Namespace: kv.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got natsv1.KeyValue
	if err := r.Get(context.Background(), types.NamespacedName{Name: kv.Name, Namespace: kv.Namespace}, &got); err != nil {
		t.Fatalf("get kv bucket: %v", err)
	}
	if !strings.Contains(got.Status.Message, "cannot be combined") {
		t.Fatalf("expected validation message, got %q", got.Status.Message)
	}
	if fcp.ensureKeyValueHit != 0 {
		t.Fatalf("expected no ensure call, got %d", fcp.ensureKeyValueHit)
	}
}

func TestKVBucketReconcileWaitsForReferencedAccountID(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-team",
			Namespace: "default",
		},
	}
	kv := &natsv1.KeyValue{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-kv",
			Namespace:  "default",
			Finalizers: []string{keyValueFinalizer},
		},
		Spec: natsv1.KeyValueSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountRef: &natsv1.AccountRef{
					Name: "app-team",
				},
			},
			Bucket: "MY_KV",
		},
	}

	r, fcp := setupKVBucketReconciler(t, account, kv)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: kv.Name, Namespace: kv.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Fatalf("expected 5s requeue, got %v", result.RequeueAfter)
	}

	var got natsv1.KeyValue
	if err := r.Get(context.Background(), types.NamespacedName{Name: kv.Name, Namespace: kv.Namespace}, &got); err != nil {
		t.Fatalf("get kv bucket: %v", err)
	}
	if !strings.Contains(got.Status.Message, "waiting for referenced Account") {
		t.Fatalf("expected waiting message, got %q", got.Status.Message)
	}
	if fcp.ensureKeyValueHit != 0 {
		t.Fatalf("expected no ensure call, got %d", fcp.ensureKeyValueHit)
	}
}

func TestKVBucketReconcileUsesReferencedAccountID(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-team",
			Namespace: "default",
		},
		Status: natsv1.AccountStatus{
			AccountID: "A-777",
		},
	}
	kv := &natsv1.KeyValue{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-kv",
			Namespace:  "default",
			Finalizers: []string{keyValueFinalizer},
		},
		Spec: natsv1.KeyValueSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountRef: &natsv1.AccountRef{
					Name: "app-team",
				},
			},
			Bucket: "MY_KV",
		},
	}

	r, fcp := setupKVBucketReconciler(t, account, kv)
	fcp.ensureKeyValueFn = func(_ context.Context, in controlplane.KeyValueInput) (controlplane.KeyValueResult, error) {
		return controlplane.KeyValueResult{
			AccountID:  in.AccountID,
			KeyValueID: "KV-999",
		}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: kv.Name, Namespace: kv.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureKeyValueHit != 1 {
		t.Fatalf("expected ensure call, got %d", fcp.ensureKeyValueHit)
	}
	if fcp.ensureKeyValueIn[0].AccountID != "A-777" {
		t.Fatalf("expected account ID A-777, got %q", fcp.ensureKeyValueIn[0].AccountID)
	}

	var got natsv1.KeyValue
	if err := r.Get(context.Background(), types.NamespacedName{Name: kv.Name, Namespace: kv.Namespace}, &got); err != nil {
		t.Fatalf("get kv bucket: %v", err)
	}
	if got.Status.KeyValueID != "KV-999" {
		t.Fatalf("expected kv bucket id KV-999, got %q", got.Status.KeyValueID)
	}
	if got.Status.Message != "applied" {
		t.Fatalf("expected status message applied, got %q", got.Status.Message)
	}
}

func TestKVBucketReconcileDirectAccountID(t *testing.T) {
	kv := &natsv1.KeyValue{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-kv",
			Namespace:  "default",
			Finalizers: []string{keyValueFinalizer},
		},
		Spec: natsv1.KeyValueSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-100",
			},
			Bucket: "MY_KV",
		},
	}

	r, fcp := setupKVBucketReconciler(t, kv)
	fcp.ensureKeyValueFn = func(_ context.Context, in controlplane.KeyValueInput) (controlplane.KeyValueResult, error) {
		return controlplane.KeyValueResult{AccountID: in.AccountID, KeyValueID: "KV-100"}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: kv.Name, Namespace: kv.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureKeyValueHit != 1 {
		t.Fatalf("expected ensure call, got %d", fcp.ensureKeyValueHit)
	}
	if fcp.ensureKeyValueIn[0].AccountID != "A-100" {
		t.Fatalf("expected direct account ID A-100, got %q", fcp.ensureKeyValueIn[0].AccountID)
	}
}

func TestEnqueueKVBucketsForAccount(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-team",
			Namespace: "default",
		},
	}
	matching := &natsv1.KeyValue{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-kv",
			Namespace: "default",
		},
		Spec: natsv1.KeyValueSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountRef: &natsv1.AccountRef{
					Name: "app-team",
				},
			},
			Bucket: "MY_KV",
		},
	}
	other := &natsv1.KeyValue{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-kv",
			Namespace: "default",
		},
		Spec: natsv1.KeyValueSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountRef: &natsv1.AccountRef{
					Name: "other-team",
				},
			},
			Bucket: "OTHER_KV",
		},
	}

	r, _ := setupKVBucketReconciler(t, account, matching, other)
	requests := r.enqueueKVBucketsForAccount(context.Background(), account)

	if len(requests) != 1 {
		t.Fatalf("expected 1 enqueue request, got %d", len(requests))
	}
	if requests[0].Name != "my-kv" || requests[0].Namespace != "default" {
		t.Fatalf("unexpected enqueue target: %+v", requests[0].NamespacedName)
	}
}
