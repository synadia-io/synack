package controllers

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	natsv1 "github.com/synadia-io/synack/api/v1alpha1"
	"github.com/synadia-io/synack/internal/controlplane"
)

func setupAccountReconciler(t *testing.T, objs ...client.Object) (*AccountReconciler, *fakeControlPlaneClient) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := natsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	fcp := &fakeControlPlaneClient{}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(
			&natsv1.Account{},
			&natsv1.Stream{},
			&natsv1.KeyValue{},
			&natsv1.ObjectStore{},
			&natsv1.NatsUser{},
			&natsv1.AppUserRoleBinding{},
		).
		WithObjects(objs...).
		Build()

	return &AccountReconciler{
		Client:       c,
		Scheme:       scheme,
		ControlPlane: fcp,
	}, fcp
}

func TestAccountReconcileHappyPath(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "app-team",
			Namespace:  "default",
			Finalizers: []string{accountFinalizer},
		},
		Spec: natsv1.AccountSpec{
			SystemID: "SYS-1",
			Name:     "app-team",
		},
	}

	r, fcp := setupAccountReconciler(t, account)
	fcp.ensureAccountFn = func(_ context.Context, in controlplane.AccountInput) (controlplane.AccountResult, error) {
		return controlplane.AccountResult{AccountID: "A-500"}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: account.Name, Namespace: account.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureAccountHit != 1 {
		t.Fatalf("expected 1 ensure call, got %d", fcp.ensureAccountHit)
	}
	if fcp.ensureAccountIn[0].SystemID != "SYS-1" {
		t.Fatalf("expected system ID SYS-1, got %q", fcp.ensureAccountIn[0].SystemID)
	}

	var got natsv1.Account
	if err := r.Get(context.Background(), types.NamespacedName{Name: account.Name, Namespace: account.Namespace}, &got); err != nil {
		t.Fatalf("get account: %v", err)
	}
	if got.Status.AccountID != "A-500" {
		t.Fatalf("expected account id A-500, got %q", got.Status.AccountID)
	}
	if got.Status.Message != messageApplied {
		t.Fatalf("expected status message %q, got %q", messageApplied, got.Status.Message)
	}
}

func TestAccountReconcileAdoptMode(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "existing",
			Namespace:  "default",
			Finalizers: []string{accountFinalizer},
		},
		Spec: natsv1.AccountSpec{
			AccountID: "A-EXISTING",
			SystemID:  "SYS-1",
			Name:      "existing",
		},
	}

	r, fcp := setupAccountReconciler(t, account)
	fcp.ensureAccountFn = func(_ context.Context, in controlplane.AccountInput) (controlplane.AccountResult, error) {
		return controlplane.AccountResult{AccountID: in.AccountID}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: account.Name, Namespace: account.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureAccountIn[0].AccountID != "A-EXISTING" {
		t.Fatalf("expected adopt account ID A-EXISTING, got %q", fcp.ensureAccountIn[0].AccountID)
	}
}

func TestAccountReconcileDeletionBlockedByDependents(t *testing.T) {
	now := metav1.Now()
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "app-team",
			Namespace:         "default",
			Finalizers:        []string{accountFinalizer},
			DeletionTimestamp: &now,
		},
		Status: natsv1.AccountStatus{
			AccountID: "A-500",
		},
	}
	stream := &natsv1.Stream{
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

	r, _ := setupAccountReconciler(t, account, stream)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: account.Name, Namespace: account.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != requeueWaitingForResource.RequeueAfter {
		t.Fatalf("expected waiting requeue, got %v", result.RequeueAfter)
	}

	var got natsv1.Account
	if err := r.Get(context.Background(), types.NamespacedName{Name: account.Name, Namespace: account.Namespace}, &got); err != nil {
		t.Fatalf("get account: %v", err)
	}
	if !strings.Contains(got.Status.Message, "waiting for dependent resources") {
		t.Fatalf("expected dependent resources message, got %q", got.Status.Message)
	}
}

func TestAccountReconcileDeletionBlockedByTargetRoleBinding(t *testing.T) {
	now := metav1.Now()
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "app-team",
			Namespace:         "default",
			Finalizers:        []string{accountFinalizer},
			DeletionTimestamp: &now,
		},
		Status: natsv1.AccountStatus{
			AccountID: "A-500",
		},
	}
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "account-binding",
			Namespace: "default",
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			TeamAppUserID: "TAU-1",
			Scope:         natsv1.ScopeAccount,
			TargetRef:     &natsv1.AppUserRoleBindingTargetRef{Name: "app-team"},
			RoleID:        "R-1",
		},
	}

	r, _ := setupAccountReconciler(t, account, binding)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: account.Name, Namespace: account.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != requeueWaitingForResource.RequeueAfter {
		t.Fatalf("expected waiting requeue, got %v", result.RequeueAfter)
	}

	var got natsv1.Account
	if err := r.Get(context.Background(), types.NamespacedName{Name: account.Name, Namespace: account.Namespace}, &got); err != nil {
		t.Fatalf("get account: %v", err)
	}
	if !strings.Contains(got.Status.Message, "waiting for dependent resources") {
		t.Fatalf("expected dependent resources message, got %q", got.Status.Message)
	}
}

func TestAccountReconcileFinalizerAddedOnFirstReconcile(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-team",
			Namespace: "default",
		},
		Spec: natsv1.AccountSpec{
			SystemID: "SYS-1",
			Name:     "app-team",
		},
	}

	r, fcp := setupAccountReconciler(t, account)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: account.Name, Namespace: account.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("expected requeue after adding finalizer")
	}
	if fcp.ensureAccountHit != 0 {
		t.Fatalf("expected no ensure call on finalizer-add pass, got %d", fcp.ensureAccountHit)
	}

	var got natsv1.Account
	if err := r.Get(context.Background(), types.NamespacedName{Name: account.Name, Namespace: account.Namespace}, &got); err != nil {
		t.Fatalf("get account: %v", err)
	}
	found := false
	for _, f := range got.Finalizers {
		if f == accountFinalizer {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected finalizer %q to be set", accountFinalizer)
	}
}
