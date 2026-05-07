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

func setupAppUserRoleBindingReconciler(t *testing.T, objs ...client.Object) (*AppUserRoleBindingReconciler, *fakeControlPlaneClient) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := natsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	fcp := &fakeControlPlaneClient{}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(
			&natsv1.AppUserRoleBinding{},
			&natsv1.TeamServiceAccount{},
			&natsv1.Team{},
			&natsv1.Account{},
		).
		WithObjects(objs...).
		Build()

	return &AppUserRoleBindingReconciler{
		Client:       c,
		Scheme:       scheme,
		ControlPlane: fcp,
	}, fcp
}

func TestAppUserRoleBindingRejectsNoSubject(t *testing.T) {
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-binding",
			Namespace:  "default",
			Finalizers: []string{appUserRoleBindingFinalizer},
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			Scope:    natsv1.ScopeTeam,
			TargetID: "T-123",
			RoleID:   "R-1",
		},
	}

	r, fcp := setupAppUserRoleBindingReconciler(t, binding)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got natsv1.AppUserRoleBinding
	if err := r.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &got); err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if !strings.Contains(got.Status.Message, "subjectRef or spec.teamAppUserId is required") {
		t.Fatalf("expected subject validation message, got %q", got.Status.Message)
	}
	if fcp.ensureAppUserRoleBindingHit != 0 {
		t.Fatalf("expected no ensure call, got %d", fcp.ensureAppUserRoleBindingHit)
	}
}

func TestAppUserRoleBindingRejectsMixedSubject(t *testing.T) {
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-binding",
			Namespace:  "default",
			Finalizers: []string{appUserRoleBindingFinalizer},
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			SubjectRef:    &natsv1.AppUserRoleBindingSubjectRef{Name: "sa-1"},
			TeamAppUserID: "TAU-direct",
			Scope:         natsv1.ScopeTeam,
			TargetID:      "T-123",
			RoleID:        "R-1",
		},
	}

	r, fcp := setupAppUserRoleBindingReconciler(t, binding)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got natsv1.AppUserRoleBinding
	if err := r.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &got); err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if !strings.Contains(got.Status.Message, "mutually exclusive") {
		t.Fatalf("expected mutually exclusive message, got %q", got.Status.Message)
	}
	if fcp.ensureAppUserRoleBindingHit != 0 {
		t.Fatalf("expected no ensure call, got %d", fcp.ensureAppUserRoleBindingHit)
	}
}

func TestAppUserRoleBindingRejectsNoTarget(t *testing.T) {
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-binding",
			Namespace:  "default",
			Finalizers: []string{appUserRoleBindingFinalizer},
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			TeamAppUserID: "TAU-123",
			Scope:         natsv1.ScopeTeam,
			RoleID:        "R-1",
		},
	}

	r, _ := setupAppUserRoleBindingReconciler(t, binding)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got natsv1.AppUserRoleBinding
	if err := r.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &got); err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if !strings.Contains(got.Status.Message, "targetRef or spec.targetId is required") {
		t.Fatalf("expected target validation message, got %q", got.Status.Message)
	}
}

func TestAppUserRoleBindingRejectsMixedTarget(t *testing.T) {
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-binding",
			Namespace:  "default",
			Finalizers: []string{appUserRoleBindingFinalizer},
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			TeamAppUserID: "TAU-123",
			Scope:         natsv1.ScopeTeam,
			TargetRef:     &natsv1.AppUserRoleBindingTargetRef{Name: "team-1"},
			TargetID:      "T-direct",
			RoleID:        "R-1",
		},
	}

	r, _ := setupAppUserRoleBindingReconciler(t, binding)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got natsv1.AppUserRoleBinding
	if err := r.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &got); err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if !strings.Contains(got.Status.Message, "mutually exclusive") {
		t.Fatalf("expected mutually exclusive message, got %q", got.Status.Message)
	}
}

func TestAppUserRoleBindingHappyPathDirectIDs(t *testing.T) {
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-binding",
			Namespace:  "default",
			Finalizers: []string{appUserRoleBindingFinalizer},
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			TeamAppUserID: "TAU-100",
			Scope:         natsv1.ScopeTeam,
			TargetID:      "T-200",
			RoleID:        "R-1",
		},
	}

	r, fcp := setupAppUserRoleBindingReconciler(t, binding)
	fcp.ensureAppUserRoleBindingFn = func(_ context.Context, in controlplane.AppUserRoleBindingInput) (controlplane.AppUserRoleBindingResult, error) {
		return controlplane.AppUserRoleBindingResult{Bound: true}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureAppUserRoleBindingHit != 1 {
		t.Fatalf("expected 1 ensure call, got %d", fcp.ensureAppUserRoleBindingHit)
	}
	if fcp.ensureAppUserRoleBindingIn[0].TeamAppUserID != "TAU-100" {
		t.Fatalf("expected TAU-100, got %q", fcp.ensureAppUserRoleBindingIn[0].TeamAppUserID)
	}
	if fcp.ensureAppUserRoleBindingIn[0].TargetID != "T-200" {
		t.Fatalf("expected T-200, got %q", fcp.ensureAppUserRoleBindingIn[0].TargetID)
	}

	var got natsv1.AppUserRoleBinding
	if err := r.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &got); err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if !got.Status.Bound {
		t.Fatalf("expected bound=true")
	}
	if got.Status.Message != messageApplied {
		t.Fatalf("expected status message %q, got %q", messageApplied, got.Status.Message)
	}
}

func TestAppUserRoleBindingResolvesSubjectRef(t *testing.T) {
	tsa := &natsv1.TeamServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sa-1",
			Namespace: "default",
		},
		Status: natsv1.TeamServiceAccountStatus{
			TeamAppUserID: "TAU-RESOLVED",
		},
	}
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-binding",
			Namespace:  "default",
			Finalizers: []string{appUserRoleBindingFinalizer},
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			SubjectRef: &natsv1.AppUserRoleBindingSubjectRef{Name: "sa-1"},
			Scope:      natsv1.ScopeTeam,
			TargetID:   "T-200",
			RoleID:     "R-1",
		},
	}

	r, fcp := setupAppUserRoleBindingReconciler(t, tsa, binding)
	fcp.ensureAppUserRoleBindingFn = func(_ context.Context, _ controlplane.AppUserRoleBindingInput) (controlplane.AppUserRoleBindingResult, error) {
		return controlplane.AppUserRoleBindingResult{Bound: true}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureAppUserRoleBindingIn[0].TeamAppUserID != "TAU-RESOLVED" {
		t.Fatalf("expected resolved TAU-RESOLVED, got %q", fcp.ensureAppUserRoleBindingIn[0].TeamAppUserID)
	}
}

func TestAppUserRoleBindingWaitsForSubjectRef(t *testing.T) {
	tsa := &natsv1.TeamServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sa-1",
			Namespace: "default",
		},
		// Status.TeamAppUserID is empty — not ready yet
	}
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-binding",
			Namespace:  "default",
			Finalizers: []string{appUserRoleBindingFinalizer},
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			SubjectRef: &natsv1.AppUserRoleBindingSubjectRef{Name: "sa-1"},
			Scope:      natsv1.ScopeTeam,
			TargetID:   "T-200",
			RoleID:     "R-1",
		},
	}

	r, fcp := setupAppUserRoleBindingReconciler(t, tsa, binding)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != requeueWaitingForResource.RequeueAfter {
		t.Fatalf("expected waiting requeue, got %v", result.RequeueAfter)
	}
	if fcp.ensureAppUserRoleBindingHit != 0 {
		t.Fatalf("expected no ensure call, got %d", fcp.ensureAppUserRoleBindingHit)
	}

	var got natsv1.AppUserRoleBinding
	if err := r.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &got); err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if !strings.Contains(got.Status.Message, "waiting for referenced TeamServiceAccount") {
		t.Fatalf("expected waiting message, got %q", got.Status.Message)
	}
}

func TestAppUserRoleBindingResolvesTeamTargetRef(t *testing.T) {
	team := &natsv1.Team{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "eng-team",
			Namespace: "default",
		},
		Status: natsv1.TeamStatus{
			ID: "T-RESOLVED",
		},
	}
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-binding",
			Namespace:  "default",
			Finalizers: []string{appUserRoleBindingFinalizer},
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			TeamAppUserID: "TAU-100",
			Scope:         natsv1.ScopeTeam,
			TargetRef:     &natsv1.AppUserRoleBindingTargetRef{Name: "eng-team"},
			RoleID:        "R-1",
		},
	}

	r, fcp := setupAppUserRoleBindingReconciler(t, team, binding)
	fcp.ensureAppUserRoleBindingFn = func(_ context.Context, _ controlplane.AppUserRoleBindingInput) (controlplane.AppUserRoleBindingResult, error) {
		return controlplane.AppUserRoleBindingResult{Bound: true}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureAppUserRoleBindingIn[0].TargetID != "T-RESOLVED" {
		t.Fatalf("expected resolved T-RESOLVED, got %q", fcp.ensureAppUserRoleBindingIn[0].TargetID)
	}
}

func TestAppUserRoleBindingResolvesAccountTargetRef(t *testing.T) {
	account := &natsv1.Account{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-acct",
			Namespace: "default",
		},
		Status: natsv1.AccountStatus{
			AccountID: "A-RESOLVED",
		},
	}
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-binding",
			Namespace:  "default",
			Finalizers: []string{appUserRoleBindingFinalizer},
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			TeamAppUserID: "TAU-100",
			Scope:         natsv1.ScopeAccount,
			TargetRef:     &natsv1.AppUserRoleBindingTargetRef{Name: "app-acct"},
			RoleID:        "R-1",
		},
	}

	r, fcp := setupAppUserRoleBindingReconciler(t, account, binding)
	fcp.ensureAppUserRoleBindingFn = func(_ context.Context, _ controlplane.AppUserRoleBindingInput) (controlplane.AppUserRoleBindingResult, error) {
		return controlplane.AppUserRoleBindingResult{Bound: true}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fcp.ensureAppUserRoleBindingIn[0].TargetID != "A-RESOLVED" {
		t.Fatalf("expected resolved A-RESOLVED, got %q", fcp.ensureAppUserRoleBindingIn[0].TargetID)
	}
	if fcp.ensureAppUserRoleBindingIn[0].Scope != controlplane.RoleBindingScopeAccount {
		t.Fatalf("expected Account scope, got %q", fcp.ensureAppUserRoleBindingIn[0].Scope)
	}
}

func TestAppUserRoleBindingTargetRefUnsupportedForSystemScope(t *testing.T) {
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-binding",
			Namespace:  "default",
			Finalizers: []string{appUserRoleBindingFinalizer},
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			TeamAppUserID: "TAU-100",
			Scope:         natsv1.ScopeSystem,
			TargetRef:     &natsv1.AppUserRoleBindingTargetRef{Name: "some-thing"},
			RoleID:        "R-1",
		},
	}

	r, fcp := setupAppUserRoleBindingReconciler(t, binding)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace},
	})
	if err == nil || !strings.Contains(err.Error(), "targetRef is not supported for scope System") {
		t.Fatalf("expected unsupported scope error, got %v", err)
	}
	if fcp.ensureAppUserRoleBindingHit != 0 {
		t.Fatalf("expected no ensure call, got %d", fcp.ensureAppUserRoleBindingHit)
	}

	var got natsv1.AppUserRoleBinding
	if err := r.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &got); err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if !strings.Contains(got.Status.Message, "targetRef is not supported for scope System") {
		t.Fatalf("expected unsupported scope message, got %q", got.Status.Message)
	}
}

func TestEnqueueBindingsForServiceAccount(t *testing.T) {
	tsa := &natsv1.TeamServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sa-1",
			Namespace: "default",
		},
	}
	matching := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "binding-1",
			Namespace: "default",
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			SubjectRef: &natsv1.AppUserRoleBindingSubjectRef{Name: "sa-1"},
			Scope:      natsv1.ScopeTeam,
			TargetID:   "T-1",
			RoleID:     "R-1",
		},
	}
	other := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "binding-2",
			Namespace: "default",
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			SubjectRef: &natsv1.AppUserRoleBindingSubjectRef{Name: "sa-other"},
			Scope:      natsv1.ScopeTeam,
			TargetID:   "T-2",
			RoleID:     "R-1",
		},
	}

	r, _ := setupAppUserRoleBindingReconciler(t, tsa, matching, other)
	requests := r.enqueueBindingsForServiceAccount(context.Background(), tsa)

	if len(requests) != 1 {
		t.Fatalf("expected 1 enqueue request, got %d", len(requests))
	}
	if requests[0].Name != "binding-1" {
		t.Fatalf("unexpected enqueue target: %+v", requests[0].NamespacedName)
	}
}
