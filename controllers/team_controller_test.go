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
)

func setupTeamReconciler(t *testing.T, objs ...client.Object) (*TeamReconciler, *fakeControlPlaneClient) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := natsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	fcp := &fakeControlPlaneClient{}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&natsv1.Team{}, &natsv1.TeamServiceAccount{}, &natsv1.AppUserRoleBinding{}).
		WithObjects(objs...).
		Build()

	return &TeamReconciler{
		Client:       c,
		Scheme:       scheme,
		ControlPlane: fcp,
	}, fcp
}

func TestTeamReconcileDeletionBlockedByTeamServiceAccount(t *testing.T) {
	now := metav1.Now()
	team := &natsv1.Team{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "eng-team",
			Namespace:         "default",
			Finalizers:        []string{teamFinalizer},
			DeletionTimestamp: &now,
		},
		Status: natsv1.TeamStatus{
			ID: "T-1",
		},
	}
	tsa := &natsv1.TeamServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bot",
			Namespace: "default",
		},
		Spec: natsv1.TeamServiceAccountSpec{
			TeamRef: &natsv1.TeamRef{Name: "eng-team"},
			Name:    "bot",
		},
	}

	r, _ := setupTeamReconciler(t, team, tsa)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: team.Name, Namespace: team.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != requeueWaitingForResource.RequeueAfter {
		t.Fatalf("expected waiting requeue, got %v", result.RequeueAfter)
	}

	var got natsv1.Team
	if err := r.Get(context.Background(), types.NamespacedName{Name: team.Name, Namespace: team.Namespace}, &got); err != nil {
		t.Fatalf("get team: %v", err)
	}
	if !strings.Contains(got.Status.Message, "waiting for dependent resources") {
		t.Fatalf("expected dependent resources message, got %q", got.Status.Message)
	}
}

func TestTeamReconcileDeletionBlockedByTargetRoleBinding(t *testing.T) {
	now := metav1.Now()
	team := &natsv1.Team{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "eng-team",
			Namespace:         "default",
			Finalizers:        []string{teamFinalizer},
			DeletionTimestamp: &now,
		},
		Status: natsv1.TeamStatus{
			ID: "T-1",
		},
	}
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "team-binding",
			Namespace: "default",
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			TeamAppUserID: "TAU-1",
			Scope:         natsv1.ScopeTeam,
			TargetRef:     &natsv1.AppUserRoleBindingTargetRef{Name: "eng-team"},
			RoleID:        "R-1",
		},
	}

	r, _ := setupTeamReconciler(t, team, binding)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: team.Name, Namespace: team.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != requeueWaitingForResource.RequeueAfter {
		t.Fatalf("expected waiting requeue, got %v", result.RequeueAfter)
	}

	var got natsv1.Team
	if err := r.Get(context.Background(), types.NamespacedName{Name: team.Name, Namespace: team.Namespace}, &got); err != nil {
		t.Fatalf("get team: %v", err)
	}
	if !strings.Contains(got.Status.Message, "waiting for dependent resources") {
		t.Fatalf("expected dependent resources message, got %q", got.Status.Message)
	}
}
