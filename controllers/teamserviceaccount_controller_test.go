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

func setupTeamServiceAccountReconciler(t *testing.T, objs ...client.Object) (*TeamServiceAccountReconciler, *fakeControlPlaneClient) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := natsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	fcp := &fakeControlPlaneClient{}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&natsv1.TeamServiceAccount{}, &natsv1.Team{}, &natsv1.AppUserRoleBinding{}).
		WithObjects(objs...).
		Build()

	return &TeamServiceAccountReconciler{
		Client:       c,
		Scheme:       scheme,
		ControlPlane: fcp,
	}, fcp
}

func TestTeamServiceAccountReconcileDeletionBlockedBySubjectRoleBinding(t *testing.T) {
	now := metav1.Now()
	tsa := &natsv1.TeamServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "bot",
			Namespace:         "default",
			Finalizers:        []string{teamServiceAccountFinalizer},
			DeletionTimestamp: &now,
		},
		Status: natsv1.TeamServiceAccountStatus{
			ID: "SA-1",
		},
	}
	binding := &natsv1.AppUserRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bot-binding",
			Namespace: "default",
		},
		Spec: natsv1.AppUserRoleBindingSpec{
			SubjectRef: &natsv1.AppUserRoleBindingSubjectRef{Name: "bot"},
			Scope:      natsv1.ScopeAccount,
			TargetID:   "A-1",
			RoleID:     "R-1",
		},
	}

	r, _ := setupTeamServiceAccountReconciler(t, tsa, binding)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: tsa.Name, Namespace: tsa.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != requeueWaitingForResource.RequeueAfter {
		t.Fatalf("expected waiting requeue, got %v", result.RequeueAfter)
	}

	var got natsv1.TeamServiceAccount
	if err := r.Get(context.Background(), types.NamespacedName{Name: tsa.Name, Namespace: tsa.Namespace}, &got); err != nil {
		t.Fatalf("get team service account: %v", err)
	}
	if !strings.Contains(got.Status.Message, "waiting for dependent AppUserRoleBindings") {
		t.Fatalf("expected dependent role binding message, got %q", got.Status.Message)
	}
}
