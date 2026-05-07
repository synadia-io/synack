package controllers

import (
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	natsv1 "github.com/synadia-io/synack/api/v1alpha1"
	"github.com/synadia-io/synack/internal/controlplane"
)

func setupNatsUserReconciler(t *testing.T, objs ...client.Object) (*NatsUserReconciler, *fakeControlPlaneClient) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := natsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add synack scheme: %v", err)
	}

	fcp := &fakeControlPlaneClient{}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&natsv1.NatsUser{}).
		WithObjects(objs...).
		Build()

	return &NatsUserReconciler{
		Client:       c,
		Scheme:       scheme,
		ControlPlane: fcp,
	}, fcp
}

func TestNatsUserReconcileCreatesCredentialsSecret(t *testing.T) {
	user := &natsv1.NatsUser{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "app-user",
			Namespace:  "default",
			Finalizers: []string{natsUserFinalizer},
			UID:        types.UID("user-uid-1"),
		},
		Spec: natsv1.NatsUserSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-123",
			},
			Name: "app-user",
			CredentialsSecret: &natsv1.NatsUserCredentialsSecret{
				Name: "app-user-creds",
			},
		},
	}

	r, fcp := setupNatsUserReconciler(t, user)
	fcp.ensureNatsUserFn = func(_ context.Context, _ controlplane.NatsUserInput) (controlplane.NatsUserResult, error) {
		return controlplane.NatsUserResult{
			NatsUserID:    "U-123",
			AccountID:     "A-123",
			UserPublicKey: "UP-123",
		}, nil
	}
	fcp.downloadNatsUserCredsFn = func(_ context.Context, natsUserID string) (string, error) {
		if natsUserID != "U-123" {
			return "", fmt.Errorf("unexpected nats user id %q", natsUserID)
		}
		return "NATS_CREDS_DATA", nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: user.Name, Namespace: user.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var secret corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Name: "app-user-creds", Namespace: "default"}, &secret); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if got := string(secret.Data["creds"]); got != "NATS_CREDS_DATA" {
		t.Fatalf("unexpected secret creds data: %q", got)
	}
	if len(secret.OwnerReferences) == 0 || secret.OwnerReferences[0].Name != user.Name {
		t.Fatalf("expected natsuser owner reference, got %+v", secret.OwnerReferences)
	}

	var gotUser natsv1.NatsUser
	if err := r.Get(context.Background(), types.NamespacedName{Name: user.Name, Namespace: user.Namespace}, &gotUser); err != nil {
		t.Fatalf("get nats user: %v", err)
	}
	if gotUser.Status.CredentialsSecretName != "app-user-creds" {
		t.Fatalf("unexpected credentialsSecretName: %q", gotUser.Status.CredentialsSecretName)
	}
	if gotUser.Status.CredentialsLastSynced == "" {
		t.Fatalf("expected credentialsLastSynced to be set")
	}
	if gotUser.Status.Message != messageApplied {
		t.Fatalf("expected status message %q, got %q", messageApplied, gotUser.Status.Message)
	}
}

func TestNatsUserReconcileUpdatesSecretAndNoopsWhenUnchanged(t *testing.T) {
	user := &natsv1.NatsUser{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "app-user",
			Namespace:  "default",
			Finalizers: []string{natsUserFinalizer},
			UID:        types.UID("user-uid-2"),
		},
		Spec: natsv1.NatsUserSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-123",
			},
			Name: "app-user",
			CredentialsSecret: &natsv1.NatsUserCredentialsSecret{
				Name: "app-user-creds",
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-user-creds",
			Namespace: "default",
			Annotations: map[string]string{
				natsUserSecretUIDAnnotation: string(user.UID),
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"creds": []byte("OLD_DATA"),
		},
	}

	r, fcp := setupNatsUserReconciler(t, user, secret)
	fcp.ensureNatsUserFn = func(_ context.Context, _ controlplane.NatsUserInput) (controlplane.NatsUserResult, error) {
		return controlplane.NatsUserResult{NatsUserID: "U-123", AccountID: "A-123"}, nil
	}
	fcp.downloadNatsUserCredsFn = func(_ context.Context, _ string) (string, error) {
		return "NEW_DATA", nil
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: user.Name, Namespace: user.Namespace}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	var updated corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Name: "app-user-creds", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get updated secret: %v", err)
	}
	if got := string(updated.Data["creds"]); got != "NEW_DATA" {
		t.Fatalf("expected updated creds data, got %q", got)
	}
	rv := updated.ResourceVersion

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	var unchanged corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Name: "app-user-creds", Namespace: "default"}, &unchanged); err != nil {
		t.Fatalf("get unchanged secret: %v", err)
	}
	if unchanged.ResourceVersion != rv {
		t.Fatalf("expected secret resourceVersion unchanged, old=%q new=%q", rv, unchanged.ResourceVersion)
	}
}

func TestNatsUserReconcileRefusesUnownedCredentialsSecret(t *testing.T) {
	user := &natsv1.NatsUser{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "app-user",
			Namespace:  "default",
			Finalizers: []string{natsUserFinalizer},
			UID:        types.UID("user-uid-unowned-secret"),
		},
		Spec: natsv1.NatsUserSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-123",
			},
			Name: "app-user",
			CredentialsSecret: &natsv1.NatsUserCredentialsSecret{
				Name: "app-user-creds",
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-user-creds",
			Namespace: "default",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"creds": []byte("DO_NOT_OVERWRITE"),
		},
	}

	r, fcp := setupNatsUserReconciler(t, user, secret)
	fcp.ensureNatsUserFn = func(_ context.Context, _ controlplane.NatsUserInput) (controlplane.NatsUserResult, error) {
		return controlplane.NatsUserResult{NatsUserID: "U-123", AccountID: "A-123"}, nil
	}
	fcp.downloadNatsUserCredsFn = func(_ context.Context, _ string) (string, error) {
		return "NEW_DATA", nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: user.Name, Namespace: user.Namespace},
	})
	if err == nil || !strings.Contains(err.Error(), "already exists and is not owned") {
		t.Fatalf("expected ownership error, got %v", err)
	}

	var unchanged corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Name: "app-user-creds", Namespace: "default"}, &unchanged); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if got := string(unchanged.Data["creds"]); got != "DO_NOT_OVERWRITE" {
		t.Fatalf("expected pre-existing data to be preserved, got %q", got)
	}
	if len(unchanged.OwnerReferences) != 0 {
		t.Fatalf("expected no owner references to be added, got %+v", unchanged.OwnerReferences)
	}
	if unchanged.Annotations[natsUserSecretUIDAnnotation] != "" {
		t.Fatalf("expected no nats user UID annotation to be added, got %q", unchanged.Annotations[natsUserSecretUIDAnnotation])
	}

	var gotUser natsv1.NatsUser
	if err := r.Get(context.Background(), types.NamespacedName{Name: user.Name, Namespace: user.Namespace}, &gotUser); err != nil {
		t.Fatalf("get nats user: %v", err)
	}
	if !strings.Contains(gotUser.Status.Message, "already exists and is not owned") {
		t.Fatalf("expected ownership error in status, got %q", gotUser.Status.Message)
	}
}

func TestNatsUserReconcileSetsStatusMessageWhenCredsDownloadFails(t *testing.T) {
	user := &natsv1.NatsUser{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "app-user",
			Namespace:  "default",
			Finalizers: []string{natsUserFinalizer},
		},
		Spec: natsv1.NatsUserSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-123",
			},
			Name: "app-user",
			CredentialsSecret: &natsv1.NatsUserCredentialsSecret{
				Name: "app-user-creds",
			},
		},
	}

	r, fcp := setupNatsUserReconciler(t, user)
	fcp.ensureNatsUserFn = func(_ context.Context, _ controlplane.NatsUserInput) (controlplane.NatsUserResult, error) {
		return controlplane.NatsUserResult{NatsUserID: "U-123", AccountID: "A-123"}, nil
	}
	fcp.downloadNatsUserCredsFn = func(_ context.Context, _ string) (string, error) {
		return "", fmt.Errorf("credential download failed")
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: user.Name, Namespace: user.Namespace},
	})
	if err == nil || !strings.Contains(err.Error(), "credential download failed") {
		t.Fatalf("expected credentials error, got %v", err)
	}

	var got natsv1.NatsUser
	if err := r.Get(context.Background(), types.NamespacedName{Name: user.Name, Namespace: user.Namespace}, &got); err != nil {
		t.Fatalf("get nats user: %v", err)
	}
	if !strings.Contains(got.Status.Message, "credential download failed") {
		t.Fatalf("expected credentials error in status, got %q", got.Status.Message)
	}

	var secret corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Name: "app-user-creds", Namespace: "default"}, &secret); err == nil {
		t.Fatalf("expected no secret to be created on download failure")
	}
}

func TestNatsUserReconcileDeletesOldCredentialsSecretOnRename(t *testing.T) {
	user := &natsv1.NatsUser{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "app-user",
			Namespace:  "default",
			Finalizers: []string{natsUserFinalizer},
			UID:        types.UID("user-uid-3"),
		},
		Spec: natsv1.NatsUserSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-123",
			},
			Name: "app-user",
			CredentialsSecret: &natsv1.NatsUserCredentialsSecret{
				Name: "new-creds",
			},
		},
		Status: natsv1.NatsUserStatus{
			CredentialsSecretName: "old-creds",
		},
	}
	oldSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "old-creds",
			Namespace: "default",
			Annotations: map[string]string{
				natsUserSecretUIDAnnotation: string(user.UID),
			},
		},
		Data: map[string][]byte{
			"creds": []byte("OLD_DATA"),
		},
	}

	r, fcp := setupNatsUserReconciler(t, user, oldSecret)
	fcp.ensureNatsUserFn = func(_ context.Context, _ controlplane.NatsUserInput) (controlplane.NatsUserResult, error) {
		return controlplane.NatsUserResult{NatsUserID: "U-123", AccountID: "A-123"}, nil
	}
	fcp.downloadNatsUserCredsFn = func(_ context.Context, _ string) (string, error) {
		return "NEW_DATA", nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: user.Name, Namespace: user.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var old corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Name: "old-creds", Namespace: "default"}, &old); err == nil {
		t.Fatalf("expected old credentials secret to be deleted")
	}

	var newSecret corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Name: "new-creds", Namespace: "default"}, &newSecret); err != nil {
		t.Fatalf("expected new credentials secret to be created: %v", err)
	}
}

func TestNatsUserReconcileDeletesOldCredentialsSecretWhenDisabled(t *testing.T) {
	user := &natsv1.NatsUser{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "app-user",
			Namespace:  "default",
			Finalizers: []string{natsUserFinalizer},
			UID:        types.UID("user-uid-4"),
		},
		Spec: natsv1.NatsUserSpec{
			AccountSelector: natsv1.AccountSelector{
				AccountID: "A-123",
			},
			Name: "app-user",
		},
		Status: natsv1.NatsUserStatus{
			CredentialsSecretName: "app-user-creds",
		},
	}
	oldSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-user-creds",
			Namespace: "default",
			Annotations: map[string]string{
				natsUserSecretUIDAnnotation: string(user.UID),
			},
		},
		Data: map[string][]byte{
			"creds": []byte("OLD_DATA"),
		},
	}

	r, fcp := setupNatsUserReconciler(t, user, oldSecret)
	fcp.ensureNatsUserFn = func(_ context.Context, _ controlplane.NatsUserInput) (controlplane.NatsUserResult, error) {
		return controlplane.NatsUserResult{NatsUserID: "U-123", AccountID: "A-123"}, nil
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: user.Name, Namespace: user.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var old corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Name: "app-user-creds", Namespace: "default"}, &old); err == nil {
		t.Fatalf("expected old credentials secret to be deleted when feature disabled")
	}
}
