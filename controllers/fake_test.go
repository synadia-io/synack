package controllers

import (
	"context"

	"github.com/synadia-io/synack/internal/controlplane"
)

type fakeControlPlaneClient struct {
	ensureStreamFn  func(ctx context.Context, in controlplane.StreamInput) (controlplane.StreamResult, error)
	ensureStreamIn  []controlplane.StreamInput
	ensureStreamHit int

	ensureKeyValueFn  func(ctx context.Context, in controlplane.KeyValueInput) (controlplane.KeyValueResult, error)
	ensureKeyValueIn  []controlplane.KeyValueInput
	ensureKeyValueHit int

	ensureObjectStoreFn  func(ctx context.Context, in controlplane.ObjectStoreInput) (controlplane.ObjectStoreResult, error)
	ensureObjectStoreIn  []controlplane.ObjectStoreInput
	ensureObjectStoreHit int

	ensureConsumerFn  func(ctx context.Context, in controlplane.ConsumerInput) (controlplane.ConsumerResult, error)
	ensureConsumerIn  []controlplane.ConsumerInput
	ensureConsumerHit int

	ensureNatsUserFn  func(ctx context.Context, in controlplane.NatsUserInput) (controlplane.NatsUserResult, error)
	ensureNatsUserIn  []controlplane.NatsUserInput
	ensureNatsUserHit int

	downloadNatsUserCredsFn  func(ctx context.Context, natsUserID string) (string, error)
	downloadNatsUserCredsIn  []string
	downloadNatsUserCredsHit int

	ensureAccountFn  func(ctx context.Context, in controlplane.AccountInput) (controlplane.AccountResult, error)
	ensureAccountIn  []controlplane.AccountInput
	ensureAccountHit int

	ensureTeamFn  func(ctx context.Context, in controlplane.TeamInput) (controlplane.TeamResult, error)
	ensureTeamIn  []controlplane.TeamInput
	ensureTeamHit int

	ensureTeamServiceAccountFn  func(ctx context.Context, in controlplane.TeamServiceAccountInput) (controlplane.TeamServiceAccountResult, error)
	ensureTeamServiceAccountIn  []controlplane.TeamServiceAccountInput
	ensureTeamServiceAccountHit int

	ensureAppUserRoleBindingFn  func(ctx context.Context, in controlplane.AppUserRoleBindingInput) (controlplane.AppUserRoleBindingResult, error)
	ensureAppUserRoleBindingIn  []controlplane.AppUserRoleBindingInput
	ensureAppUserRoleBindingHit int
}

func (f *fakeControlPlaneClient) ValidateToken(context.Context) error {
	return nil
}

func (f *fakeControlPlaneClient) EnsureStream(ctx context.Context, in controlplane.StreamInput) (controlplane.StreamResult, error) {
	f.ensureStreamHit++
	f.ensureStreamIn = append(f.ensureStreamIn, in)
	if f.ensureStreamFn != nil {
		return f.ensureStreamFn(ctx, in)
	}
	return controlplane.StreamResult{}, nil
}

func (f *fakeControlPlaneClient) EnsureAccount(ctx context.Context, in controlplane.AccountInput) (controlplane.AccountResult, error) {
	f.ensureAccountHit++
	f.ensureAccountIn = append(f.ensureAccountIn, in)
	if f.ensureAccountFn != nil {
		return f.ensureAccountFn(ctx, in)
	}
	return controlplane.AccountResult{}, nil
}

func (f *fakeControlPlaneClient) DeleteStream(_ context.Context, _ controlplane.StreamInput) error {
	return nil
}

func (f *fakeControlPlaneClient) ReadStreamState(_ context.Context, _ controlplane.StreamInput) ([]byte, bool, error) {
	return []byte(`{}`), true, nil
}

func (f *fakeControlPlaneClient) DeleteAccount(_ context.Context, _ controlplane.AccountInput) error {
	return nil
}

func (f *fakeControlPlaneClient) ReadAccountState(_ context.Context, _ controlplane.AccountInput) ([]byte, bool, error) {
	return []byte(`{}`), true, nil
}

func (f *fakeControlPlaneClient) EnsureKeyValue(ctx context.Context, in controlplane.KeyValueInput) (controlplane.KeyValueResult, error) {
	f.ensureKeyValueHit++
	f.ensureKeyValueIn = append(f.ensureKeyValueIn, in)
	if f.ensureKeyValueFn != nil {
		return f.ensureKeyValueFn(ctx, in)
	}
	return controlplane.KeyValueResult{}, nil
}

func (f *fakeControlPlaneClient) DeleteKeyValue(_ context.Context, _ controlplane.KeyValueInput) error {
	return nil
}

func (f *fakeControlPlaneClient) ReadKeyValueState(_ context.Context, _ controlplane.KeyValueInput) ([]byte, bool, error) {
	return []byte(`{}`), true, nil
}

func (f *fakeControlPlaneClient) EnsureObjectStore(ctx context.Context, in controlplane.ObjectStoreInput) (controlplane.ObjectStoreResult, error) {
	f.ensureObjectStoreHit++
	f.ensureObjectStoreIn = append(f.ensureObjectStoreIn, in)
	if f.ensureObjectStoreFn != nil {
		return f.ensureObjectStoreFn(ctx, in)
	}
	return controlplane.ObjectStoreResult{}, nil
}

func (f *fakeControlPlaneClient) DeleteObjectStore(_ context.Context, _ controlplane.ObjectStoreInput) error {
	return nil
}

func (f *fakeControlPlaneClient) ReadObjectStoreState(_ context.Context, _ controlplane.ObjectStoreInput) ([]byte, bool, error) {
	return []byte(`{}`), true, nil
}

func (f *fakeControlPlaneClient) EnsureConsumer(ctx context.Context, in controlplane.ConsumerInput) (controlplane.ConsumerResult, error) {
	f.ensureConsumerHit++
	f.ensureConsumerIn = append(f.ensureConsumerIn, in)
	if f.ensureConsumerFn != nil {
		return f.ensureConsumerFn(ctx, in)
	}
	return controlplane.ConsumerResult{}, nil
}

func (f *fakeControlPlaneClient) DeleteConsumer(_ context.Context, _ controlplane.ConsumerInput) error {
	return nil
}

func (f *fakeControlPlaneClient) ReadConsumerState(_ context.Context, _ controlplane.ConsumerInput) ([]byte, bool, error) {
	return []byte(`{}`), true, nil
}

func (f *fakeControlPlaneClient) ResolveSigningKeyGroupID(_ context.Context, _, skGroupID string) (string, error) {
	return skGroupID, nil
}

func (f *fakeControlPlaneClient) EnsureNatsUser(ctx context.Context, in controlplane.NatsUserInput) (controlplane.NatsUserResult, error) {
	f.ensureNatsUserHit++
	f.ensureNatsUserIn = append(f.ensureNatsUserIn, in)
	if f.ensureNatsUserFn != nil {
		return f.ensureNatsUserFn(ctx, in)
	}
	return controlplane.NatsUserResult{}, nil
}

func (f *fakeControlPlaneClient) DeleteNatsUser(_ context.Context, _ controlplane.NatsUserInput) error {
	return nil
}

func (f *fakeControlPlaneClient) ReadNatsUserState(_ context.Context, _ controlplane.NatsUserInput) ([]byte, bool, error) {
	return []byte(`{}`), true, nil
}

func (f *fakeControlPlaneClient) DownloadNatsUserCreds(ctx context.Context, natsUserID string) (string, error) {
	f.downloadNatsUserCredsHit++
	f.downloadNatsUserCredsIn = append(f.downloadNatsUserCredsIn, natsUserID)
	if f.downloadNatsUserCredsFn != nil {
		return f.downloadNatsUserCredsFn(ctx, natsUserID)
	}
	return "", nil
}

func (f *fakeControlPlaneClient) EnsureTeam(ctx context.Context, in controlplane.TeamInput) (controlplane.TeamResult, error) {
	f.ensureTeamHit++
	f.ensureTeamIn = append(f.ensureTeamIn, in)
	if f.ensureTeamFn != nil {
		return f.ensureTeamFn(ctx, in)
	}
	return controlplane.TeamResult{}, nil
}

func (f *fakeControlPlaneClient) DeleteTeam(_ context.Context, _ controlplane.TeamInput) error {
	return nil
}

func (f *fakeControlPlaneClient) ReadTeamState(_ context.Context, _ controlplane.TeamInput) ([]byte, bool, error) {
	return []byte(`{}`), true, nil
}

func (f *fakeControlPlaneClient) EnsureTeamServiceAccount(ctx context.Context, in controlplane.TeamServiceAccountInput) (controlplane.TeamServiceAccountResult, error) {
	f.ensureTeamServiceAccountHit++
	f.ensureTeamServiceAccountIn = append(f.ensureTeamServiceAccountIn, in)
	if f.ensureTeamServiceAccountFn != nil {
		return f.ensureTeamServiceAccountFn(ctx, in)
	}
	return controlplane.TeamServiceAccountResult{}, nil
}

func (f *fakeControlPlaneClient) DeleteTeamServiceAccount(_ context.Context, _ controlplane.TeamServiceAccountInput) error {
	return nil
}

func (f *fakeControlPlaneClient) ReadTeamServiceAccountState(_ context.Context, _ controlplane.TeamServiceAccountInput) ([]byte, bool, error) {
	return []byte(`{}`), true, nil
}

func (f *fakeControlPlaneClient) EnsureAppUserRoleBinding(ctx context.Context, in controlplane.AppUserRoleBindingInput) (controlplane.AppUserRoleBindingResult, error) {
	f.ensureAppUserRoleBindingHit++
	f.ensureAppUserRoleBindingIn = append(f.ensureAppUserRoleBindingIn, in)
	if f.ensureAppUserRoleBindingFn != nil {
		return f.ensureAppUserRoleBindingFn(ctx, in)
	}
	return controlplane.AppUserRoleBindingResult{}, nil
}

func (f *fakeControlPlaneClient) DeleteAppUserRoleBinding(_ context.Context, _ controlplane.AppUserRoleBindingInput) error {
	return nil
}

func (f *fakeControlPlaneClient) ReadAppUserRoleBindingState(_ context.Context, _ controlplane.AppUserRoleBindingInput) ([]byte, bool, error) {
	return []byte(`{}`), true, nil
}
