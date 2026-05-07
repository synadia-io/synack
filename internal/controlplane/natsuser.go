package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/synadia-io/control-plane-sdk-go/syncp"
	natsv1 "github.com/synadia-io/synack/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type NatsUserInput struct {
	AccountSelectors

	NatsUserID        string
	Name              string
	SigningKeyGroupID string
	Spec              natsv1.NatsUserSpec
}

type NatsUserResult struct {
	NatsUserID    string
	AccountID     string
	UserPublicKey string
}

func (c *client) EnsureNatsUser(ctx context.Context, in NatsUserInput) (NatsUserResult, error) {
	l := log.FromContext(ctx).WithValues("resourceType", "natsUser", "resourceName", in.Name)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return NatsUserResult{}, err
	}

	accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
	if err != nil {
		return NatsUserResult{}, err
	}

	// If we already know the ID use it directly, don't fall through to create.
	if in.NatsUserID != "" {
		existing, _, err := c.api.NatsUserAPI.GetNatsUser(authCtx, in.NatsUserID).Execute()
		if err != nil {
			err = withAPIError(err)
			if isStatusCode(err, http.StatusNotFound) {
				l.Info("known nats user ID not found, creating new nats user", "resourceID", in.NatsUserID)
				in.NatsUserID = ""
			} else {
				return NatsUserResult{}, fmt.Errorf("get nats user by id %q: %w", in.NatsUserID, err)
			}
		} else if err := validateNatsUserSigningKeyGroup(existing, in.SigningKeyGroupID); err != nil {
			return NatsUserResult{}, fmt.Errorf("nats user %q: %w", in.NatsUserID, err)
		} else {
			updated, _, err := c.api.NatsUserAPI.UpdateNatsUser(authCtx, in.NatsUserID).NatsUserUpdateRequest(natsUserUpdateRequest(in)).Execute()
			if err != nil {
				err = withAPIError(err)
				return NatsUserResult{}, fmt.Errorf("update nats user by id %q: %w", in.NatsUserID, err)
			}
			l.Info("nats user updated", "resourceID", updated.Id, "accountID", accountID)
			return NatsUserResult{
				NatsUserID:    updated.Id,
				AccountID:     accountID,
				UserPublicKey: updated.UserPublicKey,
			}, nil
		}
	}

	createReq := syncp.NatsUserCreateRequest{
		Name:             in.Name,
		SkGroupId:        in.SigningKeyGroupID,
		JwtExpiresInSecs: in.Spec.JwtExpiresInSecs,
	}

	if in.Spec.BearerToken != nil || in.Spec.Data != nil || in.Spec.Payload != nil ||
		in.Spec.Subs != nil || len(in.Spec.AllowedConnectionTypes) > 0 || len(in.Spec.Tags) > 0 {
		createReq.JwtSettings = &syncp.NatsCreateUserJwtSettings{
			BearerToken:            in.Spec.BearerToken,
			Data:                   in.Spec.Data,
			Payload:                in.Spec.Payload,
			Subs:                   in.Spec.Subs,
			AllowedConnectionTypes: in.Spec.AllowedConnectionTypes,
			Tags:                   in.Spec.Tags,
		}
	}

	created, _, err := c.api.AccountAPI.CreateUser(authCtx, accountID).NatsUserCreateRequest(createReq).Execute()
	if err != nil {
		err = withAPIError(err)
		return NatsUserResult{}, fmt.Errorf("create nats user %q: %w", in.Name, err)
	}

	l.Info("nats user created", "resourceID", created.Id, "accountID", accountID)
	return NatsUserResult{
		NatsUserID:    created.Id,
		AccountID:     accountID,
		UserPublicKey: created.UserPublicKey,
	}, nil
}

func natsUserUpdateRequest(in NatsUserInput) syncp.NatsUserUpdateRequest {
	name := in.Name
	updateReq := syncp.NatsUserUpdateRequest{
		Name:             &name,
		JwtExpiresInSecs: in.Spec.JwtExpiresInSecs,
	}

	if jwtSettings := natsUserJwtSettingsPatch(in.Spec); jwtSettings != nil {
		updateReq.JwtSettings = jwtSettings
	}

	return updateReq
}

func natsUserJwtSettingsPatch(spec natsv1.NatsUserSpec) *syncp.NatsUserJwtSettingsPatch {
	if spec.BearerToken == nil && spec.Data == nil && spec.Payload == nil && spec.Subs == nil &&
		spec.AllowedConnectionTypes == nil && spec.Tags == nil {
		return nil
	}

	return &syncp.NatsUserJwtSettingsPatch{
		BearerToken:            spec.BearerToken,
		Data:                   spec.Data,
		Payload:                spec.Payload,
		Subs:                   spec.Subs,
		AllowedConnectionTypes: spec.AllowedConnectionTypes,
		Tags:                   spec.Tags,
	}
}

func validateNatsUserSigningKeyGroup(user *syncp.NatsUserViewResponse, desiredID string) error {
	if desiredID == "" {
		return nil
	}
	if user.SkGroupId != nil && *user.SkGroupId == desiredID {
		return nil
	}
	actualID := ""
	if user.SkGroupId != nil {
		actualID = *user.SkGroupId
	}
	return fmt.Errorf("signing key group is %q, want %q; updating signing key group for an existing user is not currently supported by the Control Plane API", actualID, desiredID)
}

func (c *client) DeleteNatsUser(ctx context.Context, in NatsUserInput) error {
	l := log.FromContext(ctx).WithValues("resourceType", "natsUser", "resourceName", in.Name)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	if in.NatsUserID == "" {
		return nil
	}

	_, err = c.api.NatsUserAPI.DeleteNatsUser(authCtx, in.NatsUserID).Execute()
	if err == nil || isStatusCode(err, http.StatusNotFound) {
		l.Info("nats user deleted", "resourceID", in.NatsUserID)
		return nil
	}
	err = withAPIError(err)

	return fmt.Errorf("delete nats user %q: %w", in.NatsUserID, err)
}

func (c *client) ReadNatsUserState(ctx context.Context, in NatsUserInput) ([]byte, bool, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, false, err
	}

	if in.NatsUserID != "" {
		user, _, err := c.api.NatsUserAPI.GetNatsUser(authCtx, in.NatsUserID).Execute()
		if err != nil {
			err = withAPIError(err)
			if isStatusCode(err, http.StatusNotFound) {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("get nats user by id %q: %w", in.NatsUserID, err)
		}
		state, err := json.Marshal(user)
		if err != nil {
			return nil, false, err
		}
		return state, true, nil
	}

	return nil, false, nil
}

func (c *client) DownloadNatsUserCreds(ctx context.Context, natsUserID string) (string, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return "", err
	}

	creds, _, err := c.api.NatsUserAPI.DownloadNatsUserCreds(authCtx, natsUserID).Execute()
	if err != nil {
		err = withAPIError(err)
		return "", fmt.Errorf("download nats user creds %q: %w", natsUserID, err)
	}

	return creds, nil
}

func (c *client) ResolveSigningKeyGroupID(ctx context.Context, accountID, skGroupID string) (string, error) {
	if skGroupID != "" && !strings.EqualFold(skGroupID, "default") {
		return skGroupID, nil
	}

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return "", err
	}

	list, _, err := c.api.AccountAPI.ListAccountSkGroup(authCtx, accountID).Execute()
	if err != nil {
		err = withAPIError(err)
		return "", fmt.Errorf("list signing key groups for account %q: %w", accountID, err)
	}

	for _, skg := range list.Items {
		if strings.EqualFold(skg.Name, "Default") {
			return skg.Id, nil
		}
	}

	return "", fmt.Errorf("no default signing key group found for account %q", accountID)
}
