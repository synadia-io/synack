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

	// If we already know the ID, use it directly — never fall through to create.
	if in.NatsUserID != "" {
		name := in.Name
		updateReq := syncp.NatsUserUpdateRequest{
			Name:             &name,
			JwtExpiresInSecs: in.Spec.JwtExpiresInSecs,
		}

		updated, _, err := c.api.NatsUserAPI.UpdateNatsUser(authCtx, in.NatsUserID).NatsUserUpdateRequest(updateReq).Execute()
		if err != nil {
			return NatsUserResult{}, fmt.Errorf("update nats user by id %q: %w", in.NatsUserID, err)
		}
		l.Info("nats user updated", "resourceID", updated.Id, "accountID", accountID)
		return NatsUserResult{
			NatsUserID:    updated.Id,
			AccountID:     accountID,
			UserPublicKey: updated.UserPublicKey,
		}, nil
	}

	// List and match by name
	list, _, err := c.api.AccountAPI.ListUsers(authCtx, accountID).Execute()
	if err != nil {
		return NatsUserResult{}, fmt.Errorf("list nats users: %w", err)
	}

	for _, u := range list.Items {
		if u.Name == in.Name {
			// Found existing user - update it
			name := in.Name
			updateReq := syncp.NatsUserUpdateRequest{
				Name:             &name,
				JwtExpiresInSecs: in.Spec.JwtExpiresInSecs,
			}

			updated, _, err := c.api.NatsUserAPI.UpdateNatsUser(authCtx, u.Id).NatsUserUpdateRequest(updateReq).Execute()
			if err != nil {
				return NatsUserResult{}, fmt.Errorf("update nats user %q: %w", u.Id, err)
			}

			l.Info("nats user updated (by name match)", "resourceID", updated.Id, "accountID", accountID)
			return NatsUserResult{
				NatsUserID:    updated.Id,
				AccountID:     accountID,
				UserPublicKey: updated.UserPublicKey,
			}, nil
		}
	}

	// Create new user
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
		return NatsUserResult{}, fmt.Errorf("create nats user %q: %w", in.Name, err)
	}

	l.Info("nats user created", "resourceID", created.Id, "accountID", accountID)
	return NatsUserResult{
		NatsUserID:    created.Id,
		AccountID:     accountID,
		UserPublicKey: created.UserPublicKey,
	}, nil
}

func (c *client) DeleteNatsUser(ctx context.Context, in NatsUserInput) error {
	l := log.FromContext(ctx).WithValues("resourceType", "natsUser", "resourceName", in.Name)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	userID := in.NatsUserID
	if userID == "" {
		accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
		if err != nil {
			return err
		}

		list, _, err := c.api.AccountAPI.ListUsers(authCtx, accountID).Execute()
		if err != nil {
			return fmt.Errorf("list nats users for delete: %w", err)
		}

		found := make([]string, 0)
		for _, u := range list.Items {
			if u.Name == in.Name {
				found = append(found, u.Id)
			}
		}

		if len(found) == 0 {
			return nil
		}

		if len(found) > 1 {
			return fmt.Errorf("multiple nats users found with name %q in account %q: %s", in.Name, accountID, strings.Join(found, ", "))
		}

		userID = found[0]
	}

	_, err = c.api.NatsUserAPI.DeleteNatsUser(authCtx, userID).Execute()
	if err == nil || isStatusCode(err, http.StatusNotFound) {
		l.Info("nats user deleted", "resourceID", userID)
		return nil
	}

	return fmt.Errorf("delete nats user %q: %w", userID, err)
}

func (c *client) ReadNatsUserState(ctx context.Context, in NatsUserInput) ([]byte, bool, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, false, err
	}

	if in.NatsUserID != "" {
		user, _, err := c.api.NatsUserAPI.GetNatsUser(authCtx, in.NatsUserID).Execute()
		if err != nil {
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

	accountID, err := c.resolveAccountID(authCtx, in.AccountSelectors)
	if err != nil {
		return nil, false, err
	}

	list, _, err := c.api.AccountAPI.ListUsers(authCtx, accountID).Execute()
	if err != nil {
		return nil, false, fmt.Errorf("list nats users: %w", err)
	}

	for _, u := range list.Items {
		if u.Name != in.Name {
			continue
		}
		state, err := json.Marshal(u)
		if err != nil {
			return nil, false, err
		}
		return state, true, nil
	}

	return nil, false, nil
}
