package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/synadia-io/control-plane-sdk-go/syncp"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type RoleBindingScope string

const (
	RoleBindingScopeTeam     RoleBindingScope = "Team"
	RoleBindingScopeSystem   RoleBindingScope = "System"
	RoleBindingScopeAccount  RoleBindingScope = "Account"
	RoleBindingScopeNatsUser RoleBindingScope = "NatsUser"
)

type AppUserRoleBindingInput struct {
	TeamAppUserID string
	Scope         RoleBindingScope
	TargetID      string
	RoleID        string
}

type AppUserRoleBindingResult struct {
	Bound bool
}

func (c *client) EnsureAppUserRoleBinding(ctx context.Context, in AppUserRoleBindingInput) (AppUserRoleBindingResult, error) {
	l := log.FromContext(ctx).WithValues("resourceType", "appUserRoleBinding", "scope", in.Scope, "targetId", in.TargetID, "teamAppUserId", in.TeamAppUserID)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return AppUserRoleBindingResult{}, err
	}

	// Check if already assigned by reading current state.
	existing, found, err := c.readRoleBindingForScope(authCtx, in)
	if err != nil {
		return AppUserRoleBindingResult{}, err
	}

	if found {
		if existing.RoleId == in.RoleID {
			return AppUserRoleBindingResult{Bound: true}, nil
		}

		// Role changed, unassign and re-assign.
		if err := c.unassignForScope(authCtx, in); err != nil && !isStatusCode(err, http.StatusNotFound) {
			return AppUserRoleBindingResult{}, fmt.Errorf("unassign for role update: %w", err)
		}
	}

	if err := c.assignForScope(authCtx, in); err != nil {
		return AppUserRoleBindingResult{}, fmt.Errorf("assign %s role binding: %w", in.Scope, err)
	}

	l.Info("app user role binding applied", "roleId", in.RoleID)
	return AppUserRoleBindingResult{Bound: true}, nil
}

func (c *client) DeleteAppUserRoleBinding(ctx context.Context, in AppUserRoleBindingInput) error {
	l := log.FromContext(ctx).WithValues("resourceType", "appUserRoleBinding", "scope", in.Scope, "targetId", in.TargetID)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	err = c.unassignForScope(authCtx, in)
	if err == nil || isStatusCode(err, http.StatusNotFound) {
		l.Info("app user role binding deleted")
		return nil
	}

	return fmt.Errorf("delete %s role binding: %w", in.Scope, err)
}

func (c *client) ReadAppUserRoleBindingState(ctx context.Context, in AppUserRoleBindingInput) ([]byte, bool, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, false, err
	}

	existing, found, err := c.readRoleBindingForScope(authCtx, in)
	if err != nil {
		if isStatusCode(err, http.StatusNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	state, err := json.Marshal(existing)
	if err != nil {
		return nil, false, err
	}

	return state, true, nil
}

// assignForScope calls the scope-specific assign API.
func (c *client) assignForScope(ctx context.Context, in AppUserRoleBindingInput) error {
	req := syncp.AppUserAssignRequest{RoleId: in.RoleID}

	switch in.Scope {
	case RoleBindingScopeTeam:
		_, _, err := c.api.TeamAPI.UpdateTeamAppUser(ctx, in.TargetID, in.TeamAppUserID).AppUserAssignRequest(req).Execute()
		return err
	case RoleBindingScopeSystem:
		_, _, err := c.api.SystemAPI.AssignSystemTeamAppUser(ctx, in.TargetID, in.TeamAppUserID).AppUserAssignRequest(req).Execute()
		return err
	case RoleBindingScopeAccount:
		_, _, err := c.api.AccountAPI.AssignAccountTeamAppUser(ctx, in.TargetID, in.TeamAppUserID).AppUserAssignRequest(req).Execute()
		return err
	case RoleBindingScopeNatsUser:
		_, _, err := c.api.NatsUserAPI.AssignNatsUserTeamAppUser(ctx, in.TargetID, in.TeamAppUserID).AppUserAssignRequest(req).Execute()
		return err
	default:
		return fmt.Errorf("unsupported scope: %s", in.Scope)
	}
}

// unassignForScope calls the scope-specific unassign API.
func (c *client) unassignForScope(ctx context.Context, in AppUserRoleBindingInput) error {
	switch in.Scope {
	case RoleBindingScopeTeam:
		_, err := c.api.TeamAPI.UnAssignTeamAppUser(ctx, in.TargetID, in.TeamAppUserID).Execute()
		return err
	case RoleBindingScopeSystem:
		_, err := c.api.SystemAPI.UnAssignSystemTeamAppUser(ctx, in.TargetID, in.TeamAppUserID).Execute()
		return err
	case RoleBindingScopeAccount:
		_, err := c.api.AccountAPI.UnAssignAccountTeamAppUser(ctx, in.TargetID, in.TeamAppUserID).Execute()
		return err
	case RoleBindingScopeNatsUser:
		_, err := c.api.NatsUserAPI.UnAssignNatsUserTeamAppUser(ctx, in.TargetID, in.TeamAppUserID).Execute()
		return err
	default:
		return fmt.Errorf("unsupported scope: %s", in.Scope)
	}
}

// readRoleBindingForScope lists assignments on the target and finds the one matching the team app user.
func (c *client) readRoleBindingForScope(ctx context.Context, in AppUserRoleBindingInput) (*syncp.AppUserAssignResponse, bool, error) {
	switch in.Scope {
	case RoleBindingScopeTeam:
		list, _, err := c.api.TeamAPI.ListTeamAppUsers(ctx, in.TargetID).Execute()
		if err != nil {
			return nil, false, fmt.Errorf("list team app users: %w", err)
		}
		for _, item := range list.Items {
			if item.TeamAppUser.Id == in.TeamAppUserID {
				return &item, true, nil
			}
		}
		return nil, false, nil

	case RoleBindingScopeSystem:
		list, _, err := c.api.SystemAPI.ListSystemTeamAppUsers(ctx, in.TargetID).Execute()
		if err != nil {
			return nil, false, fmt.Errorf("list system team app users: %w", err)
		}
		for _, item := range list.Items {
			if item.TeamAppUser.Id == in.TeamAppUserID {
				return &item, true, nil
			}
		}
		return nil, false, nil

	case RoleBindingScopeAccount:
		list, _, err := c.api.AccountAPI.ListAccountTeamAppUsers(ctx, in.TargetID).Execute()
		if err != nil {
			return nil, false, fmt.Errorf("list account team app users: %w", err)
		}
		for _, item := range list.Items {
			if item.TeamAppUser.Id == in.TeamAppUserID {
				return &item, true, nil
			}
		}
		return nil, false, nil

	case RoleBindingScopeNatsUser:
		list, _, err := c.api.NatsUserAPI.ListNatsUserTeamAppUsers(ctx, in.TargetID).Execute()
		if err != nil {
			return nil, false, fmt.Errorf("list nats user team app users: %w", err)
		}
		for _, item := range list.Items {
			if item.TeamAppUser.Id == in.TeamAppUserID {
				return &item, true, nil
			}
		}
		return nil, false, nil

	default:
		return nil, false, fmt.Errorf("unsupported scope: %s", in.Scope)
	}
}
