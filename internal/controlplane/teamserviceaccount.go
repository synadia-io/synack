package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/synadia-io/control-plane-sdk-go/syncp"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type TeamServiceAccountInput struct {
	ServiceAccountID string
	TeamID           string
	Name             string
	TeamRoleID       string
}

type TeamServiceAccountResult struct {
	ServiceAccountID string
	TeamAppUserID    string
}

func (c *client) EnsureTeamServiceAccount(ctx context.Context, in TeamServiceAccountInput) (TeamServiceAccountResult, error) {
	l := log.FromContext(ctx).WithValues("resourceType", "teamServiceAccount", "resourceName", in.Name)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return TeamServiceAccountResult{}, err
	}

	// If we already know the ID use it directly, don't fall through to create.
	if in.ServiceAccountID != "" {
		existing, _, err := c.api.TeamServiceAccountAPI.GetTeamServiceAccount(authCtx, in.ServiceAccountID).Execute()
		if err != nil {
			if isStatusCode(err, http.StatusNotFound) {
				l.Info("known team service account ID not found, recreating by name", "resourceID", in.ServiceAccountID)
				in.ServiceAccountID = ""
			} else {
				return TeamServiceAccountResult{}, fmt.Errorf("get team service account %q: %w", in.ServiceAccountID, err)
			}
		} else {

			needsUpdate := false
			update := syncp.ServiceAccountUpdateRequest{}

			if existing.Name != in.Name {
				name := in.Name
				update.Name = &name
				needsUpdate = true
			}
			if in.TeamRoleID != "" && existing.RoleId != in.TeamRoleID {
				roleID := in.TeamRoleID
				update.RoleId = &roleID
				needsUpdate = true
			}

			if needsUpdate {
				_, _, err := c.api.TeamServiceAccountAPI.UpdateTeamServiceAccount(authCtx, in.ServiceAccountID).ServiceAccountUpdateRequest(update).Execute()
				if err != nil {
					return TeamServiceAccountResult{}, fmt.Errorf("update team service account %q: %w", in.ServiceAccountID, err)
				}
				l.Info("team service account updated", "resourceID", existing.Id)
			}

			tauID, err := extractTeamAppUserID(existing)
			if err != nil {
				return TeamServiceAccountResult{}, fmt.Errorf("extract team app user ID from service account %q: %w", in.ServiceAccountID, err)
			}

			return TeamServiceAccountResult{ServiceAccountID: existing.Id, TeamAppUserID: tauID}, nil
		}
	}

	createReq := syncp.ServiceAccountCreateRequest{
		Name:      in.Name,
		Resources: map[string]syncp.AppUserAssignRequest{},
	}
	if in.TeamRoleID != "" {
		createReq.RoleId = in.TeamRoleID
	}

	created, _, err := c.api.TeamAPI.CreateTeamServiceAccount(authCtx, in.TeamID).ServiceAccountCreateRequest(createReq).Execute()
	if err != nil {
		return TeamServiceAccountResult{}, fmt.Errorf("create team service account %q: %w", in.Name, err)
	}

	list, _, err := c.api.TeamAPI.ListTeamInfoAppUsers(authCtx, in.TeamID).Execute()
	if err != nil {
		// Not fatal
		l.Error(err, "failed to list team info app users")
	}

	appUserID := ""
	if list != nil {
		for _, item := range list.Items {
			if item.Id == created.Id {
				appUserID = item.AppUser.Id
				break
			}
		}
	}

	l.Info("team service account created", "resourceID", created.Id, "teamAppUserId", appUserID)
	return TeamServiceAccountResult{ServiceAccountID: created.Id, TeamAppUserID: appUserID}, nil
}

// extractTeamAppUserID finds the TeamAppUser.Id from the service account's Resources map.
// The team membership entry is keyed as "Team:<teamId>".
func extractTeamAppUserID(sa *syncp.ServiceAccountViewResponse) (string, error) {
	for key, assignment := range sa.Resources {
		if strings.HasPrefix(key, "Team:") {
			return assignment.TeamAppUser.Id, nil
		}
	}
	return "", fmt.Errorf("no team membership found in service account resources")
}

func (c *client) DeleteTeamServiceAccount(ctx context.Context, in TeamServiceAccountInput) error {
	l := log.FromContext(ctx).WithValues("resourceType", "teamServiceAccount", "resourceName", in.Name)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	if in.ServiceAccountID == "" {
		return nil
	}

	_, err = c.api.TeamServiceAccountAPI.DeleteTeamServiceAccount(authCtx, in.ServiceAccountID).Execute()
	if err == nil || isStatusCode(err, http.StatusNotFound) {
		l.Info("team service account deleted", "resourceID", in.ServiceAccountID)
		return nil
	}

	return fmt.Errorf("delete team service account %q: %w", in.ServiceAccountID, err)
}

func (c *client) ReadTeamServiceAccountState(ctx context.Context, in TeamServiceAccountInput) ([]byte, bool, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, false, err
	}

	if in.ServiceAccountID == "" {
		return nil, false, nil
	}

	sa, _, err := c.api.TeamServiceAccountAPI.GetTeamServiceAccount(authCtx, in.ServiceAccountID).Execute()
	if err != nil {
		if isStatusCode(err, http.StatusNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get team service account %q: %w", in.ServiceAccountID, err)
	}

	state, err := json.Marshal(sa)
	if err != nil {
		return nil, false, err
	}

	return state, true, nil
}
