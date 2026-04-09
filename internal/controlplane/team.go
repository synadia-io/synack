package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/synadia-io/control-plane-sdk-go/syncp"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type TeamInput struct {
	TeamID string
	Name   string
}

type TeamResult struct {
	TeamID string
}

func (c *client) EnsureTeam(ctx context.Context, in TeamInput) (TeamResult, error) {
	l := log.FromContext(ctx).WithValues("resourceType", "team", "resourceName", in.Name)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return TeamResult{}, err
	}

	// If we already know the ID use it directly, don't fall through to create.
	if in.TeamID != "" {
		existing, _, err := c.api.TeamAPI.GetTeam(authCtx, in.TeamID).Execute()
		if err != nil {
			err = withAPIError(err)
			if isStatusCode(err, http.StatusNotFound) {
				l.Info("known team ID not found, recreating by name", "resourceID", in.TeamID)
				in.TeamID = ""
			} else {
				return TeamResult{}, fmt.Errorf("get team %q: %w", in.TeamID, err)
			}
		} else {
			if existing.Name != in.Name {
				name := in.Name
				updated, _, err := c.api.TeamAPI.UpdateTeam(authCtx, in.TeamID).TeamUpdateRequest(syncp.TeamUpdateRequest{
					Name: &name,
				}).Execute()
				if err != nil {
					err = withAPIError(err)
					return TeamResult{}, fmt.Errorf("update team %q: %w", in.TeamID, err)
				}
				l.Info("team updated", "resourceID", updated.Id)
			}
			return TeamResult{TeamID: existing.Id}, nil
		}
	}

	created, _, err := c.api.SessionAPI.CreateTeam(authCtx).TeamCreateRequest(syncp.TeamCreateRequest{
		Name: in.Name,
	}).Execute()
	if err != nil {
		err = withAPIError(err)
		return TeamResult{}, fmt.Errorf("create team %q: %w", in.Name, err)
	}

	l.Info("team created", "resourceID", created.Id)
	return TeamResult{TeamID: created.Id}, nil
}

func (c *client) DeleteTeam(ctx context.Context, in TeamInput) error {
	l := log.FromContext(ctx).WithValues("resourceType", "team", "resourceName", in.Name)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	if in.TeamID == "" {
		return nil
	}

	_, err = c.api.TeamAPI.DeleteTeam(authCtx, in.TeamID).Execute()
	if err == nil || isStatusCode(err, http.StatusNotFound) {
		l.Info("team deleted", "resourceID", in.TeamID)
		return nil
	}
	err = withAPIError(err)

	return fmt.Errorf("delete team %q: %w", in.TeamID, err)
}

func (c *client) ReadTeamState(ctx context.Context, in TeamInput) ([]byte, bool, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, false, err
	}

	if in.TeamID == "" {
		return nil, false, nil
	}

	team, _, err := c.api.TeamAPI.GetTeam(authCtx, in.TeamID).Execute()
	if err != nil {
		err = withAPIError(err)
		if isStatusCode(err, http.StatusNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get team %q: %w", in.TeamID, err)
	}

	state, err := json.Marshal(team)
	if err != nil {
		return nil, false, err
	}

	return state, true, nil
}
