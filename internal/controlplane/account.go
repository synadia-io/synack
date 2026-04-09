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

type AccountInput struct {
	AccountID string
	SystemID  string
	Name      string
}

type AccountResult struct {
	AccountID string
}

func (c *client) EnsureAccount(ctx context.Context, in AccountInput) (AccountResult, error) {
	l := log.FromContext(ctx).WithValues("resourceType", "account", "resourceName", in.Name)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return AccountResult{}, err
	}

	// If we already know the ID use it directly, don't fall through to create.
	if in.AccountID != "" {
		acc, _, err := c.api.AccountAPI.GetAccount(authCtx, in.AccountID).Execute()
		if err != nil {
			err = withAPIError(err)
			if isStatusCode(err, http.StatusNotFound) {
				l.Info("known account ID not found, recreating by name", "resourceID", in.AccountID)
				in.AccountID = ""
			} else {
				return AccountResult{}, fmt.Errorf("get account %q: %w", in.AccountID, err)
			}
		} else {
			return AccountResult{AccountID: acc.Id}, nil
		}
	}

	list, _, err := c.api.SystemAPI.ListAccounts(authCtx, in.SystemID).Execute()
	if err != nil {
		err = withAPIError(err)
		return AccountResult{}, fmt.Errorf("list accounts: %w", err)
	}

	for _, a := range list.Items {
		if a.Name == in.Name {
			return AccountResult{AccountID: a.Id}, nil
		}
	}

	createReq := syncp.AccountCreateRequest{Name: in.Name}
	created, _, err := c.api.SystemAPI.CreateAccount(authCtx, in.SystemID).AccountCreateRequest(createReq).Execute()
	if err != nil {
		err = withAPIError(err)
		return AccountResult{}, fmt.Errorf("create account %q: %w", in.Name, err)
	}

	l.Info("account created", "resourceID", created.Id, "systemID", in.SystemID)

	return AccountResult{AccountID: created.Id}, nil
}

func (c *client) DeleteAccount(ctx context.Context, in AccountInput) error {
	l := log.FromContext(ctx).WithValues("resourceType", "account", "resourceName", in.Name)

	authCtx, err := c.authContext(ctx)
	if err != nil {
		return err
	}

	accountID := in.AccountID
	if accountID == "" {
		list, _, err := c.api.SystemAPI.ListAccounts(authCtx, in.SystemID).Execute()
		if err != nil {
			err = withAPIError(err)
			return fmt.Errorf("list accounts for delete: %w", err)
		}

		found := make([]string, 0)
		for _, a := range list.Items {
			if a.Name == in.Name {
				found = append(found, a.Id)
			}
		}

		if len(found) == 0 {
			return nil
		}

		if len(found) > 1 {
			return fmt.Errorf("multiple accounts found with name %q in system %q: %s", in.Name, in.SystemID, strings.Join(found, ", "))
		}

		accountID = found[0]
	}

	_, err = c.api.AccountAPI.DeleteAccount(authCtx, accountID).Execute()
	if err == nil || isStatusCode(err, http.StatusNotFound) {
		l.Info("account deleted", "resourceID", accountID, "systemID", in.SystemID)
		return nil
	}
	err = withAPIError(err)

	return fmt.Errorf("delete account %q: %w", accountID, err)
}

func (c *client) ReadAccountState(ctx context.Context, in AccountInput) ([]byte, bool, error) {
	authCtx, err := c.authContext(ctx)
	if err != nil {
		return nil, false, err
	}

	if in.AccountID != "" {
		acc, _, err := c.api.AccountAPI.GetAccount(authCtx, in.AccountID).Execute()
		if err != nil {
			err = withAPIError(err)
			if isStatusCode(err, http.StatusNotFound) {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("get account by account id %q: %w", in.AccountID, err)
		}
		state, err := json.Marshal(acc)
		if err != nil {
			return nil, false, err
		}
		return state, true, nil
	}

	list, _, err := c.api.SystemAPI.ListAccounts(authCtx, in.SystemID).Execute()
	if err != nil {
		err = withAPIError(err)
		return nil, false, fmt.Errorf("list accounts: %w", err)
	}

	for _, a := range list.Items {
		if a.Name != in.Name {
			continue
		}
		state, err := json.Marshal(a)
		if err != nil {
			return nil, false, err
		}

		return state, true, nil
	}

	return nil, false, nil
}
