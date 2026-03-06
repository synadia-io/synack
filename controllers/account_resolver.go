package controllers

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	natsv1 "github.com/synadia-io/synack/api/v1alpha1"
)

var errWaitingForAccount = fmt.Errorf("waiting for referenced Account to be ready")

// validateAccountSelectors ensures that accountRef is not combined with direct selectors.
func validateAccountSelectors(sel natsv1.AccountSelector) error {
	if sel.AccountRef == nil {
		return nil
	}

	if sel.AccountRef.Name == "" {
		return errors.New("spec.accountRef.name is required when accountRef is set")
	}

	if sel.AccountID != "" || sel.AccountPublicNKey != "" {
		return errors.New("spec.accountRef cannot be combined with accountId, account, or accountPublicNKey")
	}

	return nil
}

// resolveAccountRef resolves the account ID from an AccountSelector.
func resolveAccountRef(ctx context.Context, c client.Client, namespace string, sel natsv1.AccountSelector) (string, error) {
	if sel.AccountRef == nil {
		return sel.AccountID, nil
	}

	var account natsv1.Account
	key := types.NamespacedName{
		Namespace: namespace,
		Name:      sel.AccountRef.Name,
	}

	if err := c.Get(ctx, key, &account); err != nil {
		if apierrors.IsNotFound(err) {
			return "", errors.Join(errWaitingForAccount, fmt.Errorf("referenced Account %q not found", sel.AccountRef.Name))
		}
		return "", err
	}

	if account.Status.AccountID == "" {
		return "", errors.Join(errWaitingForAccount, fmt.Errorf("referenced Account %q is not ready", sel.AccountRef.Name))
	}

	return account.Status.AccountID, nil
}
