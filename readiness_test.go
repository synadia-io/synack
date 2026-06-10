package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/go-logr/logr"
)

type readinessControlPlaneClient struct {
	validateErrs []error
	validateHit  int
}

func (c *readinessControlPlaneClient) ValidateToken(context.Context) error {
	c.validateHit++
	if len(c.validateErrs) == 0 {
		return nil
	}

	err := c.validateErrs[0]
	c.validateErrs = c.validateErrs[1:]
	return err
}

func TestControlPlaneReadinessRetriesUntilValidationSucceeds(t *testing.T) {
	cp := &readinessControlPlaneClient{
		validateErrs: []error{errors.New("unauthorized"), nil},
	}
	check := newControlPlaneReadiness(cp, "https://example.invalid", logr.Discard())
	req := httptest.NewRequest("GET", "/readyz", nil)

	if err := check.Check(req); err == nil {
		t.Fatal("expected first readiness check to fail")
	}
	if check.ready.Load() {
		t.Fatal("expected readiness state to remain false after failed validation")
	}

	if err := check.Check(req); err != nil {
		t.Fatalf("expected second readiness check to pass: %v", err)
	}
	if !check.ready.Load() {
		t.Fatal("expected readiness state after successful validation")
	}
	if cp.validateHit != 2 {
		t.Fatalf("expected validation retry, got %d calls", cp.validateHit)
	}
}
