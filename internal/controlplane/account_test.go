package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestEnsureAccountWithoutIDCreatesWithoutNameLookup(t *testing.T) {
	var createCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/core/beta/systems/SYS/accounts":
			createCalls++
			writeControlPlaneJSON(t, w, map[string]any{
				"id":   "A-1",
				"name": "orders",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/core/beta/systems/SYS/accounts":
			t.Fatal("EnsureAccount() listed accounts instead of creating without a provided account ID")
		default:
			t.Logf("unexpected request: %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	t.Setenv("SYNACK_TEST_TOKEN", "token")
	cp, err := NewClient(Options{BaseURL: server.URL, TokenEnv: "SYNACK_TEST_TOKEN", Timeout: time.Second})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	got, err := cp.EnsureAccount(context.Background(), AccountInput{
		SystemID: "SYS",
		Name:     "orders",
	})
	if err != nil {
		t.Fatalf("EnsureAccount() error = %v", err)
	}
	if got.AccountID != "A-1" {
		t.Fatalf("EnsureAccount() AccountID = %q, want %q", got.AccountID, "A-1")
	}
	if createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1", createCalls)
	}
}
