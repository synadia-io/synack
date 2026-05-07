package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestEnsureObjectStoreInfersDeterministicIDFromAccountID(t *testing.T) {
	var patchCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/api/core/beta/jetstream/object-bucket/A-1.OBJ_assets":
			patchCalls++
			writeControlPlaneJSON(t, w, map[string]any{
				"id":          "A-1.OBJ_assets",
				"stream_name": "OBJ_assets",
				"bytes":       0,
				"config": map[string]any{
					"bucket": "assets",
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/core/beta/accounts/A-1/jetstream/object-buckets":
			t.Fatal("EnsureObjectStore() listed object buckets instead of using the deterministic stream ID")
		case r.Method == http.MethodPost:
			t.Fatal("EnsureObjectStore() created an object bucket instead of updating the deterministic stream ID")
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

	got, err := cp.EnsureObjectStore(context.Background(), ObjectStoreInput{
		AccountSelectors: AccountSelectors{AccountID: "A-1"},
		Bucket:           "assets",
	})
	if err != nil {
		t.Fatalf("EnsureObjectStore() error = %v", err)
	}
	if got.ObjectStoreID != "A-1.OBJ_assets" {
		t.Fatalf("EnsureObjectStore() ObjectStoreID = %q, want %q", got.ObjectStoreID, "A-1.OBJ_assets")
	}
	if patchCalls != 1 {
		t.Fatalf("patchCalls = %d, want 1", patchCalls)
	}
}
