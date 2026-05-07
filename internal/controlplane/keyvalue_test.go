package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestEnsureKeyValueInfersDeterministicIDFromAccountID(t *testing.T) {
	var patchCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/api/core/beta/jetstream/kv-bucket/A-1.KV_config":
			patchCalls++
			writeControlPlaneJSON(t, w, map[string]any{
				"id":          "A-1.KV_config",
				"stream_name": "KV_config",
				"bytes":       0,
				"num_values":  0,
				"config": map[string]any{
					"bucket":  "config",
					"storage": "file",
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/core/beta/accounts/A-1/jetstream/kv-buckets":
			t.Fatal("EnsureKeyValue() listed kv buckets instead of using the deterministic stream ID")
		case r.Method == http.MethodPost:
			t.Fatal("EnsureKeyValue() created a kv bucket instead of updating the deterministic stream ID")
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

	got, err := cp.EnsureKeyValue(context.Background(), KeyValueInput{
		AccountSelectors: AccountSelectors{AccountID: "A-1"},
		Bucket:           "config",
	})
	if err != nil {
		t.Fatalf("EnsureKeyValue() error = %v", err)
	}
	if got.KeyValueID != "A-1.KV_config" {
		t.Fatalf("EnsureKeyValue() KeyValueID = %q, want %q", got.KeyValueID, "A-1.KV_config")
	}
	if patchCalls != 1 {
		t.Fatalf("patchCalls = %d, want 1", patchCalls)
	}
}
