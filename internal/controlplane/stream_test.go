package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestEnsureStreamInfersDeterministicIDFromAccountID(t *testing.T) {
	var patchCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/api/core/beta/jetstream/stream/A-1.ORDERS":
			patchCalls++
			writeControlPlaneJSON(t, w, map[string]any{
				"id":             "A-1.ORDERS",
				"jetstream_type": "stream",
				"config": map[string]any{
					"name":     "ORDERS",
					"subjects": []string{"orders.>"},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/core/beta/accounts/A-1/jetstream/streams":
			t.Fatal("EnsureStream() listed streams instead of using the deterministic stream ID")
		case r.Method == http.MethodPost:
			t.Fatal("EnsureStream() created a stream instead of updating the deterministic stream ID")
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

	got, err := cp.EnsureStream(context.Background(), StreamInput{
		AccountSelectors: AccountSelectors{
			AccountID: "A-1",
		},
		Name:     "ORDERS",
		Subjects: []string{"orders.>"},
	})
	if err != nil {
		t.Fatalf("EnsureStream() error = %v", err)
	}
	if got.StreamID != "A-1.ORDERS" {
		t.Fatalf("EnsureStream() StreamID = %q, want %q", got.StreamID, "A-1.ORDERS")
	}
	if patchCalls != 1 {
		t.Fatalf("patchCalls = %d, want 1", patchCalls)
	}
}
