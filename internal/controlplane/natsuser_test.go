package controlplane

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	natsv1 "github.com/synadia-io/synack/api/v1alpha1"
)

func TestEnsureNatsUserUpdateIncludesJwtSettings(t *testing.T) {
	var patchBody map[string]any
	var patchCalls int
	skGroupID := "sk-1"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/core/beta/nats-users/U-1":
			writeControlPlaneJSON(t, w, natsUserView("U-1", "original", "U-PUB", skGroupID))
		case r.Method == http.MethodPatch && r.URL.Path == "/api/core/beta/nats-users/U-1":
			patchCalls++
			if err := json.NewDecoder(r.Body).Decode(&patchBody); err != nil {
				t.Fatalf("decode patch body: %v", err)
			}
			writeControlPlaneJSON(t, w, natsUserView("U-1", "updated", "U-PUB", skGroupID))
		default:
			t.Logf("unexpected request: %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	t.Setenv("SYNACK_TEST_TOKEN", "token")
	cp, err := NewClient(Options{
		BaseURL:  server.URL,
		TokenEnv: "SYNACK_TEST_TOKEN",
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	expires := int64(3600)
	bearer := false
	data := int64(-1)
	payload := int64(1024)
	subs := int64(10)
	got, err := cp.EnsureNatsUser(context.Background(), NatsUserInput{
		AccountSelectors:  AccountSelectors{AccountID: "A-1"},
		NatsUserID:        "U-1",
		Name:              "updated",
		SigningKeyGroupID: skGroupID,
		Spec: natsv1.NatsUserSpec{
			JwtExpiresInSecs:       &expires,
			BearerToken:            &bearer,
			Data:                   &data,
			Payload:                &payload,
			Subs:                   &subs,
			AllowedConnectionTypes: []string{"standard", "websocket"},
			Tags:                   []string{"managed", "dev"},
		},
	})
	if err != nil {
		t.Fatalf("EnsureNatsUser() error = %v", err)
	}
	if got.NatsUserID != "U-1" {
		t.Fatalf("EnsureNatsUser() NatsUserID = %q, want %q", got.NatsUserID, "U-1")
	}
	if patchCalls != 1 {
		t.Fatalf("patchCalls = %d, want 1", patchCalls)
	}

	assertJSONValue(t, patchBody, "name", "updated")
	assertJSONValue(t, patchBody, "jwt_expires_in_secs", float64(3600))
	jwtSettings, ok := patchBody["jwt_settings"].(map[string]any)
	if !ok {
		t.Fatalf("jwt_settings = %#v, want object", patchBody["jwt_settings"])
	}
	assertJSONValue(t, jwtSettings, "bearer_token", false)
	assertJSONValue(t, jwtSettings, "data", float64(-1))
	assertJSONValue(t, jwtSettings, "payload", float64(1024))
	assertJSONValue(t, jwtSettings, "subs", float64(10))
	assertJSONSlice(t, jwtSettings, "allowed_connection_types", []string{"standard", "websocket"})
	assertJSONSlice(t, jwtSettings, "tags", []string{"managed", "dev"})
}

func TestEnsureNatsUserSigningKeyGroupMismatchFailsBeforeUpdate(t *testing.T) {
	var patchCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/core/beta/nats-users/U-1":
			writeControlPlaneJSON(t, w, natsUserView("U-1", "original", "U-PUB", "sk-old"))
		case r.Method == http.MethodPatch && r.URL.Path == "/api/core/beta/nats-users/U-1":
			patchCalls++
			http.Error(w, "unexpected patch", http.StatusInternalServerError)
		default:
			t.Logf("unexpected request: %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	t.Setenv("SYNACK_TEST_TOKEN", "token")
	cp, err := NewClient(Options{
		BaseURL:  server.URL,
		TokenEnv: "SYNACK_TEST_TOKEN",
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	_, err = cp.EnsureNatsUser(context.Background(), NatsUserInput{
		AccountSelectors:  AccountSelectors{AccountID: "A-1"},
		NatsUserID:        "U-1",
		Name:              "updated",
		SigningKeyGroupID: "sk-new",
		Spec:              natsv1.NatsUserSpec{},
	})
	if err == nil {
		t.Fatal("EnsureNatsUser() error = nil, want signing key group mismatch")
	}
	if !strings.Contains(err.Error(), "updating signing key group for an existing user is not currently supported") {
		t.Fatalf("EnsureNatsUser() error = %q, want signing key group unsupported message", err)
	}
	if patchCalls != 0 {
		t.Fatalf("patchCalls = %d, want 0", patchCalls)
	}
}

func TestEnsureNatsUserWithoutIDCreatesWithoutNameLookup(t *testing.T) {
	var createBody map[string]any
	var createCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/core/beta/accounts/A-1/nats-users":
			createCalls++
			if err := json.NewDecoder(r.Body).Decode(&createBody); err != nil {
				t.Fatalf("decode create body: %v", err)
			}
			writeControlPlaneJSON(t, w, natsUserView("U-2", "same-name", "U-PUB-2", "sk-1"))
		case r.Method == http.MethodGet && r.URL.Path == "/api/core/beta/accounts/A-1/nats-users":
			t.Fatal("EnsureNatsUser() listed users by name without an ID")
		case r.Method == http.MethodPatch:
			t.Fatal("EnsureNatsUser() patched an existing user without an ID")
		default:
			t.Logf("unexpected request: %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	t.Setenv("SYNACK_TEST_TOKEN", "token")
	cp, err := NewClient(Options{
		BaseURL:  server.URL,
		TokenEnv: "SYNACK_TEST_TOKEN",
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	got, err := cp.EnsureNatsUser(context.Background(), NatsUserInput{
		AccountSelectors:  AccountSelectors{AccountID: "A-1"},
		Name:              "same-name",
		SigningKeyGroupID: "sk-1",
	})
	if err != nil {
		t.Fatalf("EnsureNatsUser() error = %v", err)
	}
	if got.NatsUserID != "U-2" {
		t.Fatalf("EnsureNatsUser() NatsUserID = %q, want %q", got.NatsUserID, "U-2")
	}
	if createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1", createCalls)
	}
	assertJSONValue(t, createBody, "name", "same-name")
	assertJSONValue(t, createBody, "sk_group_id", "sk-1")
}

func TestNatsUserNoIDReadAndDeleteDoNotLookupByName(t *testing.T) {
	var requestCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCalls++
		t.Logf("unexpected request: %s %s", r.Method, r.URL.String())
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)

	t.Setenv("SYNACK_TEST_TOKEN", "token")
	cp, err := NewClient(Options{
		BaseURL:  server.URL,
		TokenEnv: "SYNACK_TEST_TOKEN",
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	state, found, err := cp.ReadNatsUserState(context.Background(), NatsUserInput{
		AccountSelectors: AccountSelectors{AccountID: "A-1"},
		Name:             "same-name",
	})
	if err != nil {
		t.Fatalf("ReadNatsUserState() error = %v", err)
	}
	if found || state != nil {
		t.Fatalf("ReadNatsUserState() = (%q, %v), want (nil, false)", state, found)
	}

	if err := cp.DeleteNatsUser(context.Background(), NatsUserInput{
		AccountSelectors: AccountSelectors{AccountID: "A-1"},
		Name:             "same-name",
	}); err != nil {
		t.Fatalf("DeleteNatsUser() error = %v", err)
	}

	if requestCalls != 0 {
		t.Fatalf("requestCalls = %d, want 0", requestCalls)
	}
}

func natsUserView(id, name, publicKey, skGroupID string) map[string]any {
	return map[string]any{
		"id":                  id,
		"name":                name,
		"user_public_key":     publicKey,
		"sk_group_id":         skGroupID,
		"jwt_settings":        map[string]any{},
		"jwt_expires_in_secs": 0,
		"jwt_expires_at_max":  0,
		"created":             time.Now().UTC().Format(time.RFC3339),
		"account":             map[string]any{},
		"system":              map[string]any{},
		"team":                map[string]any{},
		"claims":              map[string]any{},
		"jwt":                 "",
	}
}

func writeControlPlaneJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func assertJSONValue(t *testing.T, body map[string]any, key string, want any) {
	t.Helper()
	if got := body[key]; got != want {
		t.Fatalf("%s = %#v, want %#v", key, got, want)
	}
}

func assertJSONSlice(t *testing.T, body map[string]any, key string, want []string) {
	t.Helper()
	got, ok := body[key].([]any)
	if !ok {
		t.Fatalf("%s = %#v, want array", key, body[key])
	}
	if len(got) != len(want) {
		t.Fatalf("%s length = %d, want %d", key, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s[%d] = %#v, want %#v", key, i, got[i], want[i])
		}
	}
}
