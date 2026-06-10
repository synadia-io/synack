package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/synadia-io/control-plane-sdk-go/syncp"
)

func TestAuthContextUsesTokenEnv(t *testing.T) {
	t.Setenv("SYNACK_TEST_TOKEN_ENV", "env-token")

	cp, err := NewClient(Options{
		TokenEnv: "SYNACK_TEST_TOKEN_ENV",
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, err := cp.(*client).authContext(context.Background())
	if err != nil {
		t.Fatalf("auth context: %v", err)
	}
	if got := ctx.Value(syncp.ContextAccessToken); got != "env-token" {
		t.Fatalf("expected env token, got %v", got)
	}
}

func TestAuthContextUsesTokenFile(t *testing.T) {
	t.Setenv("SYNACK_TEST_TOKEN_ENV", "env-token")
	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "token")
	if err := os.WriteFile(tokenFile, []byte("file-token\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	cp, err := NewClient(Options{
		TokenEnv:  "SYNACK_TEST_TOKEN_ENV",
		TokenFile: tokenFile,
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, err := cp.(*client).authContext(context.Background())
	if err != nil {
		t.Fatalf("auth context: %v", err)
	}
	if got := ctx.Value(syncp.ContextAccessToken); got != "file-token" {
		t.Fatalf("expected file token, got %v", got)
	}
}

func TestValidateToken(t *testing.T) {
	t.Setenv("SYNACK_TEST_TOKEN_ENV", "valid-token")

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/core/beta/version" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer valid-token" {
			t.Fatalf("expected bearer token, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"commit":"test","date":"2026-05-07","version":"test"}`))
	}))
	defer server.Close()

	cp, err := NewClient(Options{
		BaseURL:  server.URL,
		TokenEnv: "SYNACK_TEST_TOKEN_ENV",
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := cp.ValidateToken(context.Background()); err != nil {
		t.Fatalf("validate token: %v", err)
	}
	if requests != 1 {
		t.Fatalf("expected one validation request, got %d", requests)
	}
}

func TestValidateTokenRejectsInvalidToken(t *testing.T) {
	t.Setenv("SYNACK_TEST_TOKEN_ENV", "invalid-token")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/core/beta/version" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		http.Error(w, "secret token detail", http.StatusUnauthorized)
	}))
	defer server.Close()

	cp, err := NewClient(Options{
		BaseURL:  server.URL,
		TokenEnv: "SYNACK_TEST_TOKEN_ENV",
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	err = cp.ValidateToken(context.Background())
	if err == nil {
		t.Fatal("expected invalid token error")
	}
	if !strings.Contains(err.Error(), "validate control plane token") {
		t.Fatalf("expected validation context, got %v", err)
	}
	if !strings.Contains(err.Error(), "status: 401") {
		t.Fatalf("expected status code context, got %v", err)
	}
	if strings.Contains(err.Error(), "secret token detail") {
		t.Fatalf("expected response body to be hidden, got %v", err)
	}
}
