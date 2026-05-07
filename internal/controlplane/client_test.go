package controlplane

import (
	"context"
	"os"
	"path/filepath"
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
