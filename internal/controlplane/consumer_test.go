package controlplane

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	natsv1 "github.com/synadia-io/synack/api/v1alpha1"
)

func TestEnsurePullConsumerUpdateSendsSDKSupportedFields(t *testing.T) {
	var patchBody map[string]any
	var patchCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/api/core/beta/consumers/pull/C-1":
			patchCalls++
			if err := json.NewDecoder(r.Body).Decode(&patchBody); err != nil {
				t.Fatalf("decode patch body: %v", err)
			}
			writeControlPlaneJSON(t, w, pullConsumerInfo("C-1", "orders", nil))
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

	_, err = cp.EnsureConsumer(context.Background(), ConsumerInput{
		StreamID:   "S-1",
		ConsumerID: "C-1",
		Spec: natsv1.ConsumerSpec{
			Name:        "orders",
			Description: "updated",
			AckWait:     "30s",
			Backoff:     []string{"1s", "2s"},
			FilterSubjects: []string{
				"orders.created",
				"orders.updated",
			},
			InactiveThreshold:  "5m",
			MaxAckPending:      200,
			MaxDeliver:         5,
			MaxRequestBatch:    25,
			MaxRequestMaxBytes: 4096,
			MaxRequestExpires:  "10s",
			Metadata: map[string]string{
				"managed-by": "synack",
			},
			Replicas:   3,
			SampleFreq: "100%",
		},
	})
	if err != nil {
		t.Fatalf("EnsureConsumer() error = %v", err)
	}
	if patchCalls != 1 {
		t.Fatalf("patchCalls = %d, want 1", patchCalls)
	}
	assertJSONValue(t, patchBody, "description", "updated")
	assertJSONValue(t, patchBody, "ack_wait", float64(30*time.Second))
	backoff, ok := patchBody["backoff"].([]any)
	if !ok {
		t.Fatalf("backoff = %#v, want array", patchBody["backoff"])
	}
	if len(backoff) != 2 || backoff[0] != float64(time.Second) || backoff[1] != float64(2*time.Second) {
		t.Fatalf("backoff = %#v, want [%d %d]", backoff, time.Second, 2*time.Second)
	}
	assertJSONSlice(t, patchBody, "filter_subjects", []string{"orders.created", "orders.updated"})
	assertJSONValue(t, patchBody, "inactive_threshold", float64(5*time.Minute))
	assertJSONValue(t, patchBody, "max_ack_pending", float64(200))
	assertJSONValue(t, patchBody, "max_batch", float64(25))
	assertJSONValue(t, patchBody, "max_bytes", float64(4096))
	assertJSONValue(t, patchBody, "max_deliver", float64(5))
	assertJSONValue(t, patchBody, "max_expires", float64(10*time.Second))
	metadata, ok := patchBody["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("metadata = %#v, want object", patchBody["metadata"])
	}
	if got := metadata["managed-by"]; got != "synack" {
		t.Fatalf("metadata.managed-by = %#v, want %q", got, "synack")
	}
	assertJSONValue(t, patchBody, "num_replicas", float64(3))
	assertJSONValue(t, patchBody, "sample_freq", "100%")
}

func TestEnsureConsumerWithoutIDUpdatesByDerivedConsumerID(t *testing.T) {
	var patchCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/api/core/beta/consumers/pull/S-1.orders":
			patchCalls++
			writeControlPlaneJSON(t, w, pullConsumerInfo("S-1.orders", "orders", nil))
		case r.Method == http.MethodGet && r.URL.Path == "/api/core/beta/jetstream/stream/S-1/consumers":
			t.Fatal("EnsureConsumer() listed consumers instead of using the derived consumer ID")
		case r.Method == http.MethodPost:
			t.Fatal("EnsureConsumer() created a consumer instead of updating the derived consumer ID")
		case r.Method == http.MethodPatch:
			t.Fatalf("unexpected patch target: %s", r.URL.String())
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

	got, err := cp.EnsureConsumer(context.Background(), ConsumerInput{
		StreamID: "S-1",
		Spec:     natsv1.ConsumerSpec{Name: "orders"},
	})
	if err != nil {
		t.Fatalf("EnsureConsumer() error = %v", err)
	}
	if got.ConsumerID != "S-1.orders" {
		t.Fatalf("EnsureConsumer() ConsumerID = %q, want %q", got.ConsumerID, "S-1.orders")
	}
	if patchCalls != 1 {
		t.Fatalf("patchCalls = %d, want 1", patchCalls)
	}
}

func TestCreatePullConsumerWithoutDurableNameOmitsDurableName(t *testing.T) {
	var createBody map[string]any
	var createCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/api/core/beta/consumers/pull/S-1.orders":
			w.WriteHeader(http.StatusNotFound)
			writeControlPlaneJSON(t, w, map[string]any{"error": "not found"})
		case r.Method == http.MethodPost && r.URL.Path == "/api/core/beta/jetstream/stream/S-1/consumers/pull":
			createCalls++
			if err := json.NewDecoder(r.Body).Decode(&createBody); err != nil {
				t.Fatalf("decode create body: %v", err)
			}
			writeControlPlaneJSON(t, w, pullConsumerInfo("S-1.orders", "orders", nil))
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

	if _, err := cp.EnsureConsumer(context.Background(), ConsumerInput{
		StreamID: "S-1",
		Spec:     natsv1.ConsumerSpec{Name: "orders"},
	}); err != nil {
		t.Fatalf("EnsureConsumer() error = %v", err)
	}
	if createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1", createCalls)
	}
	if _, ok := createBody["durable_name"]; ok {
		t.Fatalf("durable_name = %#v, want omitted", createBody["durable_name"])
	}
}

func TestReadConsumerStateWithoutIDUsesDerivedConsumerID(t *testing.T) {
	var getCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/core/beta/consumers/pull/S-1.orders":
			getCalls++
			writeControlPlaneJSON(t, w, pullConsumerInfo("S-1.orders", "orders", map[string]any{"description": "existing"}))
		case r.Method == http.MethodGet && r.URL.Path == "/api/core/beta/jetstream/stream/S-1/consumers":
			t.Fatal("ReadConsumerState() listed consumers instead of using the derived consumer ID")
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

	state, found, err := cp.ReadConsumerState(context.Background(), ConsumerInput{
		StreamID: "S-1",
		Spec:     natsv1.ConsumerSpec{Name: "orders"},
	})
	if err != nil {
		t.Fatalf("ReadConsumerState() error = %v", err)
	}
	if !found || state == nil {
		t.Fatalf("ReadConsumerState() = (%q, %v), want state and true", state, found)
	}
	var got map[string]any
	if err := json.Unmarshal(state, &got); err != nil {
		t.Fatalf("unmarshal state: %v", err)
	}
	assertJSONValue(t, got, "description", "existing")
	if getCalls != 1 {
		t.Fatalf("getCalls = %d, want 1", getCalls)
	}
}

func TestDeleteConsumerWithoutTypeFallsBackToPushByDerivedConsumerID(t *testing.T) {
	var pullDeleteCalls int
	var pushDeleteCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/api/core/beta/consumers/pull/S-1.orders":
			pullDeleteCalls++
			w.WriteHeader(http.StatusNotFound)
			writeControlPlaneJSON(t, w, map[string]any{
				"error": "not found",
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/core/beta/consumers/push/S-1.orders":
			pushDeleteCalls++
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/api/core/beta/jetstream/stream/S-1/consumers":
			t.Fatal("DeleteConsumer() listed consumers instead of using the derived consumer ID")
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

	if err := cp.DeleteConsumer(context.Background(), ConsumerInput{
		StreamID: "S-1",
		Spec:     natsv1.ConsumerSpec{Name: "orders"},
	}); err != nil {
		t.Fatalf("DeleteConsumer() error = %v", err)
	}

	if pullDeleteCalls != 1 {
		t.Fatalf("pullDeleteCalls = %d, want 1", pullDeleteCalls)
	}
	if pushDeleteCalls != 1 {
		t.Fatalf("pushDeleteCalls = %d, want 1", pushDeleteCalls)
	}
}

func TestDeleteConsumerWithKnownIDWithoutTypeFallsBackToPush(t *testing.T) {
	var pullDeleteCalls int
	var pushDeleteCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/api/core/beta/consumers/pull/S-1.orders":
			pullDeleteCalls++
			w.WriteHeader(http.StatusNotFound)
			writeControlPlaneJSON(t, w, map[string]any{
				"error": "not found",
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/core/beta/consumers/push/S-1.orders":
			pushDeleteCalls++
			w.WriteHeader(http.StatusNoContent)
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

	if err := cp.DeleteConsumer(context.Background(), ConsumerInput{
		StreamID:   "S-1",
		ConsumerID: "S-1.orders",
		Spec:       natsv1.ConsumerSpec{Name: "orders"},
	}); err != nil {
		t.Fatalf("DeleteConsumer() error = %v", err)
	}

	if pullDeleteCalls != 1 {
		t.Fatalf("pullDeleteCalls = %d, want 1", pullDeleteCalls)
	}
	if pushDeleteCalls != 1 {
		t.Fatalf("pushDeleteCalls = %d, want 1", pushDeleteCalls)
	}
}

func pullConsumerInfo(id, name string, configOverrides map[string]any) map[string]any {
	return consumerInfo(id, name, "pull", configOverrides)
}

func pushConsumerInfo(id, name string, configOverrides map[string]any) map[string]any {
	return consumerInfo(id, name, "push", configOverrides)
}

func consumerInfo(id, name, consumerType string, configOverrides map[string]any) map[string]any {
	config := map[string]any{
		"name":           name,
		"durable_name":   name,
		"ack_policy":     "explicit",
		"deliver_policy": "all",
		"num_replicas":   0,
		"replay_policy":  "instant",
	}
	for key, value := range configOverrides {
		config[key] = value
	}
	return map[string]any{
		"id":              id,
		"name":            name,
		"consumer_type":   consumerType,
		"config":          config,
		"created":         time.Now().UTC().Format(time.RFC3339),
		"stream_name":     "ORDERS",
		"ack_floor":       map[string]any{},
		"delivered":       map[string]any{},
		"num_ack_pending": 0,
		"num_pending":     0,
		"num_redelivered": 0,
		"num_waiting":     0,
		"ts":              time.Now().UTC().Format(time.RFC3339),
	}
}
