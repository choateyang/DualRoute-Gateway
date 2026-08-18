package gateway

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestNormalizeRequestBodyIsolatesTokenRouterState(t *testing.T) {
	cfg := DefaultConfig()
	cfg.UpstreamProvider = ProviderTokenRouter
	cfg.ForcedModel = tokenRouterModel
	cfg.IsolateUpstreamState = true
	g := &Gateway{cfg: cfg}

	body, err := g.normalizeRequestBodyChecked("/v1/chat/completions", []byte(`{
		"model":"client-model",
		"session_id":"provider-session",
		"previous_response_id":"response-1",
		"conversation_id":"conversation-1",
		"messages":[{"role":"user","content":"hello"}]
	}`))
	if err != nil {
		t.Fatalf("normalizeRequestBodyChecked() error = %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode normalized body: %v", err)
	}
	for _, key := range []string{"session_id", "previous_response_id", "conversation_id"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("isolated TokenRouter request retained %q", key)
		}
	}
	if payload["model"] != tokenRouterModel {
		t.Fatalf("model = %v, want %q", payload["model"], tokenRouterModel)
	}
}

func TestNormalizeRequestBodyCanPreserveStateWhenDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.UpstreamProvider = ProviderTokenRouter
	cfg.ForcedModel = tokenRouterModel
	cfg.IsolateUpstreamState = false
	g := &Gateway{cfg: cfg}

	body, err := g.normalizeRequestBodyChecked("/v1/chat/completions", []byte(`{
		"model":"client-model",
		"session_id":"provider-session",
		"messages":[{"role":"user","content":"hello"}]
	}`))
	if err != nil {
		t.Fatalf("normalizeRequestBodyChecked() error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode normalized body: %v", err)
	}
	if payload["session_id"] != "provider-session" {
		t.Fatalf("session_id = %v, want provider-session", payload["session_id"])
	}
}

func TestSetNoStoreResponseHeaders(t *testing.T) {
	header := make(http.Header)
	setNoStoreResponseHeaders(header)
	if got := header.Get("Cache-Control"); got != "no-store, private" {
		t.Fatalf("Cache-Control = %q", got)
	}
	if got := header.Get("Pragma"); got != "no-cache" {
		t.Fatalf("Pragma = %q", got)
	}
}
