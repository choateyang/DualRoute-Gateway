package gateway

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig(upstream string) Config {
	c := DefaultConfig()
	c.UpstreamURL = upstream
	c.UpstreamAPIKey = "tokenrouter-test-key"
	c.ForcedModel = ""
	c.GatewayKeys = []string{"test-key"}
	c.MaxConcurrency = 1
	c.QueueSize = 1
	c.MaxRetries = 2
	c.CooldownBase = time.Millisecond
	c.CooldownMax = 5 * time.Millisecond
	c.ProxyProbeURLs = nil
	return c
}

func TestLoadConfigRequiresUpstreamAPIKey(t *testing.T) {
	t.Setenv("UPSTREAM_API_KEY", "")
	if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "UPSTREAM_API_KEY is required") {
		t.Fatalf("expected missing upstream key error, got %v", err)
	}
}

func TestLoadConfigForcesProviderModelWithoutGatewayKeys(t *testing.T) {
	t.Setenv("UPSTREAM_API_KEY", "upstream-test-key")
	t.Setenv("UPSTREAM_PROVIDER", ProviderOpenCode)
	t.Setenv("UPSTREAM_MODEL", "client-override-must-not-apply")
	t.Setenv("GATEWAY_KEYS", "")
	c, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c.ForcedModel != openCodeModel || c.UpstreamURL != openCodeZenURL || len(c.GatewayKeys) != 0 {
		t.Fatalf("config = %#v", c)
	}
}

func TestForwardReplacesClientAuthorizationWithUpstreamKey(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tokenrouter-test-key" {
			t.Errorf("authorization = %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	g, err := New(testConfig(upstream.URL), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
}

func TestGatewayAcceptsCredentialVariantsAndSetsAnthropicUpstreamKey(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if got := r.Header.Get("X-API-Key"); got != "tokenrouter-test-key" {
			t.Errorf("x-api-key = %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	g, err := New(testConfig(upstream.URL), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/anthropic/v1/messages", strings.NewReader(`{"model":"model-a","messages":[]}`))
	req.Header.Set("X-API-Key", "test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
}

func TestGatewayAcceptsLowercaseBearerScheme(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	g, err := New(testConfig(upstream.URL), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
}

func TestUpstreamTargetURLAvoidsDuplicateVersionPrefix(t *testing.T) {
	tests := []struct {
		baseURL string
		path    string
		want    string
	}{
		{baseURL: "https://api.tokenrouter.com/v1", path: "/v1/chat/completions", want: "https://api.tokenrouter.com/v1/chat/completions"},
		{baseURL: "https://api.tokenrouter.com/v1/", path: "/v1/models", want: "https://api.tokenrouter.com/v1/models"},
		{baseURL: "https://example.com", path: "/v1/models", want: "https://example.com/v1/models"},
	}
	for _, tt := range tests {
		if got := upstreamTargetURL(tt.baseURL, tt.path); got != tt.want {
			t.Errorf("upstreamTargetURL(%q, %q) = %q, want %q", tt.baseURL, tt.path, got, tt.want)
		}
	}
}

func TestMethodAllowedRejectsGetChatCompletions(t *testing.T) {
	for _, path := range []string{
		"/v1/chat/completions",
		"/v1/responses",
		"/openai/v1/chat/completions",
		"/openai/v1/responses",
		"/anthropic/v1/chat/completions",
		"/codex/v1/chat/completions",
	} {
		if allowed, allow := methodAllowed(path, http.MethodGet); allowed || allow != http.MethodPost {
			t.Errorf("methodAllowed(%q, GET) = (%v, %q), want (false, POST)", path, allowed, allow)
		}
	}
}

func TestMethodAllowedAcceptsModelsGetAndChatPost(t *testing.T) {
	if allowed, _ := methodAllowed("/v1/models", http.MethodGet); !allowed {
		t.Fatal("GET /v1/models should be allowed")
	}
	if allowed, _ := methodAllowed("/v1/chat/completions", http.MethodPost); !allowed {
		t.Fatal("POST /v1/chat/completions should be allowed")
	}
	if allowed, _ := methodAllowed("/v1/responses", http.MethodPost); !allowed {
		t.Fatal("POST /v1/responses should be allowed")
	}
}

func TestHandlerReturns405ForGetChatCompletions(t *testing.T) {
	g, err := New(testConfig("https://example.com"), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	if got := res.Header().Get("Allow"); got != http.MethodPost {
		t.Fatalf("Allow = %q, want POST", got)
	}
	var body map[string]string
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["error"] != "method_not_allowed" || body["method"] != http.MethodGet || body["path"] != "/v1/chat/completions" || body["allow"] != http.MethodPost {
		t.Fatalf("body = %#v", body)
	}
}

func TestHandlerReturns405ForGetResponses(t *testing.T) {
	g, err := New(testConfig("https://example.com"), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusMethodNotAllowed || res.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("status=%d allow=%q", res.Code, res.Header().Get("Allow"))
	}
}

func TestForwardDoesNotRetrySingleEgress429(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tokenrouter-test-key" {
			t.Errorf("authorization = %q", got)
		}
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer upstream.Close()
	g, err := New(testConfig(upstream.URL), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestForwardRetriesOnlyDistinctEgressAndPreservesModel(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, header := range []string{"X-OpenCode-Client", "X-OpenCode-Request", "X-Title"} {
			if got := r.Header.Get(header); got != "" {
				t.Errorf("%s = %q", header, got)
			}
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"model":"x","messages":[]}` {
			t.Errorf("body = %q", body)
		}
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	g, err := New(testConfig(upstream.URL), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: g.client, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: g.client, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
		{client: g.client, url: "socks5h://proxy:10803", egress: "192.0.2.3"},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK || calls.Load() != 3 {
		t.Fatalf("status = %d, calls = %d, body = %s", res.Code, calls.Load(), res.Body.String())
	}
}

func TestForwardStripsOpenCodeHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, header := range []string{"X-OpenCode-Client", "X-OpenCode-Request", "HTTP-Referer", "X-Title"} {
			if got := r.Header.Get(header); got != "" {
				t.Errorf("%s = %q", header, got)
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	g, err := New(testConfig(upstream.URL), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	req.Header.Set("User-Agent", "client-spoof")
	req.Header.Set("X-OpenCode-Client", "desktop")
	req.Header.Set("X-OpenCode-Request", "request-spoof")
	req.Header.Set("HTTP-Referer", "https://opencode.ai/")
	req.Header.Set("X-Title", "opencode")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
}

func TestNormalizeDeepSeekThinkingControls(t *testing.T) {
	g := &Gateway{cfg: DefaultConfig()}
	g.cfg.UpstreamProvider = ProviderOpenCode
	g.cfg.ForcedModel = ""
	g.cfg.DisableThinkingByDefault = true
	tests := []struct {
		name                string
		path                string
		body                string
		wantThinking        string
		wantReasoningEffort string
	}{
		{name: "chat default", path: "/v1/chat/completions", body: `{"model":"deepseek-v4-flash-free","messages":[]}`, wantThinking: "disabled", wantReasoningEffort: "none"},
		{name: "responses default", path: "/v1/responses", body: `{"model":"deepseek-v4-flash-free","input":"hello"}`, wantThinking: "disabled", wantReasoningEffort: "none"},
		{name: "legacy disabled bridge", path: "/v1/chat/completions", body: `{"model":"deepseek-v4-flash-free","thinking":{"type":"disabled"}}`, wantThinking: "disabled", wantReasoningEffort: "none"},
		{name: "enabled remains enabled", path: "/v1/chat/completions", body: `{"model":"deepseek-v4-flash-free","thinking":{"type":"enabled"}}`, wantThinking: "enabled"},
		{name: "explicit none", path: "/v1/chat/completions", body: `{"model":"deepseek-v4-flash-free","reasoning_effort":"none"}`, wantReasoningEffort: "none"},
		{name: "explicit low", path: "/v1/chat/completions", body: `{"model":"deepseek-v4-flash-free","reasoning_effort":"low"}`, wantReasoningEffort: "low"},
		{name: "explicit high", path: "/v1/responses", body: `{"model":"deepseek-v4-flash-free","reasoning_effort":"high"}`, wantReasoningEffort: "high"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := g.normalizeRequestBody(tt.path, []byte(tt.body))
			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatal(err)
			}
			thinkingType := ""
			if thinking, ok := payload["thinking"].(map[string]any); ok {
				thinkingType, _ = thinking["type"].(string)
			}
			if thinkingType != tt.wantThinking {
				t.Fatalf("thinking.type = %q, want %q; payload=%#v", thinkingType, tt.wantThinking, payload)
			}
			reasoningEffort, _ := payload["reasoning_effort"].(string)
			if reasoningEffort != tt.wantReasoningEffort {
				t.Fatalf("reasoning_effort = %q, want %q; payload=%#v", reasoningEffort, tt.wantReasoningEffort, payload)
			}
		})
	}
}

func TestNormalizeDeepSeekThinkingCanRemainUnchanged(t *testing.T) {
	g := &Gateway{cfg: DefaultConfig()}
	g.cfg.ForcedModel = ""
	g.cfg.DisableThinkingByDefault = false
	body := `{"model":"deepseek-v4-flash-free","messages":[]}`
	if got := string(g.normalizeRequestBody("/v1/chat/completions", []byte(body))); got != body {
		t.Fatalf("body = %s, want unchanged %s", got, body)
	}
}

func TestNormalizeResponsesPassesNativeInputThrough(t *testing.T) {
	g := &Gateway{cfg: DefaultConfig()}
	g.cfg.ForcedModel = ""
	g.cfg.FreeModelsOnly = false
	g.cfg.DisableThinkingByDefault = false
	g.cfg.MinThinkingMaxTokens = 0
	body := g.normalizeRequestBody("/v1/responses", []byte(`{
		"model":"model-a",
		"instructions":"Gateway instructions",
		"input":[
			{"role":"system","content":"Rules"},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"Hello"}],"id":"msg_from_client"},
			{"type":"input_text","text":"standalone content"}
		]
	}`))
	if string(body) == "" || !strings.Contains(string(body), `"input"`) || !strings.Contains(string(body), `"instructions"`) {
		t.Fatalf("native Responses input was changed: %s", body)
	}
}

func TestNormalizeResponsesPassesLegacyMessagesThrough(t *testing.T) {
	g := &Gateway{cfg: DefaultConfig()}
	g.cfg.ForcedModel = ""
	g.cfg.FreeModelsOnly = false
	g.cfg.DisableThinkingByDefault = false
	g.cfg.MinThinkingMaxTokens = 0
	body := g.normalizeRequestBody("/v1/responses", []byte(`{"model":"model-a","messages":[{"role":"user","content":"Hello"},{"role":"assistant","content":"Hi","id":"msg_client"}]}`))
	if got := string(body); got != `{"model":"model-a","messages":[{"role":"user","content":"Hello"},{"role":"assistant","content":"Hi","id":"msg_client"}]}` {
		t.Fatalf("legacy messages were changed: %s", got)
	}
}

func TestNormalizeResponsesConvertsNativeFunctionTools(t *testing.T) {
	g := &Gateway{cfg: DefaultConfig()}
	g.cfg.ForcedModel = ""
	g.cfg.FreeModelsOnly = false
	g.cfg.DisableThinkingByDefault = false
	g.cfg.MinThinkingMaxTokens = 0
	body, err := g.normalizeRequestBodyChecked("/v1/responses", []byte(`{
		"model":"model-a",
		"input":"hello",
		"tools":[{"type":"function","name":"get_weather","description":"Get weather","parameters":{"type":"object","properties":{}},"strict":true}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	tools, ok := payload["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools = %#v", payload["tools"])
	}
	tool := tools[0].(map[string]any)
	function, ok := tool["function"].(map[string]any)
	if !ok || function["name"] != "get_weather" {
		t.Fatalf("function = %#v", tool["function"])
	}
	if _, exists := tool["name"]; exists {
		t.Fatalf("native name was not moved: %#v", tool)
	}
}

func TestNormalizeResponsesRejectsFunctionToolWithoutName(t *testing.T) {
	g := &Gateway{cfg: DefaultConfig()}
	g.cfg.ForcedModel = ""
	g.cfg.FreeModelsOnly = false
	g.cfg.DisableThinkingByDefault = false
	g.cfg.MinThinkingMaxTokens = 0
	_, err := g.normalizeRequestBodyChecked("/v1/responses", []byte(`{"model":"model-a","input":"hello","tools":[{"type":"function","parameters":{"type":"object"}}]}`))
	if err == nil || !strings.Contains(err.Error(), "tool 0 name is required") {
		t.Fatalf("err = %v", err)
	}
}

func TestNormalizeResponsesConvertsFunctionToolChoice(t *testing.T) {
	g := &Gateway{cfg: DefaultConfig()}
	g.cfg.ForcedModel = ""
	g.cfg.FreeModelsOnly = false
	g.cfg.DisableThinkingByDefault = false
	g.cfg.MinThinkingMaxTokens = 0
	body, err := g.normalizeRequestBodyChecked("/v1/responses", []byte(`{"model":"model-a","input":"hello","tool_choice":{"type":"function","name":"get_weather"}}`))
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	choice, ok := payload["tool_choice"].(map[string]any)
	if !ok {
		t.Fatalf("tool_choice = %#v", payload["tool_choice"])
	}
	function, ok := choice["function"].(map[string]any)
	if !ok || function["name"] != "get_weather" {
		t.Fatalf("function = %#v", choice["function"])
	}
	if _, exists := choice["name"]; exists {
		t.Fatalf("native tool_choice name was not moved: %#v", choice)
	}
}

func TestNormalizeDeepSeekThinkingRaisesLowTokenBudgets(t *testing.T) {
	g := &Gateway{cfg: DefaultConfig()}
	g.cfg.UpstreamProvider = ProviderOpenCode
	g.cfg.ForcedModel = ""
	g.cfg.MinThinkingMaxTokens = 8192
	tests := []struct {
		name      string
		path      string
		body      string
		budgetKey string
		want      float64
	}{
		{name: "thinking enabled chat", path: "/v1/chat/completions", body: `{"model":"deepseek-v4-flash-free","thinking":{"type":"enabled"},"max_tokens":1024}`, budgetKey: "max_tokens", want: 8192},
		{name: "reasoning high completion budget", path: "/v1/chat/completions", body: `{"model":"deepseek-v4-flash-free","reasoning_effort":"high","max_completion_tokens":2048}`, budgetKey: "max_completion_tokens", want: 8192},
		{name: "responses output budget", path: "/v1/responses", body: `{"model":"deepseek-v4-flash-free","reasoning_effort":"low","max_output_tokens":1024}`, budgetKey: "max_output_tokens", want: 8192},
		{name: "missing chat budget", path: "/v1/chat/completions", body: `{"model":"deepseek-v4-flash-free","thinking":{"type":"enabled"}}`, budgetKey: "max_tokens", want: 8192},
		{name: "sufficient budget unchanged", path: "/v1/chat/completions", body: `{"model":"deepseek-v4-flash-free","reasoning_effort":"high","max_tokens":16384}`, budgetKey: "max_tokens", want: 16384},
		{name: "reasoning none unchanged", path: "/v1/chat/completions", body: `{"model":"deepseek-v4-flash-free","reasoning_effort":"none","max_tokens":1024}`, budgetKey: "max_tokens", want: 1024},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := g.normalizeRequestBody(tt.path, []byte(tt.body))
			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatal(err)
			}
			if got, _ := payload[tt.budgetKey].(float64); got != tt.want {
				t.Fatalf("%s = %v, want %v; payload=%#v", tt.budgetKey, got, tt.want, payload)
			}
		})
	}
}

func TestNormalizeDeepSeekThinkingBudgetGuardCanBeDisabled(t *testing.T) {
	g := &Gateway{cfg: DefaultConfig()}
	g.cfg.ForcedModel = ""
	g.cfg.MinThinkingMaxTokens = 0
	body := `{"model":"deepseek-v4-flash-free","thinking":{"type":"enabled"},"max_tokens":1024}`
	if got := string(g.normalizeRequestBody("/v1/chat/completions", []byte(body))); got != body {
		t.Fatalf("body = %s, want unchanged %s", got, body)
	}
}

func TestForwardAddsContextToUpstreamResponseEOF(t *testing.T) {
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(&errorReader{err: io.ErrUnexpectedEOF}),
		}, nil
	})}
	c := testConfig("https://example.com")
	c.MaxRetries = 0
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	failing := &proxySlot{client: client, egress: "192.0.2.1"}
	healthy := &proxySlot{client: g.client, url: "socks5h://proxy:10802", egress: "192.0.2.2"}
	g.slots = []*proxySlot{failing, healthy}
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	if g.stats.Success.Load() != 0 || g.stats.Errors.Load() != 1 {
		t.Fatalf("stats success=%d errors=%d", g.stats.Success.Load(), g.stats.Errors.Load())
	}
	if failing.fails == 0 || g.active.Load() != 1 {
		t.Fatalf("failing slot=%#v active=%d", failing, g.active.Load())
	}
	g.logsMu.RLock()
	logs := append([]systemLog(nil), g.logs...)
	g.logsMu.RUnlock()
	if len(logs) == 0 || !strings.Contains(logs[len(logs)-1].Message, "forward failed") || !strings.Contains(logs[len(logs)-1].Fields["error"].(string), "upstream response body") {
		t.Fatalf("logs = %#v", logs)
	}
}

func TestForwardRetriesNonStreamingTruncatedResponseOnNextEgress(t *testing.T) {
	firstClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(&errorReader{err: io.ErrUnexpectedEOF})}, nil
	})}
	secondClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
	})}
	c := testConfig("https://example.com")
	c.MaxRetries = 1
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: firstClient, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: secondClient, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[],"stream":false}`))
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK || res.Body.String() != `{"ok":true}` {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	if g.active.Load() != 1 || g.stats.Success.Load() != 1 || g.stats.Errors.Load() != 0 {
		t.Fatalf("active=%d success=%d errors=%d", g.active.Load(), g.stats.Success.Load(), g.stats.Errors.Load())
	}
	g.auditMu.RLock()
	audits := append([]auditRecord(nil), g.audits...)
	g.auditMu.RUnlock()
	if len(audits) != 2 || audits[0].Status != http.StatusBadGateway || audits[1].Status != http.StatusOK || audits[1].Attempts != 2 {
		t.Fatalf("audits=%#v", audits)
	}
	if audits[0].RequestID == "" || audits[0].RequestID != audits[1].RequestID || res.Header().Get("X-Gateway-Request-ID") != audits[0].RequestID {
		t.Fatalf("request IDs: header=%q audits=%#v", res.Header().Get("X-Gateway-Request-ID"), audits)
	}
}

func TestForwardRetriesStreamingResponseBeforeFirstOutput(t *testing.T) {
	firstCalls, secondCalls := 0, 0
	firstClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		firstCalls++
		header := make(http.Header)
		header.Set("X-Upstream-Attempt", "first")
		header.Set("Content-Length", "1")
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(&errorReader{err: io.ErrUnexpectedEOF})}, nil
	})}
	secondClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		secondCalls++
		body := "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}\n\ndata: [DONE]\n\n"
		header := make(http.Header)
		header.Set("X-Upstream-Attempt", "second")
		header.Set("Content-Length", strconv.Itoa(len(body)))
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	c := testConfig("https://example.com")
	c.MaxRetries = 1
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: firstClient, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: secondClient, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[],"stream":true}`))
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"content":"OK"`) || firstCalls != 1 || secondCalls != 1 {
		t.Fatalf("status=%d first=%d second=%d body=%s", res.Code, firstCalls, secondCalls, res.Body.String())
	}
	if got := res.Header().Values("X-Upstream-Attempt"); len(got) != 1 || got[0] != "second" || res.Header().Get("Content-Length") != "" {
		t.Fatalf("response headers leaked across retry: %#v", res.Header())
	}
	g.auditMu.RLock()
	audits := append([]auditRecord(nil), g.audits...)
	g.auditMu.RUnlock()
	if len(audits) != 2 || audits[0].Status != http.StatusBadGateway || audits[1].Status != http.StatusOK || audits[1].Attempts != 2 {
		t.Fatalf("audits = %#v", audits)
	}
	if audits[0].RequestID == "" || audits[0].RequestID != audits[1].RequestID || res.Header().Get("X-Gateway-Request-ID") != audits[0].RequestID {
		t.Fatalf("request IDs: header=%q audits=%#v", res.Header().Get("X-Gateway-Request-ID"), audits)
	}
}

func TestForwardRetriesStreamingResponseAfterFirstOutputTimeout(t *testing.T) {
	firstCalls, secondCalls := 0, 0
	firstReader, firstWriter := io.Pipe()
	defer firstWriter.Close()
	firstClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		firstCalls++
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: firstReader}, nil
	})}
	secondClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		secondCalls++
		body := "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\ndata: [DONE]\n\n"
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	c := testConfig("https://example.com")
	c.MaxRetries = 1
	c.StreamFirstOutputTimeout = 5 * time.Millisecond
	c.StreamFailureCooldown = time.Minute
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: firstClient, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: secondClient, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[],"stream":true}`))
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"content":"OK"`) || firstCalls != 1 || secondCalls != 1 {
		t.Fatalf("status=%d first=%d second=%d body=%s", res.Code, firstCalls, secondCalls, res.Body.String())
	}
	disabled, ready := g.slots[0].readiness("model-a", time.Now())
	if disabled || !ready.After(time.Now().Add(30*time.Second)) {
		t.Fatalf("failed slot was not cooled for stream failure: disabled=%v ready=%s", disabled, ready)
	}
}

type errorReader struct{ err error }

func (r *errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestModelCooldownDoesNotBlockOtherModelsOnSameEgress(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"model":"model-a"`) {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	c := testConfig(upstream.URL)
	c.MaxRetries = 0
	c.CooldownBase = time.Minute
	c.CooldownMax = time.Minute
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	request := func(model string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"`+model+`","messages":[]}`))
		req.Header.Set("Authorization", "Bearer test-key")
		res := httptest.NewRecorder()
		g.Handler().ServeHTTP(res, req)
		return res
	}
	if res := request("model-a"); res.Code != http.StatusTooManyRequests {
		t.Fatalf("model-a status = %d", res.Code)
	}
	if res := request("model-b"); res.Code != http.StatusOK {
		t.Fatalf("model-b status = %d, body = %s", res.Code, res.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestModel429CoolsAllEgresses(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer upstream.Close()
	c := testConfig(upstream.URL)
	c.MaxRetries = 2
	c.CooldownMax = time.Minute
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: g.client, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: g.client, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[]}`))
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusTooManyRequests || calls.Load() != 1 {
		t.Fatalf("status = %d, calls = %d, body = %s", res.Code, calls.Load(), res.Body.String())
	}
	for index, slot := range g.slots {
		_, ready := slot.readiness("model-a", time.Now())
		if !ready.After(time.Now().Add(30 * time.Second)) {
			t.Fatalf("slot %d was not cooled: ready=%s", index, ready)
		}
	}
}

func Test429DoesNotChangeActiveEgress(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer upstream.Close()
	c := testConfig(upstream.URL)
	c.MaxRetries = 1
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: g.client, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: g.client, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
	}
	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[]}`))
		req.Header.Set("Authorization", "Bearer test-key")
		res := httptest.NewRecorder()
		g.Handler().ServeHTTP(res, req)
		return res
	}
	if res := request(); res.Code != http.StatusTooManyRequests {
		t.Fatalf("first status = %d, body = %s", res.Code, res.Body.String())
	}
	if got := g.active.Load(); got != 0 {
		t.Fatalf("active changed after 429 = %d, want 0", got)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestOpenCode429RetriesNextEgress(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	c := testConfig(upstream.URL)
	c.UpstreamProvider = ProviderOpenCode
	c.MaxRetries = 1
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: g.client, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: g.client, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[]}`))
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("status = %d, calls = %d, body = %s", res.Code, calls.Load(), res.Body.String())
	}
	if got := g.active.Load(); got != 1 {
		t.Fatalf("active egress index = %d, want 1", got)
	}
}

func TestSlotSelectionSticksUntilCurrentEgressIsExcluded(t *testing.T) {
	g, err := New(testConfig("https://example.com"), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: g.client, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: g.client, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
		{client: g.client, url: "socks5h://proxy:10803", egress: "192.0.2.3"},
	}
	for i := 0; i < 3; i++ {
		slot, _ := g.selectSlotExcluding("model-free", nil)
		if slot != g.slots[0] {
			t.Fatalf("ordinary request %d selected %q", i+1, slot.url)
		}
	}
	excluded := map[string]struct{}{g.slots[0].identity(): {}}
	slot, _ := g.selectSlotExcluding("model-free", excluded)
	if slot != g.slots[1] {
		t.Fatalf("failover selected %q", slot.url)
	}
	slot, _ = g.selectSlotExcluding("model-free", nil)
	if slot != g.slots[1] {
		t.Fatalf("selection did not remain on failover slot: %q", slot.url)
	}
}

func TestSummaryMarksOnlyCurrentSlotActive(t *testing.T) {
	g, err := New(testConfig("https://example.com"), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{{client: g.client, url: "proxy-a"}, {client: g.client, url: "proxy-b"}}
	g.active.Store(1)
	encoded, _ := json.Marshal(g.summary())
	var summary struct {
		Slots []struct {
			Active bool `json:"active"`
		} `json:"slots"`
	}
	if err := json.Unmarshal(encoded, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Slots[0].Active || !summary.Slots[1].Active {
		t.Fatalf("active slots = %#v", summary.Slots)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestStaticProxyProbeRecordsAndDeduplicatesEgress(t *testing.T) {
	config := testConfig("https://example.com")
	config.ProxyProbeURLs = []string{"https://probe.example"}
	config.InstanceAdminToken = "internal"
	g, err := New(config, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	slot := func(raw, ip string) *proxySlot {
		client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(ip))}, nil
		})}
		return &proxySlot{client: client, url: raw}
	}
	g.slots = []*proxySlot{slot("socks5h://mihomo:10801", "192.0.2.1"), slot("socks5h://mihomo:10802", "192.0.2.1"), slot("socks5h://mihomo:10803", "192.0.2.2")}
	g.probeAndDeduplicateStaticSlots()
	if len(g.slots) != 3 || g.slots[0].egress != "192.0.2.1" || !g.slots[1].disabled || g.slots[2].egress != "192.0.2.2" {
		t.Fatalf("slots = %#v", g.slots)
	}
}

func TestStaticProxyDedupPrefersExplicitProxyOverDirect(t *testing.T) {
	config := testConfig("https://example.com")
	config.ProxyProbeURLs = []string{"https://probe.example"}
	g, err := New(config, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	slot := func(raw string) *proxySlot {
		client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("192.0.2.8"))}, nil
		})}
		return &proxySlot{client: client, url: raw}
	}
	g.slots = []*proxySlot{slot(""), slot("socks5h://mihomo:10801")}

	g.probeAndDeduplicateStaticSlots()

	if len(g.slots) != 2 || !g.slots[0].disabled || g.slots[1].url != "socks5h://mihomo:10801" || g.slots[1].egress != "192.0.2.8" {
		t.Fatalf("slots = %#v", g.slots)
	}
}

func TestStaticProxyProbeMarksFailuresUnavailableAndHealthNeedsOneHealthySlot(t *testing.T) {
	config := testConfig("https://example.com")
	config.ProxyProbeURLs = []string{"https://probe.example"}
	failed := &proxySlot{url: "socks5h://mihomo:10801", disabled: true, client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("certificate mismatch")
	})}}
	healthy := &proxySlot{url: "socks5h://mihomo:10802", disabled: true, client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("192.0.2.20"))}, nil
	})}}
	g := &Gateway{cfg: config, slots: []*proxySlot{failed, healthy}}
	g.probeAndDeduplicateStaticSlots()
	if !failed.disabled || failed.probeError == "" {
		t.Fatalf("failed slot = %#v", failed)
	}
	if healthy.disabled || healthy.egress != "192.0.2.20" {
		t.Fatalf("healthy slot = %#v", healthy)
	}
	res := httptest.NewRecorder()
	g.health(res, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("health status = %d, body = %s", res.Code, res.Body.String())
	}
}

func TestHealthReportsUnavailableWhileAllSlotsAreCoolingDown(t *testing.T) {
	g := &Gateway{slots: []*proxySlot{{
		url:    "socks5h://mihomo:10801",
		egress: "192.0.2.10",
		until:  time.Now().Add(time.Minute),
	}}}
	response := httptest.NewRecorder()
	g.health(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("cooling slot health status = %d, body = %s", response.Code, response.Body.String())
	}

	g.slots[0].until = time.Time{}
	response = httptest.NewRecorder()
	g.health(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("ready slot health status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestProbeDirectSlotRecordsEgress(t *testing.T) {
	config := testConfig("https://example.com")
	config.ProxyProbeURLs = []string{"https://probe.example"}
	g, err := New(config, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	direct := &proxySlot{client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("192.0.2.9"))}, nil
	})}}
	g.slots = []*proxySlot{direct}

	g.probeDirectSlot(direct)

	if direct.egress != "192.0.2.9" {
		t.Fatalf("egress = %q", direct.egress)
	}
}

func TestAdminProbeRefreshesSelectedSlotEgress(t *testing.T) {
	config := testConfig("https://example.com")
	config.ProxyProbeURLs = []string{"https://probe.example"}
	config.InstanceAdminToken = "internal"
	g, err := New(config, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("192.0.2.44"))}, nil
	})}
	g.slots = []*proxySlot{{client: client, url: "socks5h://mihomo:10801", egress: "192.0.2.10", fails: 2, until: time.Now().Add(time.Minute), modelCooldowns: map[string]cooldownState{"model-free": {Fails: 1, Until: time.Now().Add(time.Minute)}}}}
	req := httptest.NewRequest(http.MethodPost, "/admin/probe", strings.NewReader(`{"url":"socks5h://mihomo:10801"}`))
	req.Header.Set("Authorization", "Bearer internal")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	if g.slots[0].egress != "192.0.2.44" || !strings.Contains(res.Body.String(), "192.0.2.44") {
		t.Fatalf("slot = %#v, body = %s", g.slots[0], res.Body.String())
	}
	if g.slots[0].fails != 0 || !g.slots[0].until.IsZero() || len(g.slots[0].modelCooldowns) != 0 {
		t.Fatalf("old cooldown was retained after egress change: %#v", g.slots[0])
	}
}

func TestAdminRotateAdvancesStickySlotAndSkipsForbiddenEgress(t *testing.T) {
	config := testConfig("https://example.com")
	config.InstanceAdminToken = "internal"
	g, err := New(config, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: g.client, url: "proxy-a", egress: "192.0.2.1"},
		{client: g.client, url: "proxy-b", egress: "192.0.2.2"},
		{client: g.client, url: "proxy-c", egress: "192.0.2.3"},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/rotate", strings.NewReader(`{"forbidden":["192.0.2.2"]}`))
	req.Header.Set("Authorization", "Bearer internal")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK || g.active.Load() != 2 || !strings.Contains(res.Body.String(), "192.0.2.3") {
		t.Fatalf("status = %d, active = %d, body = %s", res.Code, g.active.Load(), res.Body.String())
	}
}

func TestParseProbeIPSupportsPlainAndCloudflareTrace(t *testing.T) {
	for input, want := range map[string]string{
		"192.0.2.9\n":                    "192.0.2.9",
		"fl=1\nip=2001:db8::9\nloc=US\n": "2001:db8::9",
		"not an ip":                      "",
	} {
		if got := parseProbeIP([]byte(input)); got != want {
			t.Errorf("parseProbeIP(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestProbeSlotFallsBackToNextURL(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("first probe unavailable")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("ip=192.0.2.22\nloc=US\n")),
		}, nil
	})}
	g := &Gateway{cfg: Config{
		ProxyProbeURLs: []string{"https://first.example", "https://second.example"},
		ProxyProbeWait: time.Second,
	}}
	ip, err := g.probeSlot(&proxySlot{client: client, url: "socks5h://proxy:1080"})
	if err != nil {
		t.Fatal(err)
	}
	if ip != "192.0.2.22" || calls.Load() != 2 {
		t.Fatalf("ip = %q, calls = %d", ip, calls.Load())
	}
}

func TestUnauthorizedAndQueueLimit(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	c := testConfig(upstream.URL)
	c.MaxConcurrency = 1
	c.QueueSize = 0
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	unauth := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	got := httptest.NewRecorder()
	g.Handler().ServeHTTP(got, unauth)
	if got.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", got.Code)
	}
	first := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	first.Header.Set("Authorization", "Bearer test-key")
	done := make(chan struct{})
	go func() { r := httptest.NewRecorder(); g.Handler().ServeHTTP(r, first); close(done) }()
	time.Sleep(5 * time.Millisecond)
	second := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	second.Header.Set("Authorization", "Bearer test-key")
	r2 := httptest.NewRecorder()
	g.Handler().ServeHTTP(r2, second)
	if r2.Code != http.StatusTooManyRequests {
		t.Fatalf("queue status = %d", r2.Code)
	}
	<-done
}

func TestBuildProxySlotsAcceptsSOCKS5(t *testing.T) {
	g, err := New(testConfig("https://example.com"), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	slots, err := g.buildProxySlots([]string{"socks5h://user:pass@127.0.0.1:1080"})
	if err != nil {
		t.Fatal(err)
	}
	if len(slots) != 1 || slots[0].url != "socks5h://user:pass@127.0.0.1:1080" {
		t.Fatalf("unexpected slots: %#v", slots)
	}
}

func TestBuildProxySlotsRejectsVLESS(t *testing.T) {
	g, err := New(testConfig("https://example.com"), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.buildProxySlots([]string{"vless://uuid@example.com:443"}); err == nil {
		t.Fatal("expected vless URL rejection")
	}
}

func TestDialSOCKS5PassesHostnameToProxy(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	targets := make(chan string, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		greeting := make([]byte, 3)
		_, _ = io.ReadFull(conn, greeting)
		_, _ = conn.Write([]byte{0x05, 0x00})
		header := make([]byte, 5)
		_, _ = io.ReadFull(conn, header)
		host := make([]byte, int(header[4]))
		_, _ = io.ReadFull(conn, host)
		port := make([]byte, 2)
		_, _ = io.ReadFull(conn, port)
		targets <- net.JoinHostPort(string(host), strconv.Itoa(int(binary.BigEndian.Uint16(port))))
		_, _ = conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0})
	}()

	conn, err := dialSOCKS5(context.Background(), "tcp", listener.Addr().String(), "opencode.ai:443", "", "")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if target := <-targets; target != "opencode.ai:443" {
		t.Fatalf("target = %q", target)
	}
}

func TestTokenUsageParsesRegularAndStreamingResponses(t *testing.T) {
	regular := parseTokenUsage([]byte(`{"usage":{"prompt_tokens":12,"completion_tokens":8,"total_tokens":20,"prompt_tokens_details":{"cached_tokens":4}}}`))
	if regular != (tokenUsage{PromptTokens: 12, CompletionTokens: 8, TotalTokens: 20, CachedTokens: 4}) {
		t.Fatalf("regular usage = %#v", regular)
	}
	responses := parseTokenUsage([]byte(`{"response":{"usage":{"input_tokens":15,"output_tokens":9,"total_tokens":24,"input_tokens_details":{"cached_tokens":6}}}}`))
	if responses != (tokenUsage{PromptTokens: 15, CompletionTokens: 9, TotalTokens: 24, CachedTokens: 6}) {
		t.Fatalf("responses usage = %#v", responses)
	}
	pro := parseTokenUsage([]byte(`{"usage":{"prompt_cache_hit_tokens":11,"prompt_cache_miss_tokens":5,"completion_tokens":9,"total_tokens":25}}`))
	if pro != (tokenUsage{PromptTokens: 16, CompletionTokens: 9, TotalTokens: 25, CachedTokens: 11}) {
		t.Fatalf("pro usage = %#v", pro)
	}
	if model := requestModel([]byte(`{"model":"deepseek-v4-flash-free","input":"hello"}`)); model != "deepseek-v4-flash-free" {
		t.Fatalf("responses model = %q", model)
	}
	if !streamingRequest([]byte(`{"model":"deepseek-v4-flash-free","input":"hello","stream":true}`)) {
		t.Fatal("responses stream was not detected")
	}
	if !sseLineHasOutput(`data: {"type":"response.output_text.delta","delta":"OK"}`) {
		t.Fatal("responses output event was not detected")
	}

	response := httptest.NewRecorder()
	stream := strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":15,\"completion_tokens\":9,\"total_tokens\":24}}\n\ndata: [DONE]\n\n")
	usage, firstTokenMS, committed, err := copyResponse(response, stream, time.Now().Add(-20*time.Millisecond), true)
	if err != nil {
		t.Fatal(err)
	}
	if usage != (tokenUsage{PromptTokens: 15, CompletionTokens: 9, TotalTokens: 24}) {
		t.Fatalf("stream usage = %#v", usage)
	}
	if firstTokenMS < 10 {
		t.Fatalf("first token latency = %dms", firstTokenMS)
	}
	if !committed {
		t.Fatal("stream response was not committed after its first output token")
	}
	if !strings.Contains(response.Body.String(), "data: [DONE]") {
		t.Fatalf("stream was not forwarded: %q", response.Body.String())
	}

	response = httptest.NewRecorder()
	responsesStream := strings.NewReader("event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"OK\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}\n\n")
	usage, _, committed, err = copyStreamResponse(response, responsesStream, time.Now(), true, "/v1/responses", "deepseek-v4-flash-free", 0)
	if err != nil {
		t.Fatal(err)
	}
	if usage != (tokenUsage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}) {
		t.Fatalf("responses stream usage = %#v", usage)
	}
	if !committed || !strings.Contains(response.Body.String(), `"response.output_text.delta"`) {
		t.Fatalf("responses stream was not committed: %q", response.Body.String())
	}
}

func TestCopyResponseAddsFinishReasonBeforeDoneWhenUpstreamOmitsIt(t *testing.T) {
	response := httptest.NewRecorder()
	stream := strings.NewReader("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"OK\"}}]}\n\ndata: [DONE]\n\n")

	_, _, committed, err := copyResponse(response, stream, time.Now(), true)
	if err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("stream response was not committed")
	}
	body := response.Body.String()
	finishAt := strings.Index(body, `"finish_reason":"stop"`)
	doneAt := strings.Index(body, "data: [DONE]")
	if finishAt < 0 || doneAt < 0 || finishAt > doneAt {
		t.Fatalf("terminal finish chunk must precede [DONE], body = %q", body)
	}
}

func TestCopyResponseDoesNotDuplicateUpstreamFinishReason(t *testing.T) {
	response := httptest.NewRecorder()
	stream := strings.NewReader("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"OK\"}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")

	_, _, committed, err := copyResponse(response, stream, time.Now(), true)
	if err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("stream response was not committed")
	}
	if count := strings.Count(response.Body.String(), `"finish_reason":"stop"`); count != 1 {
		t.Fatalf("finish reason count = %d, body = %q", count, response.Body.String())
	}
}

func TestResponsesStreamPreservesProtocolAndToolEventsCommit(t *testing.T) {
	response := httptest.NewRecorder()
	stream := strings.NewReader("event: response.function_call_arguments.delta\n" +
		"data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"{\\\"city\\\":\\\"Seoul\\\"}\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5}}}\n\n")
	usage, _, committed, err := copyStreamResponse(response, stream, time.Now(), true, "/v1/responses", "deepseek-v4-flash-free", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !committed || usage != (tokenUsage{PromptTokens: 2, CompletionTokens: 3, TotalTokens: 5}) {
		t.Fatalf("committed=%v usage=%#v", committed, usage)
	}
	body := response.Body.String()
	if !strings.Contains(body, "response.function_call_arguments.delta") || strings.Contains(body, `"finish_reason":"stop"`) {
		t.Fatalf("responses protocol was changed: %q", body)
	}
}

func TestResponsesStreamAddsCreatedSnapshotAndCompletesIt(t *testing.T) {
	response := httptest.NewRecorder()
	stream := strings.NewReader("event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":7,\"output_tokens\":2,\"total_tokens\":9}}}\n\n")
	usage, _, committed, err := copyStreamResponse(response, stream, time.Now(), true, "/v1/responses", "deepseek-v4-flash-free", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !committed || usage != (tokenUsage{PromptTokens: 7, CompletionTokens: 2, TotalTokens: 9}) {
		t.Fatalf("committed=%v usage=%#v", committed, usage)
	}
	body := response.Body.String()
	sequence := []string{
		"event: response.created", "event: response.output_item.added", "event: response.content_part.added",
		"event: response.output_text.delta", "event: response.output_text.done", "event: response.content_part.done",
		"event: response.output_item.done", "event: response.completed",
	}
	previous := -1
	for _, event := range sequence {
		position := strings.Index(body, event)
		if position < 0 || position <= previous {
			t.Fatalf("responses event sequence is invalid at %q: %q", event, body)
		}
		previous = position
	}
	if !strings.HasPrefix(body, "event: response.created\ndata: ") || strings.Count(body, "event: response.created") != 1 {
		t.Fatalf("created event missing or duplicated: %q", body)
	}
	lines := strings.Split(body, "\n")
	var created, completed struct {
		Type     string `json:"type"`
		Response struct {
			ID     string `json:"id"`
			Model  string `json:"model"`
			Status string `json:"status"`
			Output []struct {
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
			} `json:"output"`
			Usage struct {
				InputTokens  int64 `json:"input_tokens"`
				OutputTokens int64 `json:"output_tokens"`
			} `json:"usage"`
		} `json:"response"`
	}
	for index, line := range lines {
		if line == "event: response.created" && index+1 < len(lines) {
			if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[index+1], "data: ")), &created); err != nil {
				t.Fatal(err)
			}
		}
		if line == "event: response.completed" && index+1 < len(lines) {
			if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[index+1], "data: ")), &completed); err != nil {
				t.Fatal(err)
			}
		}
	}
	if created.Type != "response.created" || created.Response.ID == "" || created.Response.Model != "deepseek-v4-flash-free" || created.Response.Status != "in_progress" {
		t.Fatalf("created snapshot = %#v", created)
	}
	if completed.Type != "response.completed" || completed.Response.ID != created.Response.ID || completed.Response.Status != "completed" || completed.Response.Usage.InputTokens != 7 || completed.Response.Usage.OutputTokens != 2 || len(completed.Response.Output) != 1 || completed.Response.Output[0].Content[0].Text != "Hello" {
		t.Fatalf("completed snapshot = %#v", completed)
	}
	var delta struct {
		Type         string `json:"type"`
		ItemID       string `json:"item_id"`
		OutputIndex  int    `json:"output_index"`
		ContentIndex int    `json:"content_index"`
		Delta        string `json:"delta"`
	}
	for index, line := range lines {
		if line == "event: response.output_text.delta" && index+1 < len(lines) {
			if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[index+1], "data: ")), &delta); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if delta.Type != "response.output_text.delta" || delta.ItemID == "" || delta.OutputIndex != 0 || delta.ContentIndex != 0 || delta.Delta != "Hello" {
		t.Fatalf("delta was not normalized: %#v", delta)
	}
}

func TestResponsesStreamDoesNotDuplicateUpstreamCreatedEvent(t *testing.T) {
	response := httptest.NewRecorder()
	stream := strings.NewReader("event: response.created\n" +
		"data: {\"type\":\"response.created\",\"response\":{\"id\":\"upstream-response\"}}\n\n" +
		"event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"OK\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{}}\n\n")
	_, _, committed, err := copyStreamResponse(response, stream, time.Now(), true, "/v1/responses", "model-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !committed || strings.Count(response.Body.String(), "event: response.created") != 1 {
		t.Fatalf("created event was duplicated: %q", response.Body.String())
	}
}

func TestCopyStreamResponseTimesOutBeforeFirstOutput(t *testing.T) {
	response := httptest.NewRecorder()
	_, _, committed, err := copyStreamResponse(response, strings.NewReader(""), time.Now().Add(-time.Second), true, "/v1/chat/completions", "", time.Millisecond)
	if committed || !errors.Is(err, errUpstreamFirstOutputTimeout) {
		t.Fatalf("committed=%v err=%v", committed, err)
	}
}

func TestCopyStreamResponseRejectsChatEOFWithoutDoneAfterOutput(t *testing.T) {
	response := httptest.NewRecorder()
	stream := strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
	_, _, committed, err := copyResponse(response, stream, time.Now(), true)
	if !committed || !errors.Is(err, errUpstreamResponseRead) {
		t.Fatalf("committed=%v err=%v", committed, err)
	}
}

func TestCopyStreamResponseRejectsResponsesEOFWithoutCompleted(t *testing.T) {
	response := httptest.NewRecorder()
	stream := strings.NewReader("event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n")
	_, _, committed, err := copyStreamResponse(response, stream, time.Now(), true, "/v1/responses", "model-a", time.Second)
	if !committed || !errors.Is(err, errUpstreamResponseRead) {
		t.Fatalf("committed=%v err=%v", committed, err)
	}
}

func TestNormalizeResponsesPassesUnsupportedContentPartThrough(t *testing.T) {
	g := &Gateway{cfg: DefaultConfig()}
	g.cfg.ForcedModel = ""
	body := `{"model":"model-a","input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.com/image.png"}]}]}`
	got, err := g.normalizeRequestBodyChecked("/v1/responses", []byte(body))
	if err != nil || string(got) != body {
		t.Fatalf("body=%s err=%v", got, err)
	}
}

func TestResponsesStreamKeepsFunctionCallInCompletedSnapshot(t *testing.T) {
	response := httptest.NewRecorder()
	stream := strings.NewReader("event: response.function_call_arguments.delta\n" +
		"data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"output_index\":0,\"delta\":\"{\\\"city\\\":\\\"Seoul\\\"}\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5}}}\n\n")
	_, _, committed, err := copyStreamResponse(response, stream, time.Now(), true, "/v1/responses", "model-a", time.Second)
	if err != nil || !committed {
		t.Fatalf("committed=%v err=%v body=%q", committed, err, response.Body.String())
	}
	body := response.Body.String()
	if strings.Index(body, "event: response.output_item.added") > strings.Index(body, "event: response.function_call_arguments.delta") {
		t.Fatalf("output item was added after its delta event: %q", body)
	}
	if !strings.Contains(body, `"type":"function_call"`) || !strings.Contains(body, `"arguments":"{\"city\":\"Seoul\"}"`) {
		t.Fatalf("function call was lost from terminal response: %q", body)
	}
}

func TestResponsesStreamPreservesCompleteUpstreamSnapshot(t *testing.T) {
	response := httptest.NewRecorder()
	stream := strings.NewReader("event: response.created\n" +
		"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_upstream\",\"model\":\"model-a\",\"output\":[]}}\n\n" +
		"event: response.output_item.added\n" +
		"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_upstream\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
		"event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_upstream\",\"output_index\":0,\"content_index\":0,\"delta\":\"OK\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_upstream\",\"model\":\"model-a\",\"status\":\"completed\",\"output\":[{\"id\":\"msg_upstream\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"OK\",\"annotations\":[]}]}],\"usage\":{\"input_tokens\":3,\"output_tokens\":1,\"total_tokens\":4}}}\n\n")
	_, _, committed, err := copyStreamResponse(response, stream, time.Now(), true, "/v1/responses", "model-a", time.Second)
	if err != nil || !committed {
		t.Fatalf("committed=%v err=%v body=%q", committed, err, response.Body.String())
	}
	body := response.Body.String()
	if strings.Count(body, "resp_upstream") != 2 || !strings.Contains(body, `"text":"OK"`) {
		t.Fatalf("upstream completed snapshot was rewritten: %q", body)
	}
}

func TestForwardDropsUpstreamContentLengthForTransformedStream(t *testing.T) {
	body := "event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"OK\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{}}\n\n"
	upstream := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Content-Type", "text/event-stream")
		header.Set("Content-Length", strconv.Itoa(len(body)))
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	c := testConfig("https://example.com")
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{{client: upstream}}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"hello","stream":true}`))
	req.Header.Set("Authorization", "Bearer test-key")
	response := httptest.NewRecorder()
	g.Handler().ServeHTTP(response, req)
	if response.Code != http.StatusOK || response.Header().Get("Content-Length") != "" || !strings.Contains(response.Body.String(), "response.created") {
		t.Fatalf("status=%d content-length=%q body=%q", response.Code, response.Header().Get("Content-Length"), response.Body.String())
	}
}

func TestHasUntriedSlotIncludesCoolingCandidate(t *testing.T) {
	g := &Gateway{slots: []*proxySlot{
		{client: &http.Client{}, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: &http.Client{}, url: "socks5h://proxy:10802", egress: "192.0.2.2", until: time.Now().Add(time.Second)},
	}}
	excluded := map[string]struct{}{g.slots[0].identity(): {}}
	if !g.hasUntriedSlot("model-a", excluded) {
		t.Fatal("cooling alternative must remain eligible for waitForSlotExcluding")
	}
}

func TestMaskGatewayKeyDoesNotExposePlaintext(t *testing.T) {
	masked := maskGatewayKey("gw_1234567890abcdef")
	if masked != "gw_...cdef" || strings.Contains(masked, "1234567890") {
		t.Fatalf("masked key = %q", masked)
	}
}
