package controlplane

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateUsesFixedImageNetworkAndManagedLabel(t *testing.T) {
	t.Setenv("MAX_RETRIES", "0")
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/v1.43/containers/create") {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, `{"Id":"abc"}`)
	}))
	defer server.Close()
	docker := &dockerClient{client: server.Client(), base: server.URL}
	cfg := Config{GatewayImage: "fixed/gateway:test", DockerNetwork: "gateway-network", InstanceToken: "internal"}
	instance := Instance{Name: "gateway-b", ProxyURLs: []string{"http://proxy:18081"}, MaxConcurrency: 4, QueueSize: 8}
	if err := docker.create(cfg, instance, []string{"gw_test"}, "upstream-secret-key"); err != nil {
		t.Fatal(err)
	}
	if payload["Image"] != cfg.GatewayImage {
		t.Fatalf("image = %v", payload["Image"])
	}
	labels := payload["Labels"].(map[string]any)
	if labels[managedLabel] != "true" {
		t.Fatalf("managed label = %v", labels[managedLabel])
	}
	if _, exists := labels["dualroute.gateway.upstream_url"]; exists {
		t.Fatalf("upstream URL must not be configurable through labels: %v", labels)
	}
	if strings.Contains(fmt.Sprint(labels), "upstream-secret-key") {
		t.Fatalf("Docker labels leaked upstream API key: %v", labels)
	}
	host := payload["HostConfig"].(map[string]any)
	if host["NetworkMode"] != cfg.DockerNetwork {
		t.Fatalf("network = %v", host["NetworkMode"])
	}
	binds := host["Binds"].([]any)
	if len(binds) != 1 || binds[0] != "dualroute-gateway-data-gateway-b:/data:rw" {
		t.Fatalf("binds = %v", binds)
	}
	environment := fmt.Sprint(payload["Env"])
	if !strings.Contains(environment, "MAX_RETRIES=0") || !strings.Contains(environment, "UPSTREAM_URL="+defaultUpstreamURL) || !strings.Contains(environment, "UPSTREAM_API_KEY=upstream-secret-key") {
		t.Fatalf("environment = %s", environment)
	}
}

func TestCreateUsesProviderSpecificUpstreamConfiguration(t *testing.T) {
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		_, _ = io.WriteString(w, `{"Id":"abc"}`)
	}))
	defer server.Close()
	docker := &dockerClient{client: server.Client(), base: server.URL}
	instance := Instance{Name: "gateway-opencode", Provider: ProviderOpenCode, MaxConcurrency: 4, QueueSize: 8}
	if err := docker.create(Config{GatewayImage: "gateway:test", DockerNetwork: "gateway-network", InstanceToken: "internal"}, instance, []string{"open-key"}, "upstream-key"); err != nil {
		t.Fatal(err)
	}
	environment := fmt.Sprint(payload["Env"])
	if !strings.Contains(environment, "UPSTREAM_PROVIDER=opencode") || !strings.Contains(environment, "UPSTREAM_URL="+openCodeAPIURL) || !strings.Contains(environment, "UPSTREAM_MODEL="+openCodeModel) {
		t.Fatalf("environment = %s", environment)
	}
	labels := payload["Labels"].(map[string]any)
	if labels["dualroute.gateway.provider"] != ProviderOpenCode {
		t.Fatalf("provider label = %v", labels["dualroute.gateway.provider"])
	}
}

func TestDockerLogsDecodesFramesAndRedactsSecrets(t *testing.T) {
	frame := func(value string) []byte {
		result := make([]byte, 8+len(value))
		result[0] = 1
		binary.BigEndian.PutUint32(result[4:8], uint32(len(value)))
		copy(result[8:], value)
		return result
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/containers/gateway-a/logs") {
			t.Fatalf("path = %s", r.URL.String())
		}
		_, _ = w.Write(frame("2026-08-14T01:02:03Z WARN subscription https://example.com/sub?token=secret\n"))
		_, _ = w.Write(frame("2026-08-14T01:02:04Z Authorization: Bearer secret-token\n"))
	}))
	defer server.Close()
	docker := &dockerClient{client: server.Client(), base: server.URL}
	logs, err := docker.logs("gateway-a", "gateway-a", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 || logs[0].Level != "warn" || logs[0].Instance != "gateway-a" {
		t.Fatalf("logs = %#v", logs)
	}
	joined := fmt.Sprint(logs)
	if strings.Contains(joined, "token=secret") || strings.Contains(joined, "secret-token") || !strings.Contains(joined, "[redacted-url]") {
		t.Fatalf("log redaction failed: %s", joined)
	}
}

func TestInspectRejectsUnmanagedContainer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, `{"Id":"abc","Name":"/database","Config":{"Image":"db","Labels":{}},"State":{"Status":"running"}}`)
	}))
	defer server.Close()
	docker := &dockerClient{client: server.Client(), base: server.URL}
	if _, err := docker.inspectManaged("database"); err == nil {
		t.Fatal("expected unmanaged container rejection")
	}
}

func TestWaitHealthyRequiresRunningContainer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1.43/containers/gateway-b/json" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"State":{"Status":"exited"}}`)
	}))
	defer server.Close()
	docker := &dockerClient{client: server.Client(), base: server.URL, probe: func(string) error { return nil }}
	if err := docker.waitHealthy("gateway-b"); err == nil || !strings.Contains(err.Error(), "not running") {
		t.Fatalf("expected non-running error, got %v", err)
	}
}

func TestWaitHealthyReturnsProbeFailure(t *testing.T) {
	docker := &dockerClient{probe: func(string) error { return errors.New("health endpoint unavailable") }}
	if err := docker.waitHealthy("gateway-b"); err == nil || !strings.Contains(err.Error(), "health endpoint unavailable") {
		t.Fatalf("expected probe error, got %v", err)
	}
}

func TestDiscoverInstancesIncludesDockerContainerID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1.43/containers/json" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		_, _ = io.WriteString(w, `[{"Id":"bfb532310b1e-full-id","Names":["/gateway-b"],"State":"running","Labels":{"dualroute.gateway.managed":"true","dualroute.gateway.name":"gateway-b"}}]`)
	}))
	defer server.Close()
	s := New(Config{InstanceToken: "internal", DataDir: t.TempDir()})
	s.docker = &dockerClient{client: server.Client(), base: server.URL}
	instances, err := s.discoverInstances()
	if err != nil {
		t.Fatal(err)
	}
	if len(instances) != 1 || instances[0].ContainerID != "bfb532310b1e-full-id" {
		t.Fatalf("instances = %#v", instances)
	}
}

func TestInstanceCreateRemovesUnhealthyContainer(t *testing.T) {
	removed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1.43/containers/json":
			_, _ = io.WriteString(w, `[]`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1.43/containers/create":
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodGet && r.URL.Path == "/v1.43/containers/gateway-b/json":
			_, _ = io.WriteString(w, `{"Id":"gateway-b-id","Name":"/gateway-b","Config":{"Image":"gateway:test","Labels":{"dualroute.gateway.managed":"true"}},"State":{"Status":"running"}}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1.43/containers/gateway-b-id/start":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1.43/containers/gateway-b-id":
			removed = true
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	s := New(Config{InstanceToken: "internal", DataDir: t.TempDir(), GatewayImage: "gateway:test", DockerNetwork: "gateway-network", MaxInstances: 16})
	s.docker = &dockerClient{client: server.Client(), base: server.URL, probe: func(string) error { return errors.New("unhealthy") }}
	req := httptest.NewRequest(http.MethodPost, "/api/instances", strings.NewReader(`{"name":"gateway-b","upstream_api_key":"upstream-secret-key","max_concurrency":4,"queue_size":8}`))
	res := httptest.NewRecorder()
	s.instanceCreate(res, req)
	if res.Code != http.StatusBadGateway || !strings.Contains(res.Body.String(), "health_check_failed") {
		t.Fatalf("response = %d %s", res.Code, res.Body.String())
	}
	if !removed {
		t.Fatal("unhealthy container was not removed")
	}
}

func TestExecUsesDetachedStartAndChecksExitCode(t *testing.T) {
	var startPayload struct {
		Detach bool `json:"Detach"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1.43/containers/nginx/json":
			_, _ = io.WriteString(w, `{"Id":"nginx-id"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1.43/containers/nginx/exec":
			_, _ = io.WriteString(w, `{"Id":"exec-id"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1.43/exec/exec-id/start":
			if err := json.NewDecoder(r.Body).Decode(&startPayload); err != nil {
				t.Fatal(err)
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1.43/exec/exec-id/json":
			_, _ = io.WriteString(w, `{"Running":false,"ExitCode":0}`)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	docker := &dockerClient{client: server.Client(), base: server.URL}
	if err := docker.exec("nginx", []string{"nginx", "-t"}); err != nil {
		t.Fatal(err)
	}
	if !startPayload.Detach {
		t.Fatal("exec start must be detached")
	}
}

func TestExecReturnsNonZeroExitCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1.43/containers/nginx/json":
			_, _ = io.WriteString(w, `{"Id":"nginx-id"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1.43/containers/nginx/exec":
			_, _ = io.WriteString(w, `{"Id":"exec-id"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1.43/exec/exec-id/start":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1.43/exec/exec-id/json":
			_, _ = io.WriteString(w, `{"Running":false,"ExitCode":1}`)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	docker := &dockerClient{client: server.Client(), base: server.URL}
	if err := docker.exec("nginx", []string{"nginx", "-t"}); err == nil || !strings.Contains(err.Error(), "code 1") {
		t.Fatalf("expected exit code error, got %v", err)
	}
}

func TestValidateInstanceRequestAcceptsSOCKS5(t *testing.T) {
	request := instanceRequest{Name: "gateway-b", UpstreamAPIKey: "upstream-secret-key", ProxyURLs: []string{"socks5h://mihomo:10801"}, MaxConcurrency: 4, QueueSize: 8}
	if code, message := validateInstanceRequest(&request, false); code != 0 {
		t.Fatalf("validation failed: %d %s", code, message)
	}
}

func TestValidateInstanceRequestRequiresUpstreamAPIKeyOnCreate(t *testing.T) {
	request := instanceRequest{MaxConcurrency: 4, QueueSize: 8}
	if code, _ := validateInstanceRequest(&request, true); code != http.StatusBadRequest {
		t.Fatalf("missing upstream API key status = %d", code)
	}
	request = instanceRequest{AuthMode: "custom", MaxConcurrency: 4, QueueSize: 8}
	if code, _ := validateInstanceRequest(&request, true); code != http.StatusBadRequest {
		t.Fatalf("missing custom upstream API key status = %d", code)
	}
	request.UpstreamAPIKey = "key\nwith-newline"
	if code, _ := validateInstanceRequest(&request, true); code != http.StatusBadRequest {
		t.Fatalf("invalid upstream API key status = %d", code)
	}
	request.UpstreamAPIKey = "upstream-secret-key"
	if code, message := validateInstanceRequest(&request, true); code != 0 {
		t.Fatalf("valid upstream API key rejected: %d %s", code, message)
	}
}

func TestValidateInstanceRequestRejectsPublicKeyMode(t *testing.T) {
	request := instanceRequest{AuthMode: "public", MaxConcurrency: 4, QueueSize: 8}
	if code, _ := validateInstanceRequest(&request, true); code != http.StatusBadRequest {
		t.Fatalf("public mode accepted: %d", code)
	}
}

func TestValidateInstanceRequestRejectsPublicKeyInCustomMode(t *testing.T) {
	request := instanceRequest{AuthMode: "custom", UpstreamAPIKey: "public", MaxConcurrency: 4, QueueSize: 8}
	if code, _ := validateInstanceRequest(&request, true); code != http.StatusBadRequest {
		t.Fatalf("public key accepted in custom mode: %d", code)
	}
}

func TestNormalizeInstanceName(t *testing.T) {
	tests := map[string]string{
		"":               "",
		"b":              "gateway-b",
		" Gateway B ":    "gateway-b",
		"gateway-node_1": "gateway-node-1",
	}
	for input, expected := range tests {
		if got := normalizeInstanceName(input); got != expected {
			t.Errorf("normalizeInstanceName(%q) = %q, want %q", input, got, expected)
		}
	}
}

func TestNextInstanceName(t *testing.T) {
	instances := []Instance{{Name: "gateway-a"}, {Name: "gateway-b"}}
	if got := nextInstanceName(instances); got != "gateway-c" {
		t.Fatalf("next name = %q", got)
	}
}

func TestValidateInstanceRequestRejectsNativeTunnelURL(t *testing.T) {
	request := instanceRequest{Name: "gateway-b", ProxyURLs: []string{"vless://uuid@example.com:443"}, MaxConcurrency: 4, QueueSize: 8}
	if code, _ := validateInstanceRequest(&request, false); code != http.StatusBadRequest {
		t.Fatalf("status = %d", code)
	}
}

func TestCreateWithSpecPreservesComposeMetadataAndMounts(t *testing.T) {
	t.Setenv("REQUEST_TIMEOUT", "9m")
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	docker := &dockerClient{client: server.Client(), base: server.URL}
	var original containerSpec
	original.Config.Image = "gateway:test"
	original.Config.Env = []string{"REQUEST_TIMEOUT=9m", "MAX_CONCURRENCY=4"}
	original.Config.Labels = map[string]string{"com.docker.compose.service": "gateway-a", managedLabel: "true"}
	original.Config.ExposedPorts = map[string]any{"13339/tcp": map[string]any{}}
	original.HostConfig.NetworkMode = "gateway-network"
	original.HostConfig.Binds = []string{"/srv/gateway/a:/data:rw"}
	original.HostConfig.ReadonlyRootfs = true
	original.HostConfig.RestartPolicy = map[string]any{"Name": "unless-stopped"}
	instance := Instance{Name: "gateway-a", ProxyURLs: []string{"socks5h://mihomo:10801"}, MaxConcurrency: 6, QueueSize: 12}
	if err := docker.createWithSpec(Config{InstanceToken: "internal"}, instance, []string{"gw_test"}, "upstream-secret-key", original); err != nil {
		t.Fatal(err)
	}
	if payload["Image"] != "gateway:test" {
		t.Fatalf("image = %v", payload["Image"])
	}
	environment := payload["Env"].([]any)
	joinedEnvironment := fmt.Sprint(environment)
	if !strings.Contains(joinedEnvironment, "REQUEST_TIMEOUT=9m") || !strings.Contains(joinedEnvironment, "MAX_CONCURRENCY=6") || !strings.Contains(joinedEnvironment, "UPSTREAM_URL="+defaultUpstreamURL) || !strings.Contains(joinedEnvironment, "UPSTREAM_API_KEY=upstream-secret-key") {
		t.Fatalf("environment = %v", environment)
	}
	labels := payload["Labels"].(map[string]any)
	if labels["com.docker.compose.service"] != "gateway-a" {
		t.Fatalf("compose label = %v", labels["com.docker.compose.service"])
	}
	if _, exists := labels["dualroute.gateway.upstream_url"]; exists {
		t.Fatalf("upstream URL label was retained: %v", labels)
	}
	if strings.Contains(fmt.Sprint(labels), "upstream-secret-key") {
		t.Fatalf("Docker labels leaked upstream API key: %v", labels)
	}
	host := payload["HostConfig"].(map[string]any)
	binds := host["Binds"].([]any)
	if len(binds) != 1 || binds[0] != "/srv/gateway/a:/data:rw" {
		t.Fatalf("binds = %v", binds)
	}
}

func TestBuildMihomoConfigUsesProviderAndSOCKSListener(t *testing.T) {
	config, proxyURLs := buildMihomoConfig("https://example.com/sub?token=secret", []string{"Hong Kong 1", "Japan (2)"})
	for _, required := range []string{"mixed-port: 7890", "type: http", `url: "https://example.com/sub?token=secret"`, `- "clash.meta/v1.19.29"`, "MATCH,GATEWAY", "port: 10801", "port: 10802", "name: GATEWAY-SLOT-1", "name: GATEWAY-SLOT-2"} {
		if !strings.Contains(config, required) {
			t.Fatalf("config missing %q:\n%s", required, config)
		}
	}
	if strings.Contains(config, "filter:") {
		t.Fatalf("selector is still pinned to one node:\n%s", config)
	}
	if len(proxyURLs) != 2 || proxyURLs[0] != "socks5h://mihomo:10801" || proxyURLs[1] != "socks5h://mihomo:10802" {
		t.Fatalf("proxy URLs = %v", proxyURLs)
	}
}

func TestSelectMihomoProxyGroup(t *testing.T) {
	var method, path, body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		data, _ := io.ReadAll(r.Body)
		body = string(data)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	s := New(Config{InstanceToken: "internal", MihomoAPIURL: server.URL, DataDir: t.TempDir()})
	if err := s.selectMihomoProxyGroup("GATEWAY-SLOT-2", "Japan (2)"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPut || path != "/proxies/GATEWAY-SLOT-2" || !strings.Contains(body, `"name":"Japan (2)"`) {
		t.Fatalf("request = %s %s %s", method, path, body)
	}
}

func TestValidateMihomoSubscriptionUsesClashUserAgent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != mihomoUserAgent {
			t.Fatalf("user agent = %q", got)
		}
		_, _ = io.WriteString(w, "proxies:\n  - name: test\n    type: socks5\n    server: 127.0.0.1\n    port: 1080\n")
	}))
	defer server.Close()
	s := New(Config{InstanceToken: "internal", DataDir: t.TempDir()})
	s.client = server.Client()
	if err := s.validateMihomoSubscription(context.Background(), server.URL+"?token=secret"); err != nil {
		t.Fatal(err)
	}
}

func TestValidateMihomoSubscriptionRejectsNonClashWithoutLeakingURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "dmxlc3M6Ly9leGFtcGxl")
	}))
	defer server.Close()
	s := New(Config{InstanceToken: "internal", DataDir: t.TempDir()})
	s.client = server.Client()
	secretURL := server.URL + "?token=do-not-leak"
	err := s.validateMihomoSubscription(context.Background(), secretURL)
	if err == nil || !strings.Contains(err.Error(), "not Clash/Mihomo YAML") {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), secretURL) || strings.Contains(err.Error(), "do-not-leak") {
		t.Fatalf("error leaks subscription URL: %v", err)
	}
}

func TestValidateMihomoSubscriptionReportsHTTPStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "expired", http.StatusForbidden)
	}))
	defer server.Close()
	s := New(Config{InstanceToken: "internal", DataDir: t.TempDir()})
	s.client = server.Client()
	err := s.validateMihomoSubscription(context.Background(), server.URL)
	if err == nil || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("error = %v", err)
	}
}

func TestGetMihomoGroupAndUniqueNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/proxies/GATEWAY" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"name":"GATEWAY","now":"Japan","all":["DIRECT","Japan","Hong Kong","Japan","REJECT"]}`)
	}))
	defer server.Close()
	s := New(Config{InstanceToken: "internal", MihomoAPIURL: server.URL, DataDir: t.TempDir()})
	group, err := s.getMihomoGroup()
	if err != nil {
		t.Fatal(err)
	}
	nodes := uniqueMihomoNodes(group.All)
	if fmt.Sprint(nodes) != "[Japan Hong Kong]" {
		t.Fatalf("nodes = %v", nodes)
	}
}

func TestRoutableMihomoNodesRejectsDirectEntries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/providers/proxies/gateway-subscription" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"proxies":[{"name":"direct-looking-node","type":"Direct"},{"name":"proxy-node","type":"VLESS"},{"name":"group-node","type":"Selector"}]}`)
	}))
	defer server.Close()
	s := New(Config{InstanceToken: "internal", MihomoAPIURL: server.URL, MihomoMaxSlots: 8, DataDir: t.TempDir()})
	nodes := s.routableMihomoNodes([]string{"direct-looking-node", "proxy-node", "group-node"})
	if fmt.Sprint(nodes) != "[proxy-node]" {
		t.Fatalf("nodes = %v", nodes)
	}
}

func TestRoutableMihomoNodesKeepsFortyCandidates(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/providers/proxies/gateway-subscription" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		proxies := make([]string, 0, 40)
		for index := 1; index <= 40; index++ {
			proxies = append(proxies, fmt.Sprintf(`{"name":"node-%02d","type":"VLESS"}`, index))
		}
		_, _ = io.WriteString(w, `{"proxies":[`+strings.Join(proxies, ",")+"]}")
	}))
	defer server.Close()
	s := New(Config{InstanceToken: "internal", MihomoAPIURL: server.URL, MihomoMaxSlots: 64, DataDir: t.TempDir()})
	values := make([]string, 0, 40)
	for index := 1; index <= 40; index++ {
		values = append(values, fmt.Sprintf("node-%02d", index))
	}
	nodes := s.routableMihomoNodes(values)
	if len(nodes) != 40 || nodes[0] != "node-01" || nodes[39] != "node-40" {
		t.Fatalf("nodes = %d, first=%q, last=%q", len(nodes), nodes[0], nodes[len(nodes)-1])
	}
}

func TestMihomoProbeTriggersProviderHealthCheck(t *testing.T) {
	var putCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/providers/proxies/gateway-subscription" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Method == http.MethodPut {
			putCalls++
			body, _ := io.ReadAll(r.Body)
			if string(body) != `{"health-check":true}` {
				t.Fatalf("probe body = %s", body)
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = io.WriteString(w, `{"proxies":[{"name":"node-1","type":"VLESS","alive":false}]}`)
	}))
	defer server.Close()
	s := &Server{cfg: Config{MihomoAPIURL: server.URL}, client: server.Client()}
	res := httptest.NewRecorder()
	s.mihomoProbe(res)
	if res.Code != http.StatusOK || putCalls != 1 || !strings.Contains(res.Body.String(), `"healthy":0`) {
		t.Fatalf("status = %d, puts = %d, body = %s", res.Code, putCalls, res.Body.String())
	}
}

func TestMihomoProxyURLs(t *testing.T) {
	urls := mihomoProxyURLs(3)
	if fmt.Sprint(urls) != "[socks5h://mihomo:10801 socks5h://mihomo:10802 socks5h://mihomo:10803]" {
		t.Fatalf("URLs = %v", urls)
	}
}

func TestSelectTrafficPoolPrefersProxyInstances(t *testing.T) {
	instances := []Instance{
		{Name: "gateway-a", Status: "running"},
		{Name: "gateway-b", Status: "running", ProxyURLs: []string{"socks5h://mihomo:10801"}},
		{Name: "gateway-c", Status: "stopped", ProxyURLs: []string{"socks5h://mihomo:10802"}},
	}
	selected := selectTrafficPool(instances, false)
	if len(selected) != 1 || selected[0].Name != "gateway-b" {
		t.Fatalf("selected = %#v", selected)
	}
}

func TestSelectTrafficPoolFallsBackToDirectWhenNoProxyIsRunning(t *testing.T) {
	instances := []Instance{
		{Name: "gateway-a", Status: "running"},
		{Name: "gateway-b", Status: "stopped", ProxyURLs: []string{"socks5h://mihomo:10801"}},
	}
	selected := selectTrafficPool(instances, false)
	if len(selected) != 1 || selected[0].Name != "gateway-a" {
		t.Fatalf("selected = %#v", selected)
	}
}

func TestSelectTrafficPoolCanIncludeDirectFallback(t *testing.T) {
	instances := []Instance{
		{Name: "gateway-a", Status: "running"},
		{Name: "gateway-b", Status: "running", ProxyURLs: []string{"socks5h://mihomo:10801"}},
	}
	selected := selectTrafficPool(instances, true)
	if len(selected) != 2 {
		t.Fatalf("selected = %#v", selected)
	}
}

func TestResolveMihomoSettingsKeepsOrClearsSecretURL(t *testing.T) {
	current := mihomoSettings{SubscriptionURL: "https://example.com/sub?token=secret", Nodes: []string{"Japan"}}
	kept := resolveMihomoSettings(current, mihomoUpdateRequest{})
	if kept.SubscriptionURL != current.SubscriptionURL {
		t.Fatalf("blank update replaced subscription: %q", kept.SubscriptionURL)
	}
	cleared := resolveMihomoSettings(current, mihomoUpdateRequest{Clear: true})
	if cleared.SubscriptionURL != "" {
		t.Fatalf("clear retained subscription: %q", cleared.SubscriptionURL)
	}
}

func TestInvalidateMihomoProviderCacheRemovesOldSubscription(t *testing.T) {
	configDir := t.TempDir()
	cachePath := filepath.Join(configDir, "providers", mihomoProviderName+".yaml")
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cachePath, []byte("old subscription"), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := invalidateMihomoProviderCache(configDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old provider cache still exists: %v", err)
	}
	if err := os.WriteFile(cachePath, []byte("new subscription"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.restore(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "old subscription" {
		t.Fatalf("restored provider cache = %q", data)
	}
}

func TestRestoreMihomoProviderCacheRemovesFailedFirstSubscription(t *testing.T) {
	configDir := t.TempDir()
	cachePath := filepath.Join(configDir, "providers", mihomoProviderName+".yaml")
	snapshot, err := invalidateMihomoProviderCache(configDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cachePath, []byte("failed subscription"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.restore(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed provider cache still exists: %v", err)
	}
}

func TestReplaceKeepsOldContainerWhenReplacementIsUnhealthy(t *testing.T) {
	oldRenamed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && (r.URL.Path == "/v1.43/containers/gateway-a/json" || r.URL.Path == "/v1.43/containers/old-id/json"):
			_, _ = io.WriteString(w, `{"Id":"old-id","Name":"/gateway-a","Config":{"Image":"gateway:test","Env":["REQUEST_TIMEOUT=5m"],"Labels":{"dualroute.gateway.managed":"true"},"ExposedPorts":{"13339/tcp":{}}},"HostConfig":{"NetworkMode":"gateway-network","Binds":["/srv/a:/data:rw"],"ReadonlyRootfs":true,"SecurityOpt":[],"Tmpfs":{},"RestartPolicy":{"Name":"unless-stopped"}},"State":{"Status":"running"}}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1.43/containers/create":
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1.43/containers/gateway-a-next-"):
			_, _ = io.WriteString(w, `{"Id":"next-id","Name":"/gateway-a-next","Config":{"Image":"gateway:test","Labels":{"dualroute.gateway.managed":"true"}},"State":{"Status":"created"}}`)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1.43/containers/next-id/start"):
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1.43/containers/next-id":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1.43/containers/old-id/rename":
			oldRenamed = true
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	docker := &dockerClient{client: server.Client(), base: server.URL, probe: func(string) error { return errors.New("unhealthy") }}
	instance := Instance{Name: "gateway-a", MaxConcurrency: 4, QueueSize: 8}
	if err := docker.replace(Config{InstanceToken: "internal"}, instance, []string{"gw_test"}, "upstream-secret-key"); err == nil {
		t.Fatal("expected unhealthy replacement error")
	}
	if oldRenamed {
		t.Fatal("old container was renamed before replacement became healthy")
	}
}
