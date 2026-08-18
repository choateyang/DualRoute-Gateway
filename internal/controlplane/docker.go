package controlplane

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const managedLabel = "dualroute.gateway.managed"

var errContainerNotFound = errors.New("container not found")

var instanceNamePattern = regexp.MustCompile(`^gateway-[a-z0-9][a-z0-9-]{0,38}$`)
var logURLPattern = regexp.MustCompile(`https?://[^\s"']+`)
var logBearerPattern = regexp.MustCompile(`(?i)Bearer\s+[A-Za-z0-9._~+/-]+`)

type dockerClient struct {
	client *http.Client
	base   string
	probe  func(string) error
}

type dockerContainer struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	Image  string            `json:"Image"`
	State  string            `json:"State"`
	Status string            `json:"Status"`
	Labels map[string]string `json:"Labels"`
}

type containerSpec struct {
	Config struct {
		Image        string            `json:"Image"`
		Env          []string          `json:"Env"`
		Labels       map[string]string `json:"Labels"`
		ExposedPorts map[string]any    `json:"ExposedPorts"`
	} `json:"Config"`
	HostConfig struct {
		NetworkMode    string            `json:"NetworkMode"`
		Binds          []string          `json:"Binds"`
		ReadonlyRootfs bool              `json:"ReadonlyRootfs"`
		SecurityOpt    []string          `json:"SecurityOpt"`
		Tmpfs          map[string]string `json:"Tmpfs"`
		RestartPolicy  map[string]any    `json:"RestartPolicy"`
	} `json:"HostConfig"`
}

func newDockerClient(endpoint string) *dockerClient {
	transport := &http.Transport{}
	base := "http://docker"
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		base = strings.TrimRight(endpoint, "/")
	} else {
		path := strings.TrimPrefix(endpoint, "unix://")
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		}
	}
	return &dockerClient{client: &http.Client{Transport: transport, Timeout: 20 * time.Second}, base: base, probe: probeGatewayContainer}
}

func (d *dockerClient) request(method, path string, body io.Reader, out any) error {
	req, err := http.NewRequest(method, d.base+path, body)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("docker API %s %s: HTTP %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(message)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (d *dockerClient) logs(name, source string, tail int) ([]SystemLog, error) {
	if tail < 1 {
		tail = 100
	}
	path := "/v1.43/containers/" + url.PathEscape(name) + "/logs?stdout=true&stderr=true&timestamps=true&tail=" + strconv.Itoa(tail)
	req, err := http.NewRequest(http.MethodGet, d.base+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("docker logs for %s returned HTTP %d", name, resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	lines := decodeDockerLogLines(data)
	result := make([]SystemLog, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		at := time.Time{}
		if stamp, rest, found := strings.Cut(line, " "); found {
			if parsed, parseErr := time.Parse(time.RFC3339Nano, stamp); parseErr == nil {
				at, line = parsed, rest
			}
		}
		if at.IsZero() {
			at = time.Now()
		}
		line = logURLPattern.ReplaceAllString(line, "[redacted-url]")
		line = logBearerPattern.ReplaceAllString(line, "Bearer [redacted]")
		level := "info"
		lower := strings.ToLower(line)
		if strings.Contains(lower, "error") || strings.Contains(lower, "fatal") {
			level = "error"
		} else if strings.Contains(lower, "warn") {
			level = "warn"
		}
		result = append(result, SystemLog{At: at, Level: level, Message: line, Instance: source})
	}
	return result, nil
}

func decodeDockerLogLines(data []byte) []string {
	var payloads []string
	for len(data) >= 8 && (data[0] == 1 || data[0] == 2) && data[1] == 0 && data[2] == 0 && data[3] == 0 {
		length := int(binary.BigEndian.Uint32(data[4:8]))
		if length < 0 || length > len(data)-8 {
			break
		}
		payloads = append(payloads, string(data[8:8+length]))
		data = data[8+length:]
	}
	if len(payloads) == 0 {
		payloads = append(payloads, string(data))
	} else if len(data) > 0 {
		payloads = append(payloads, string(data))
	}
	var lines []string
	for _, payload := range payloads {
		for _, line := range strings.Split(strings.ReplaceAll(payload, "\r\n", "\n"), "\n") {
			if strings.TrimSpace(line) != "" {
				lines = append(lines, line)
			}
		}
	}
	return lines
}

func (d *dockerClient) listManaged() ([]dockerContainer, error) {
	filters, _ := json.Marshal(map[string][]string{"label": []string{managedLabel + "=true"}})
	var containers []dockerContainer
	err := d.request(http.MethodGet, "/v1.43/containers/json?all=1&filters="+url.QueryEscape(string(filters)), nil, &containers)
	return containers, err
}

func (d *dockerClient) inspectManaged(name string) (dockerContainer, error) {
	var raw struct {
		ID     string `json:"Id"`
		Name   string `json:"Name"`
		Config struct {
			Image  string            `json:"Image"`
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
		State struct {
			Status string `json:"Status"`
		} `json:"State"`
	}
	if err := d.request(http.MethodGet, "/v1.43/containers/"+url.PathEscape(name)+"/json", nil, &raw); err != nil {
		return dockerContainer{}, err
	}
	if raw.Config.Labels[managedLabel] != "true" {
		return dockerContainer{}, fmt.Errorf("container %q is not managed by this control plane", name)
	}
	return dockerContainer{ID: raw.ID, Names: []string{raw.Name}, Image: raw.Config.Image, State: raw.State.Status, Labels: raw.Config.Labels}, nil
}

func (d *dockerClient) create(cfg Config, instance Instance, keys []string, upstreamAPIKey string) error {
	if !instanceNamePattern.MatchString(instance.Name) {
		return fmt.Errorf("instance name must match %s", instanceNamePattern.String())
	}
	instance.Provider = providerOrDefault(instance.Provider)
	direct := len(instance.ProxyURLs) == 0
	environment := []string{
		"LISTEN_ADDR=0.0.0.0:13339",
		"UPSTREAM_URL=" + upstreamURLForProvider(instance.Provider),
		"UPSTREAM_PROVIDER=" + instance.Provider,
		"UPSTREAM_MODEL=" + upstreamModelForProvider(instance.Provider),
		"UPSTREAM_API_KEY=" + upstreamAPIKey,
		"GATEWAY_KEYS=" + strings.Join(keys, ","),
		"INSTANCE_ADMIN_TOKEN=" + cfg.InstanceToken,
		"PROXY_URLS=" + strings.Join(instance.ProxyURLs, ","),
		"DIRECT_ENABLED=" + strconv.FormatBool(direct),
		"MAX_CONCURRENCY=" + strconv.Itoa(instance.MaxConcurrency),
		"QUEUE_SIZE=" + strconv.Itoa(instance.QueueSize),
		"DATA_DIR=/data",
		"MAX_RETRIES=" + env("MAX_RETRIES", "2"),
		"REQUEST_TIMEOUT=" + env("REQUEST_TIMEOUT", "5m"),
		"STREAM_FIRST_OUTPUT_TIMEOUT=" + env("STREAM_FIRST_OUTPUT_TIMEOUT", "20s"),
		"STREAM_FAILURE_COOLDOWN=" + env("STREAM_FAILURE_COOLDOWN", "10m"),
		"COOLDOWN_BASE=" + env("COOLDOWN_BASE", "5s"),
		"COOLDOWN_MAX=" + env("COOLDOWN_MAX", "60s"),
		"PROXY_PROBE_URL=" + env("PROXY_PROBE_URL", "https://api.ipify.org,https://ifconfig.me/ip,https://www.cloudflare.com/cdn-cgi/trace"),
		"PROXY_PROBE_TIMEOUT=" + env("PROXY_PROBE_TIMEOUT", "10s"),
		"PROXY_PROBE_CONCURRENCY=" + env("PROXY_PROBE_CONCURRENCY", "8"),
		"TARGET_EGRESS_SLOTS=" + env("TARGET_EGRESS_SLOTS", "0"),
		"FREE_MODELS_ONLY=" + strconv.FormatBool(instance.Provider == ProviderOpenCode),
		"DISABLE_THINKING_BY_DEFAULT=" + strconv.FormatBool(instance.Provider == ProviderOpenCode),
		"MIN_THINKING_MAX_TOKENS=" + providerThinkingTokenBudget(instance.Provider),
		"ISOLATE_UPSTREAM_STATE=" + env("ISOLATE_UPSTREAM_STATE", "true"),
	}
	labels := map[string]string{
		managedLabel:                        "true",
		"dualroute.gateway.name":            instance.Name,
		"dualroute.gateway.proxy_urls":      strings.Join(instance.ProxyURLs, ","),
		"dualroute.gateway.max_concurrency": strconv.Itoa(instance.MaxConcurrency),
		"dualroute.gateway.queue_size":      strconv.Itoa(instance.QueueSize),
		"dualroute.gateway.provider":        instance.Provider,
	}
	payload := map[string]any{
		"Image":        cfg.GatewayImage,
		"Env":          environment,
		"Labels":       labels,
		"ExposedPorts": map[string]any{"13339/tcp": map[string]any{}},
		"HostConfig": map[string]any{
			"NetworkMode":    cfg.DockerNetwork,
			"Binds":          []string{instanceDataVolume(instance.Name) + ":/data:rw"},
			"ReadonlyRootfs": true,
			"SecurityOpt":    []string{"no-new-privileges:true"},
			"Tmpfs":          map[string]string{"/tmp": "rw,noexec,nosuid,size=16m"},
			"RestartPolicy":  map[string]string{"Name": "unless-stopped"},
		},
	}
	encoded, _ := json.Marshal(payload)
	return d.request(http.MethodPost, "/v1.43/containers/create?name="+url.QueryEscape(instance.Name), strings.NewReader(string(encoded)), nil)
}

func (d *dockerClient) action(name, action string) error {
	container, err := d.inspectManaged(name)
	if err != nil {
		return err
	}
	endpoint := "/v1.43/containers/" + url.PathEscape(container.ID) + "/" + action
	if action == "stop" || action == "restart" {
		endpoint += "?t=15"
	}
	return d.request(http.MethodPost, endpoint, nil, nil)
}

func (d *dockerClient) waitHealthy(name string) error {
	if d.probe != nil {
		if err := d.probe(name); err != nil {
			return err
		}
	}
	status, err := d.containerStatus(name)
	if err != nil {
		return err
	}
	if status != "running" {
		return fmt.Errorf("container %q is not running (status %q)", name, status)
	}
	return nil
}

func (d *dockerClient) remove(name string) error {
	container, err := d.inspectManaged(name)
	if err != nil {
		return err
	}
	return d.request(http.MethodDelete, "/v1.43/containers/"+url.PathEscape(container.ID)+"?force=true&v=true", nil, nil)
}

func (d *dockerClient) removeDataVolume(name string) error {
	return d.request(http.MethodDelete, "/v1.43/volumes/"+url.PathEscape(instanceDataVolume(name))+"?force=true", nil, nil)
}

func instanceDataVolume(name string) string {
	return "dualroute-gateway-data-" + name
}

func (d *dockerClient) replace(cfg Config, instance Instance, keys []string, upstreamAPIKey string) error {
	current, err := d.inspectManaged(instance.Name)
	if err != nil {
		return err
	}
	wasRunning := current.State == "running"
	var original containerSpec
	if err := d.request(http.MethodGet, "/v1.43/containers/"+url.PathEscape(current.ID)+"/json", nil, &original); err != nil {
		return err
	}
	backupName := instance.Name + "-previous-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	temporaryName := instance.Name + "-next-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if err := d.createWithSpecNamed(cfg, temporaryName, instance, keys, upstreamAPIKey, original); err != nil {
		return err
	}
	removeTemporary := func() {
		if replacement, inspectErr := d.inspectManaged(temporaryName); inspectErr == nil {
			_ = d.request(http.MethodDelete, "/v1.43/containers/"+url.PathEscape(replacement.ID)+"?force=true&v=true", nil, nil)
		}
	}
	if wasRunning {
		if err := d.action(temporaryName, "start"); err != nil {
			removeTemporary()
			return err
		}
		if d.probe != nil {
			if err := d.probe(temporaryName); err != nil {
				removeTemporary()
				return err
			}
		}
	}
	if err := d.request(http.MethodPost, "/v1.43/containers/"+url.PathEscape(current.ID)+"/rename?name="+url.QueryEscape(backupName), nil, nil); err != nil {
		removeTemporary()
		return err
	}
	if replacement, err := d.inspectManaged(temporaryName); err != nil {
		_ = d.request(http.MethodPost, "/v1.43/containers/"+url.PathEscape(current.ID)+"/rename?name="+url.QueryEscape(instance.Name), nil, nil)
		removeTemporary()
		return err
	} else if err := d.request(http.MethodPost, "/v1.43/containers/"+url.PathEscape(replacement.ID)+"/rename?name="+url.QueryEscape(instance.Name), nil, nil); err != nil {
		_ = d.request(http.MethodPost, "/v1.43/containers/"+url.PathEscape(current.ID)+"/rename?name="+url.QueryEscape(instance.Name), nil, nil)
		removeTemporary()
		return err
	}
	if wasRunning {
		_ = d.action(backupName, "stop")
	}
	_ = d.request(http.MethodDelete, "/v1.43/containers/"+url.PathEscape(current.ID)+"?force=true&v=true", nil, nil)
	return nil
}

func (d *dockerClient) createWithSpec(cfg Config, instance Instance, keys []string, upstreamAPIKey string, original containerSpec) error {
	return d.createWithSpecNamed(cfg, instance.Name, instance, keys, upstreamAPIKey, original)
}

func (d *dockerClient) createWithSpecNamed(cfg Config, containerName string, instance Instance, keys []string, upstreamAPIKey string, original containerSpec) error {
	if !instanceNamePattern.MatchString(instance.Name) {
		return fmt.Errorf("instance name must match %s", instanceNamePattern.String())
	}
	instance.Provider = providerOrDefault(instance.Provider)
	direct := len(instance.ProxyURLs) == 0
	overrides := map[string]string{
		"LISTEN_ADDR": "0.0.0.0:13339", "GATEWAY_KEYS": strings.Join(keys, ","), "INSTANCE_ADMIN_TOKEN": cfg.InstanceToken,
		"UPSTREAM_URL":      upstreamURLForProvider(instance.Provider),
		"UPSTREAM_PROVIDER": instance.Provider,
		"UPSTREAM_MODEL":    upstreamModelForProvider(instance.Provider),
		"UPSTREAM_API_KEY":  upstreamAPIKey,
		"PROXY_URLS":        strings.Join(instance.ProxyURLs, ","), "DIRECT_ENABLED": strconv.FormatBool(direct),
		"MAX_CONCURRENCY": strconv.Itoa(instance.MaxConcurrency), "QUEUE_SIZE": strconv.Itoa(instance.QueueSize), "DATA_DIR": "/data",
		"MAX_RETRIES": env("MAX_RETRIES", "2"), "REQUEST_TIMEOUT": env("REQUEST_TIMEOUT", "5m"), "STREAM_FIRST_OUTPUT_TIMEOUT": env("STREAM_FIRST_OUTPUT_TIMEOUT", "20s"), "STREAM_FAILURE_COOLDOWN": env("STREAM_FAILURE_COOLDOWN", "10m"),
		"COOLDOWN_BASE": env("COOLDOWN_BASE", "5s"), "COOLDOWN_MAX": env("COOLDOWN_MAX", "60s"),
		"PROXY_PROBE_URL": env("PROXY_PROBE_URL", "https://api.ipify.org,https://ifconfig.me/ip,https://www.cloudflare.com/cdn-cgi/trace"), "PROXY_PROBE_TIMEOUT": env("PROXY_PROBE_TIMEOUT", "10s"),
		"PROXY_PROBE_CONCURRENCY": env("PROXY_PROBE_CONCURRENCY", "8"), "TARGET_EGRESS_SLOTS": env("TARGET_EGRESS_SLOTS", "0"),
		"FREE_MODELS_ONLY": strconv.FormatBool(instance.Provider == ProviderOpenCode), "DISABLE_THINKING_BY_DEFAULT": strconv.FormatBool(instance.Provider == ProviderOpenCode), "MIN_THINKING_MAX_TOKENS": providerThinkingTokenBudget(instance.Provider),
		"ISOLATE_UPSTREAM_STATE": env("ISOLATE_UPSTREAM_STATE", "true"),
	}
	environment := mergeEnvironment(original.Config.Env, overrides)
	labels := map[string]string{}
	for key, value := range original.Config.Labels {
		labels[key] = value
	}
	labels[managedLabel] = "true"
	labels["dualroute.gateway.name"] = instance.Name
	delete(labels, "dualroute.gateway.upstream_url")
	labels["dualroute.gateway.proxy_urls"] = strings.Join(instance.ProxyURLs, ",")
	labels["dualroute.gateway.max_concurrency"] = strconv.Itoa(instance.MaxConcurrency)
	labels["dualroute.gateway.queue_size"] = strconv.Itoa(instance.QueueSize)
	labels["dualroute.gateway.provider"] = instance.Provider
	payload := map[string]any{"Image": original.Config.Image, "Env": environment, "Labels": labels, "ExposedPorts": original.Config.ExposedPorts,
		"HostConfig": map[string]any{"NetworkMode": original.HostConfig.NetworkMode, "Binds": original.HostConfig.Binds, "ReadonlyRootfs": original.HostConfig.ReadonlyRootfs, "SecurityOpt": original.HostConfig.SecurityOpt, "Tmpfs": original.HostConfig.Tmpfs, "RestartPolicy": original.HostConfig.RestartPolicy}}
	encoded, _ := json.Marshal(payload)
	return d.request(http.MethodPost, "/v1.43/containers/create?name="+url.QueryEscape(containerName), strings.NewReader(string(encoded)), nil)
}

func mergeEnvironment(existing []string, overrides map[string]string) []string {
	seen := make(map[string]struct{}, len(existing)+len(overrides))
	result := make([]string, 0, len(existing)+len(overrides))
	for _, entry := range existing {
		name, _, ok := strings.Cut(entry, "=")
		if !ok || name == "" {
			continue
		}
		if value, replace := overrides[name]; replace {
			result = append(result, name+"="+value)
			seen[name] = struct{}{}
		} else {
			result = append(result, entry)
			seen[name] = struct{}{}
		}
	}
	for name, value := range overrides {
		if _, exists := seen[name]; !exists {
			result = append(result, name+"="+value)
		}
	}
	sort.Strings(result)
	return result
}

func probeGatewayContainer(container string) error {
	client := &http.Client{Timeout: time.Second}
	endpoint := "http://" + container + ":13339/healthz"
	var lastErr error
	// Static proxy slots are probed asynchronously. Wait long enough for a
	// healthy slot to appear without requiring every subscription node to work.
	for attempt := 0; attempt < 120; attempt++ {
		resp, err := client.Get(endpoint)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			err = fmt.Errorf("health check returned HTTP %d", resp.StatusCode)
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("replacement gateway did not become healthy: %w", lastErr)
}

func (d *dockerClient) exec(container string, command []string) error {
	if _, err := d.inspectByName(container); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{"AttachStdout": false, "AttachStderr": false, "Cmd": command})
	var created struct {
		ID string `json:"Id"`
	}
	if err := d.request(http.MethodPost, "/v1.43/containers/"+url.PathEscape(container)+"/exec", strings.NewReader(string(payload)), &created); err != nil {
		return err
	}
	// Detached exec keeps the Docker API response as regular HTTP. Attached
	// exec upgrades the connection to a multiplexed stream, which this client
	// deliberately does not parse. The inspect loop still verifies exit status.
	start, _ := json.Marshal(map[string]any{"Detach": true})
	if err := d.request(http.MethodPost, "/v1.43/exec/"+url.PathEscape(created.ID)+"/start", strings.NewReader(string(start)), nil); err != nil {
		return err
	}
	var inspected struct {
		Running  bool `json:"Running"`
		ExitCode int  `json:"ExitCode"`
	}
	for i := 0; i < 20; i++ {
		if err := d.request(http.MethodGet, "/v1.43/exec/"+url.PathEscape(created.ID)+"/json", nil, &inspected); err != nil {
			return err
		}
		if !inspected.Running {
			if inspected.ExitCode != 0 {
				return fmt.Errorf("docker exec %q exited with code %d", command, inspected.ExitCode)
			}
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("docker exec %q did not finish", command)
}

func (d *dockerClient) inspectByName(name string) (map[string]any, error) {
	var result map[string]any
	err := d.request(http.MethodGet, "/v1.43/containers/"+url.PathEscape(name)+"/json", nil, &result)
	return result, err
}

func (d *dockerClient) containerStatus(name string) (string, error) {
	var result struct {
		State struct {
			Status string `json:"Status"`
		} `json:"State"`
	}
	if err := d.request(http.MethodGet, "/v1.43/containers/"+url.PathEscape(name)+"/json", nil, &result); err != nil {
		return "", err
	}
	if result.State.Status == "" {
		return "", errContainerNotFound
	}
	return result.State.Status, nil
}

func (d *dockerClient) restartByName(name string) error {
	if _, err := d.inspectByName(name); err != nil {
		return err
	}
	if err := d.request(http.MethodPost, "/v1.43/containers/"+url.PathEscape(name)+"/restart?t=15", nil, nil); err != nil {
		return err
	}
	var lastStatus string
	for attempt := 0; attempt < 20; attempt++ {
		status, err := d.containerStatus(name)
		if err == nil {
			lastStatus = status
			if status == "running" {
				return nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("container %q did not return to running state (last status %q)", name, lastStatus)
}

func (s *Server) discoverInstances() ([]Instance, error) {
	containers, err := s.docker.listManaged()
	if err != nil {
		return nil, err
	}
	instances := make([]Instance, 0, len(containers))
	for _, container := range containers {
		name := strings.TrimPrefix(first(container.Names), "/")
		if configured := container.Labels["dualroute.gateway.name"]; configured != "" {
			name = configured
		}
		instances = append(instances, Instance{
			Name: name, Container: strings.TrimPrefix(first(container.Names), "/"), ContainerID: container.ID, Managed: true,
			URL: "http://" + strings.TrimPrefix(first(container.Names), "/") + ":13339", Status: container.State,
			ProxyURLs:      split(container.Labels["dualroute.gateway.proxy_urls"]),
			MaxConcurrency: positiveInt(container.Labels["dualroute.gateway.max_concurrency"], 4),
			QueueSize:      nonNegativeInt(container.Labels["dualroute.gateway.queue_size"], 8),
			Provider:       providerOrDefault(container.Labels["dualroute.gateway.provider"]),
		})
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].Name < instances[j].Name })
	return instances, nil
}

func providerOrDefault(provider string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == ProviderOpenCode {
		return ProviderOpenCode
	}
	return ProviderTokenRouter
}

func upstreamURLForProvider(provider string) string {
	if providerOrDefault(provider) == ProviderOpenCode {
		return openCodeAPIURL
	}
	return defaultUpstreamURL
}

func upstreamModelForProvider(provider string) string {
	if providerOrDefault(provider) == ProviderOpenCode {
		return openCodeModel
	}
	return tokenRouterModel
}

func providerThinkingTokenBudget(provider string) string {
	if providerOrDefault(provider) == ProviderOpenCode {
		return "8192"
	}
	return "0"
}

func (s *Server) refreshInstances() error {
	instances, err := s.discoverInstances()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.instances = instances
	s.persistInstancesLocked()
	s.mu.Unlock()
	return nil
}

func (s *Server) ReconcileInstances() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return s.refreshInstances()
}

func selectTrafficPool(instances []Instance, directFallback bool) []Instance {
	running := make([]Instance, 0, len(instances))
	proxy := make([]Instance, 0, len(instances))
	for _, instance := range instances {
		if instance.Status != "running" {
			continue
		}
		running = append(running, instance)
		if len(instance.ProxyURLs) > 0 {
			proxy = append(proxy, instance)
		}
	}
	if len(proxy) > 0 && !directFallback {
		return proxy
	}
	return running
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func labelOrDefault(labels map[string]string, key, fallback string) string {
	if value := strings.TrimSpace(labels[key]); value != "" {
		return value
	}
	return fallback
}

func positiveInt(value string, fallback int) int {
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 {
		return fallback
	}
	return n
}
func nonNegativeInt(value string, fallback int) int {
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}
func positiveIntEnv(name string, fallback int) int { return positiveInt(os.Getenv(name), fallback) }
