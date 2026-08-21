package controlplane

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

//go:embed web/index.html web/app.js web/style.css web/pages/*.html
var assets embed.FS

const (
	ProviderTokenRouter = "tokenrouter"
	ProviderOpenCode    = "opencode"
	ProviderCline       = "cline"
	ProviderFreeBuff    = "freebuff"

	defaultUpstreamURL     = "https://api.tokenrouter.com/v1"
	openCodeAPIURL         = "https://opencode.ai/zen"
	clineAPIURL            = "https://api.cline.bot/api/v1"
	freeBuffAPIURL         = "https://www.codebuff.com"
	tokenRouterModel       = "deepseek/deepseek-v4-pro-0813-free"
	openCodeModel          = "deepseek-v4-flash-free"
	tokenRouterClientModel = "TokenRouter/deepseek-v4-pro"
	openCodeClientModel    = "OpenCode/deepseek-v4-flash"
	clineClientModel       = "cline/deepseek-v4-flash"
	clineUpstreamModel     = "deepseek/deepseek-v4-flash"
)

// Keep in sync with the free tier of https://opencode.ai/zen/v1/models.
// Only keyless free models belong here; paid models require upstream keys.
var openCodeModels = []string{
	"big-pickle",
	"deepseek-v4-flash-free",
	"hy3-free",
	"laguna-s-2.1-free",
	"mimo-v2.5-free",
	"muse-spark-1.2-contributor-free",
	"nemotron-3-ultra-free",
	"nemotron-3.5-lightning-free",
	"x-preview-f-free",
}

const (
	providerAuthModePublic = "public"
	providerAuthModeCustom = "custom"
)

const (
	maxAPIRequestBodyBytes = 16 << 20
	maxClineModels         = 128
	maxClineModelLength    = 160
)

const instanceHealthyHeader = "X-DualRoute-Instance-Healthy"

var errAPIRequestBodyTooLarge = errors.New("request_body_too_large")

type Config struct {
	ListenAddr      string
	APIListenAddr   string
	InstanceToken   string
	Instances       []Instance
	DataDir         string
	DockerSocket    string
	DockerNetwork   string
	GatewayImage    string
	DirectFallback  bool
	MihomoContainer string
	MihomoConfigDir string
	MihomoAPIURL    string
	MihomoMaxSlots  int
	MaxInstances    int
	// Optional HTTP(S) proxy used only to refresh the FreeBuff model catalog.
	FreeBuffModelsProxy string
	// Ordered, trusted GitHub release URLs for the FreeBuff model catalog.
	FreeBuffModelsURLs []string
	// BootstrapKeys remains for programmatic migrations. New deployments start
	// with no gateway keys and add them through the control plane.
	BootstrapKeys []string
}

type Instance struct {
	Name           string   `json:"name"`
	URL            string   `json:"url"`
	Container      string   `json:"container"`
	ContainerID    string   `json:"container_id,omitempty"`
	Managed        bool     `json:"managed"`
	Status         string   `json:"status"`
	ProxyURLs      []string `json:"proxy_urls,omitempty"`
	MaxConcurrency int      `json:"max_concurrency,omitempty"`
	QueueSize      int      `json:"queue_size,omitempty"`
	Provider       string   `json:"provider"`
	ClineTaskID    string   `json:"cline_task_id,omitempty"`
	UpstreamKeyID  string   `json:"upstream_key_id,omitempty"`
	UpstreamKeyIDs []string `json:"upstream_key_ids,omitempty"`
}
type Server struct {
	cfg                      Config
	client                   *http.Client
	mu                       sync.RWMutex
	lifecycleMu              sync.Mutex
	rotationMu               sync.Mutex
	keys                     []string
	upstreamKeys             map[string]string
	instances                []Instance
	rotationLogs             []SystemLog
	rotationFailures         map[string]string
	lastUpstream429          map[string]uint64
	clineModelsMu            sync.RWMutex
	clineModels              map[string]struct{}
	providerModelsMu         sync.RWMutex
	freeBuffModels           []string
	freeBuffModelsAt         time.Time
	freeBuffModelsRetryAt    time.Time
	freeBuffModelsRefreshing bool
	freeBuffModelsClient     *http.Client
	freeBuffModelsURLs       []string
	openCodeModelsMu         sync.RWMutex
	openCodeDynamicModels    []string
	openCodeModelsAt         time.Time
	openCodeModelsRetryAt    time.Time
	openCodeModelsRefreshing bool
	geoIPMu                  sync.Mutex
	geoIPCache               map[string]geoIPResult
	geoIPClient              *http.Client
	geoIPBaseURL             string
	providerKeys             []ProviderKey
	modelSettings            ModelSettings
	docker                   *dockerClient
	apiMu                    sync.Mutex
	apiInflight              map[string]int
	apiCircuits              map[string]apiCircuit
	apiReadiness             map[string]apiReadiness
	apiCursor                atomic.Uint64
	authMu                   sync.Mutex
	auth                     adminCredential
	sessions                 map[string]session
	loginMu                  sync.Mutex
	loginAttempts            map[string]loginAttempt
}

type GatewayKey struct {
	Key string `json:"key"`
}

type apiCircuit struct {
	Failures int
	Until    time.Time
}

type apiReadiness struct {
	Healthy bool
	Until   time.Time
}

var clineModelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}$`)

var openCodeModelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$`)

type observedResponseBody struct {
	io.ReadCloser
	onSuccess func()
	onFailure func()
	once      sync.Once
}

func (b *observedResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.once.Do(b.onSuccess)
	} else if err != nil {
		b.once.Do(b.onFailure)
	}
	return n, err
}

type AuditRecord struct {
	At               time.Time      `json:"at"`
	RequestID        string         `json:"request_id,omitempty"`
	Method           string         `json:"method"`
	Path             string         `json:"path"`
	Model            string         `json:"model,omitempty"`
	ClientModel      string         `json:"client_model,omitempty"`
	Status           int            `json:"status"`
	Slot             string         `json:"slot"`
	Egress           string         `json:"egress,omitempty"`
	LatencyMS        int64          `json:"latency_ms"`
	Source           string         `json:"source"`
	Attempts         int            `json:"attempts"`
	RetryAfter       string         `json:"retry_after,omitempty"`
	ClientKey        string         `json:"client_key,omitempty"`
	Stream           bool           `json:"stream"`
	PromptTokens     int64          `json:"prompt_tokens,omitempty"`
	CompletionTokens int64          `json:"completion_tokens,omitempty"`
	TotalTokens      int64          `json:"total_tokens,omitempty"`
	CachedTokens     int64          `json:"cached_tokens,omitempty"`
	FirstTokenMS     int64          `json:"first_token_ms,omitempty"`
	InputCostUSD     float64        `json:"input_cost_usd"`
	OutputCostUSD    float64        `json:"output_cost_usd"`
	CacheCostUSD     float64        `json:"cache_cost_usd"`
	TotalCostUSD     float64        `json:"total_cost_usd"`
	Instance         string         `json:"instance"`
	Recovered        bool           `json:"recovered,omitempty"`
	AttemptHistory   []AuditAttempt `json:"attempt_history,omitempty"`
}
type AuditAttempt struct {
	At         time.Time `json:"at"`
	Status     int       `json:"status"`
	Egress     string    `json:"egress,omitempty"`
	Source     string    `json:"source"`
	Attempt    int       `json:"attempt"`
	LatencyMS  int64     `json:"latency_ms"`
	RetryAfter string    `json:"retry_after,omitempty"`
}
type SystemLog struct {
	At       time.Time      `json:"at"`
	Level    string         `json:"level"`
	Message  string         `json:"message"`
	Fields   map[string]any `json:"fields,omitempty"`
	Instance string         `json:"instance"`
}
type LogSource struct {
	ID      string           `json:"id"`
	Label   string           `json:"label"`
	Entries []map[string]any `json:"entries"`
}
type Summary struct {
	Slots             []map[string]any  `json:"slots"`
	Stats             map[string]uint64 `json:"stats"`
	MaxConcurrency    int               `json:"max_concurrency"`
	Instance          string            `json:"instance"`
	Online            bool              `json:"online"`
	Error             string            `json:"error,omitempty"`
	Status            string            `json:"status"`
	Managed           bool              `json:"managed"`
	Container         string            `json:"container"`
	ContainerID       string            `json:"container_id,omitempty"`
	UpstreamKeyMasked string            `json:"upstream_api_key_masked"`
	UpstreamKeySet    bool              `json:"upstream_api_key_configured"`
	UpstreamKeyID     string            `json:"upstream_key_id,omitempty"`
	UpstreamKeyIDs    []string          `json:"upstream_key_ids,omitempty"`
	UpstreamKeyLabel  string            `json:"upstream_key_label,omitempty"`
	AuthMode          string            `json:"auth_mode"`
	ProxyURLs         []string          `json:"proxy_urls"`
	QueueSize         int               `json:"queue_size"`
	Provider          string            `json:"provider"`
	InTrafficPool     bool              `json:"in_traffic_pool"`
	InstanceURL       string            `json:"-"`
}

type instanceRequest struct {
	Name           string   `json:"name"`
	Provider       string   `json:"provider"`
	UpstreamAPIKey string   `json:"upstream_api_key"`
	UpstreamKeyID  string   `json:"upstream_key_id"`
	UpstreamKeyIDs []string `json:"upstream_key_ids"`
	AuthMode       string   `json:"auth_mode"`
	ProxyURLs      []string `json:"proxy_urls"`
	MaxConcurrency int      `json:"max_concurrency"`
	QueueSize      int      `json:"queue_size"`
	ClineTaskID    string   `json:"cline_task_id,omitempty"`
}

func LoadConfig() (Config, error) {
	c := Config{ListenAddr: env("CONTROL_LISTEN_ADDR", "0.0.0.0:13338"), APIListenAddr: env("API_LISTEN_ADDR", "0.0.0.0:13337"), InstanceToken: os.Getenv("INSTANCE_ADMIN_TOKEN"), DataDir: env("CONTROL_DATA_DIR", "/control-data"), DockerSocket: env("DOCKER_SOCKET", "/var/run/docker.sock"), DockerNetwork: env("GATEWAY_NETWORK", "dualroute-gateway_default"), GatewayImage: env("GATEWAY_IMAGE", "dualroute-gateway-gateway:latest"), DirectFallback: compatibleBoolEnv("DIRECT_FALLBACK", "NGINX_DIRECT_FALLBACK", false), MihomoContainer: env("MIHOMO_CONTAINER", "dualroute-gateway-mihomo"), MihomoConfigDir: env("MIHOMO_CONFIG_DIR", "/mihomo-config"), MihomoAPIURL: strings.TrimRight(env("MIHOMO_API_URL", "http://mihomo:9090"), "/"), MihomoMaxSlots: positiveIntEnv("MIHOMO_MAX_SLOTS", 64), MaxInstances: positiveIntEnv("MAX_INSTANCES", 16), FreeBuffModelsProxy: strings.TrimSpace(env("FREEBUFF_MODELS_PROXY_URL", "")), FreeBuffModelsURLs: split(env("FREEBUFF_MODELS_URLS", freeBuffModelsURL))}
	if len(c.FreeBuffModelsURLs) == 0 {
		c.FreeBuffModelsURLs = []string{freeBuffModelsURL}
	}
	for _, raw := range c.FreeBuffModelsURLs {
		modelURL, err := url.Parse(raw)
		if err != nil || modelURL.Scheme != "https" || modelURL.Host == "" {
			return c, fmt.Errorf("FREEBUFF_MODELS_URLS must contain absolute HTTPS URLs")
		}
	}
	if c.FreeBuffModelsProxy != "" {
		proxyURL, err := url.Parse(c.FreeBuffModelsProxy)
		if err != nil || (proxyURL.Scheme != "http" && proxyURL.Scheme != "https") || proxyURL.Host == "" {
			return c, fmt.Errorf("FREEBUFF_MODELS_PROXY_URL must be an absolute HTTP(S) URL")
		}
	}
	if c.MihomoMaxSlots > 128 {
		c.MihomoMaxSlots = 128
	}
	for i, raw := range split(os.Getenv("GATEWAY_INSTANCES")) {
		parts := strings.SplitN(raw, "=", 2)
		instance := Instance{Name: fmt.Sprintf("gateway-%d", i+1), URL: raw}
		if len(parts) == 2 {
			instance.Name, instance.URL = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		}
		u, err := url.Parse(instance.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return c, fmt.Errorf("invalid GATEWAY_INSTANCES entry %q", raw)
		}
		instance.URL = strings.TrimRight(instance.URL, "/")
		c.Instances = append(c.Instances, instance)
	}
	return c, nil
}

func New(cfg Config) *Server {
	s := &Server{cfg: cfg, client: &http.Client{Timeout: 15 * time.Second}, docker: newDockerClient(cfg.DockerSocket), apiInflight: make(map[string]int), apiCircuits: make(map[string]apiCircuit), apiReadiness: make(map[string]apiReadiness), upstreamKeys: make(map[string]string), rotationFailures: make(map[string]string), lastUpstream429: make(map[string]uint64), sessions: make(map[string]session), loginAttempts: make(map[string]loginAttempt), clineModels: make(map[string]struct{}), modelSettings: defaultModelSettings(), freeBuffModels: append([]string(nil), freeBuffFallbackModels...), geoIPCache: make(map[string]geoIPResult), geoIPClient: &http.Client{Timeout: 3 * time.Second}, geoIPBaseURL: env("GEOIP_URL", "https://ipwho.is")}
	s.freeBuffModelsURLs = append([]string(nil), cfg.FreeBuffModelsURLs...)
	if len(s.freeBuffModelsURLs) == 0 {
		s.freeBuffModelsURLs = []string{freeBuffModelsURL}
	}
	if cfg.FreeBuffModelsProxy != "" {
		proxyURL, _ := url.Parse(cfg.FreeBuffModelsProxy)
		s.freeBuffModelsClient = &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}, Timeout: 5 * time.Second}
	}
	s.loadInstanceToken()
	s.loadAuth()
	s.loadKeys()
	s.loadUpstreamKeys()
	s.loadProviderKeys()
	s.loadModelSettings()
	if len(s.keys) == 0 {
		s.keys = append([]string(nil), cfg.BootstrapKeys...)
		s.persistLocked()
	}
	s.loadInstances()
	if len(s.instances) == 0 {
		s.instances = cfg.Instances
	}
	return s
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.page)
	mux.HandleFunc("/static/", s.static)
	mux.HandleFunc("/api/", s.api)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, `{"status":"ok"}`) })
	return mux
}

// APIHandler is the public data-plane listener. It validates the client key
// before routing so instance-specific keys cannot reach another instance.
func (s *Server) APIHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.proxyAPI)
	for _, prefix := range []string{"/v1/", "/openai/", "/anthropic/", "/codex/"} {
		mux.HandleFunc(prefix, s.proxyAPI)
	}
	return mux
}

func (s *Server) proxyAPI(w http.ResponseWriter, r *http.Request) {
	apiKey := requestCredential(r)
	s.mu.RLock()
	instances := append([]Instance(nil), s.instances...)
	shared := s.hasGatewayKeyLocked(apiKey)
	s.mu.RUnlock()
	if apiKey == "" || !shared {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	if isModelsRequest(r.URL.Path) {
		s.models(w, instances)
		return
	}
	if provider, required, err := s.requestedProvider(r); err != nil {
		if errors.Is(err, errAPIRequestBodyTooLarge) {
			writeAPIError(w, http.StatusRequestEntityTooLarge, "request_body_too_large", err)
		} else {
			writeAPIError(w, http.StatusBadRequest, "unsupported_model", err)
		}
		return
	} else if required {
		instances = instancesForProvider(instances, provider)
	}
	selected := s.readyTrafficPool(selectTrafficPool(instances, s.cfg.DirectFallback))
	if len(selected) == 0 {
		http.Error(w, `{"error":"no_healthy_gateway_instances"}`, http.StatusServiceUnavailable)
		return
	}
	instance := s.acquireAPIInstance(selected)
	if instance.Name == "" {
		http.Error(w, `{"error":"all_gateway_instances_cooling_down"}`, http.StatusServiceUnavailable)
		return
	}
	target, err := url.Parse(instance.URL)
	if err != nil || target.Host == "" {
		http.Error(w, `{"error":"invalid_gateway_instance"}`, http.StatusBadGateway)
		return
	}
	defer s.releaseAPIInstance(instance.Name)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, proxyErr error) {
		s.markAPIInstanceFailure(instance.Name)
		http.Error(w, `{"error":"gateway_unavailable"}`, http.StatusBadGateway)
	}
	proxy.ModifyResponse = func(response *http.Response) error {
		instanceHealthy := strings.EqualFold(strings.TrimSpace(response.Header.Get(instanceHealthyHeader)), "true")
		response.Header.Del(instanceHealthyHeader)
		if response.StatusCode == http.StatusTooManyRequests {
			if strings.TrimSpace(response.Header.Get("Retry-After")) == "" {
				response.Header.Set("Retry-After", "60")
			}
			if providerOrDefault(instance.Provider) == ProviderTokenRouter {
				// TokenRouter limits are account/key scoped, so select another
				// instance. OpenCode and Cline manage exit/model cooling internally.
				s.markAPIInstanceRateLimit(instance.Name, response.Header.Get("Retry-After"))
			}
			return nil
		}
		if response.StatusCode >= http.StatusInternalServerError {
			if instanceHealthy {
				s.markAPIInstanceSuccess(instance.Name)
			} else {
				s.markAPIInstanceFailure(instance.Name)
			}
			return nil
		}
		response.Body = &observedResponseBody{
			ReadCloser: response.Body,
			onSuccess:  func() { s.markAPIInstanceSuccess(instance.Name) },
			onFailure:  func() { s.markAPIInstanceFailure(instance.Name) },
		}
		return nil
	}
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
	}
	proxy.ServeHTTP(w, r)
}

func isModelsRequest(path string) bool {
	return path == "/v1/models" || path == "/openai/v1/models"
}

func (s *Server) requestedProvider(r *http.Request) (string, bool, error) {
	if r.Body == nil || r.Method == http.MethodGet || r.Method == http.MethodHead {
		return "", false, nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxAPIRequestBodyBytes+1))
	if err != nil {
		return "", false, fmt.Errorf("request_body_unreadable")
	}
	if len(body) > maxAPIRequestBodyBytes {
		return "", false, errAPIRequestBodyTooLarge
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	var request struct {
		Model string `json:"model"`
	}
	if len(body) == 0 || json.Unmarshal(body, &request) != nil || strings.TrimSpace(request.Model) == "" {
		if isClineClientType(r.Header.Get("X-CLIENT-TYPE")) {
			if !s.providerEnabled(ProviderCline) {
				return "", false, fmt.Errorf("provider %q is disabled", ProviderCline)
			}
			return ProviderCline, true, nil
		}
		return "", false, nil
	}
	if isClineClientType(r.Header.Get("X-CLIENT-TYPE")) {
		if !s.registerClineModel(request.Model) {
			return "", false, fmt.Errorf("invalid or excessive Cline model identifier")
		}
		clineModel, _ := s.providerModelFromClient(ProviderCline, request.Model)
		if !s.modelEnabled(ProviderCline, clineModel) {
			return "", false, fmt.Errorf("model %q is disabled", request.Model)
		}
		return ProviderCline, true, nil
	}
	model := strings.TrimSpace(request.Model)
	if provider, upstreamModel, ok := s.providerFromClientModel(model); ok {
		if provider == ProviderFreeBuff && strings.HasPrefix(strings.ToLower(model), "freebuff/") {
			s.refreshFreeBuffModels()
			for _, candidate := range s.modelCatalog() {
				if candidate.ID != ProviderFreeBuff {
					continue
				}
				for _, item := range candidate.Models {
					if strings.EqualFold(item.ClientModel, model) {
						upstreamModel = item.ID
						break
					}
				}
			}
		}
		if !s.modelEnabled(provider, upstreamModel) {
			return "", false, fmt.Errorf("model %q is disabled", model)
		}
		if provider == ProviderFreeBuff && !catalogContains(s.modelCatalog(), provider, upstreamModel) {
			return "", false, fmt.Errorf("unknown FreeBuff model %q", upstreamModel)
		}
		return provider, true, nil
	}
	if strings.HasPrefix(strings.ToLower(model), "freebuff/") {
		for _, candidate := range s.modelCatalog() {
			if candidate.ID != ProviderFreeBuff {
				continue
			}
			for _, item := range candidate.Models {
				if item.ClientModel == model {
					if !s.modelEnabled(ProviderFreeBuff, item.ID) {
						return "", false, fmt.Errorf("model %q is disabled", model)
					}
					return ProviderFreeBuff, true, nil
				}
			}
		}
	}
	if strings.HasPrefix(strings.ToLower(model), "opencode/") {
		// A model added upstream after the last catalog refresh is resolved by
		// triggering a refresh and re-checking the dynamic list once.
		s.refreshOpenCodeModels()
		for _, candidate := range s.modelCatalog() {
			if candidate.ID != ProviderOpenCode {
				continue
			}
			for _, item := range candidate.Models {
				if strings.EqualFold(item.ClientModel, model) {
					if !s.modelEnabled(ProviderOpenCode, item.ID) {
						return "", false, fmt.Errorf("model %q is disabled", model)
					}
					return ProviderOpenCode, true, nil
				}
			}
		}
	}
	{
		s.clineModelsMu.RLock()
		_, clineModel := s.clineModels[model]
		s.clineModelsMu.RUnlock()
		if clineModel {
			if !s.modelEnabled(ProviderCline, model) {
				return "", false, fmt.Errorf("model %q is disabled", model)
			}
			return ProviderCline, true, nil
		}
		return "", false, fmt.Errorf("model must use a configured provider alias")
	}
}

func isClineClientType(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "cline-vscode" || value == "cline-cli"
}

func (s *Server) registerClineModel(model string) bool {
	model = strings.TrimSpace(model)
	if upstream, ok := s.providerModelFromClient(ProviderCline, model); ok {
		model = upstream
	}
	if len(model) == 0 || len(model) > maxClineModelLength || !clineModelPattern.MatchString(model) {
		return false
	}
	s.clineModelsMu.Lock()
	if _, exists := s.clineModels[model]; !exists && len(s.clineModels) >= maxClineModels {
		s.clineModelsMu.Unlock()
		return false
	}
	s.clineModels[model] = struct{}{}
	s.clineModelsMu.Unlock()
	return true
}

func (s *Server) models(w http.ResponseWriter, instances []Instance) {
	s.refreshFreeBuffModels()
	s.refreshOpenCodeModels()
	s.refreshClineModels(instances)
	providers := make(map[string]bool)
	for _, instance := range instances {
		providers[providerOrDefault(instance.Provider)] = true
	}
	models := make([]map[string]any, 0, 16)
	for _, group := range s.modelCatalog() {
		if !providers[group.ID] || !group.Enabled {
			continue
		}
		for _, catalogModel := range group.Models {
			if !catalogModel.Enabled {
				continue
			}
			model := map[string]any{"id": catalogModel.ClientModel, "object": "model", "owned_by": group.ID}
			if group.ID == ProviderOpenCode {
				model["contextWindow"] = 1000000
				model["supportsReasoningEffort"] = true
				model["reasoningEffort"] = "none"
				model["reasoningEfforts"] = []map[string]any{{"value": "none", "label": "None", "default": true}, {"value": "low", "label": "Low"}, {"value": "medium", "label": "Medium"}, {"value": "high", "label": "High"}}
			} else if group.ID == ProviderCline {
				model["contextWindow"] = 131072
				model["maxTokens"] = 4096
				model["reasoning"] = true
			} else if group.ID == ProviderFreeBuff {
				model["reasoning"] = true
			}
			models = append(models, model)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": models})
}

func (s *Server) refreshClineModels(instances []Instance) {
	var key string
	s.mu.RLock()
	if len(s.keys) > 0 {
		key = s.keys[0]
	}
	s.mu.RUnlock()
	if key == "" {
		return
	}
	for _, instance := range instances {
		if providerOrDefault(instance.Provider) != ProviderCline || instance.Status != "running" || instance.URL == "" {
			continue
		}
		req, err := http.NewRequest(http.MethodGet, strings.TrimRight(instance.URL, "/")+"/v1/models", nil)
		if err != nil {
			continue
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := s.client.Do(req)
		if err != nil {
			continue
		}
		var payload struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&payload)
		resp.Body.Close()
		if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
			continue
		}
		models := make(map[string]struct{})
		for _, model := range payload.Data {
			if id := strings.TrimSpace(model.ID); id != "" {
				models[id] = struct{}{}
			}
		}
		if len(models) == 0 {
			continue
		}
		s.clineModelsMu.Lock()
		for id := range models {
			s.clineModels[id] = struct{}{}
		}
		s.clineModelsMu.Unlock()
	}
}

func (s *Server) readyTrafficPool(instances []Instance) []Instance {
	if len(instances) == 0 {
		return nil
	}
	ready := make([]bool, len(instances))
	jobs := make(chan int)
	workers := min(8, len(instances))
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				ready[index] = s.apiInstanceReady(instances[index])
			}
		}()
	}
	for index := range instances {
		jobs <- index
	}
	close(jobs)
	wg.Wait()

	result := make([]Instance, 0, len(instances))
	for index, instance := range instances {
		if ready[index] {
			result = append(result, instance)
		}
	}
	return result
}

func (s *Server) apiInstanceReady(instance Instance) bool {
	now := time.Now()
	s.apiMu.Lock()
	cached, exists := s.apiReadiness[instance.Name]
	s.apiMu.Unlock()
	if exists && now.Before(cached.Until) {
		return cached.Healthy
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(instance.URL, "/")+"/healthz", nil)
	healthy := false
	probeError := ""
	if err == nil {
		response, requestErr := s.client.Do(req)
		if requestErr == nil {
			healthy = response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices
			if !healthy {
				probeError = fmt.Sprintf("healthz returned HTTP %d", response.StatusCode)
			}
			response.Body.Close()
		} else {
			probeError = requestErr.Error()
		}
	} else {
		probeError = err.Error()
	}
	cacheFor := 5 * time.Second
	if !healthy {
		cacheFor = time.Second
	}
	s.apiMu.Lock()
	s.apiReadiness[instance.Name] = apiReadiness{Healthy: healthy, Until: time.Now().Add(cacheFor)}
	s.apiMu.Unlock()
	if !healthy && (!exists || cached.Healthy) {
		s.addRotationLog("warn", "gateway health preflight failed", instance.Name, map[string]any{"error": probeError})
	}
	return healthy
}

func (s *Server) acquireAPIInstance(instances []Instance) Instance {
	s.apiMu.Lock()
	defer s.apiMu.Unlock()
	now := time.Now()
	available := make([]Instance, 0, len(instances))
	for _, instance := range instances {
		circuit, exists := s.apiCircuits[instance.Name]
		if exists && !now.Before(circuit.Until) {
			delete(s.apiCircuits, instance.Name)
			exists = false
		}
		if !exists {
			available = append(available, instance)
		}
	}
	if len(available) == 0 {
		return Instance{}
	}
	instances = available
	var selected Instance
	selectedLoad := int(^uint(0) >> 1)
	start := int(s.apiCursor.Add(1)-1) % len(instances)
	for i := range instances {
		instance := instances[(start+i)%len(instances)]
		load := s.apiInflight[instance.Name]
		if load < selectedLoad {
			selected, selectedLoad = instance, load
		}
	}
	s.apiInflight[selected.Name]++
	return selected
}

func (s *Server) markAPIInstanceFailure(name string) {
	s.apiMu.Lock()
	defer s.apiMu.Unlock()
	circuit := s.apiCircuits[name]
	circuit.Failures++
	cooldown := 15 * time.Second
	for step := 1; step < circuit.Failures && cooldown < 2*time.Minute; step++ {
		cooldown *= 2
	}
	if cooldown > 2*time.Minute {
		cooldown = 2 * time.Minute
	}
	circuit.Until = time.Now().Add(cooldown)
	s.apiCircuits[name] = circuit
	s.apiReadiness[name] = apiReadiness{Healthy: false, Until: time.Now().Add(time.Second)}
}

func (s *Server) markAPIInstanceRateLimit(name, retryAfter string) {
	cooldown := time.Minute
	if seconds, err := strconv.Atoi(strings.TrimSpace(retryAfter)); err == nil && seconds > 0 {
		cooldown = time.Duration(seconds) * time.Second
	}
	s.apiMu.Lock()
	circuit := s.apiCircuits[name]
	circuit.Failures++
	circuit.Until = time.Now().Add(cooldown)
	s.apiCircuits[name] = circuit
	s.apiReadiness[name] = apiReadiness{Healthy: false, Until: time.Now().Add(time.Second)}
	s.apiMu.Unlock()
}

func (s *Server) markAPIInstanceSuccess(name string) {
	s.apiMu.Lock()
	delete(s.apiCircuits, name)
	s.apiReadiness[name] = apiReadiness{Healthy: true, Until: time.Now().Add(5 * time.Second)}
	s.apiMu.Unlock()
}

func (s *Server) releaseAPIInstance(name string) {
	s.apiMu.Lock()
	defer s.apiMu.Unlock()
	if s.apiInflight[name] > 1 {
		s.apiInflight[name]--
	} else {
		delete(s.apiInflight, name)
	}
}
func (s *Server) page(w http.ResponseWriter, r *http.Request) {
	page := strings.Trim(strings.TrimSpace(r.URL.Path), "/")
	if page == "" {
		page = "instances"
	}
	titles := map[string]string{"instances": "实例与出口", "mihomo": "Mihomo 转换", "upstreams": "上游密钥", "models": "模型管理", "keys": "访问密钥", "logs": "审计与日志", "tokens": "API Token 统计"}
	title, ok := titles[page]
	if !ok {
		http.NotFound(w, r)
		return
	}
	layout, _ := assets.ReadFile("web/index.html")
	content, err := assets.ReadFile("web/pages/" + page + ".html")
	if err != nil {
		http.Error(w, `{"error":"page_unavailable"}`, http.StatusInternalServerError)
		return
	}
	data := strings.ReplaceAll(string(layout), "{{PAGE_CONTENT}}", string(content))
	data = strings.ReplaceAll(data, "{{PAGE_NAME}}", page)
	data = strings.ReplaceAll(data, "{{PAGE_TITLE}}", title)
	w.Header().Set("content-type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, data)
}
func (s *Server) static(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/static/")
	data, err := assets.ReadFile("web/" + name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if strings.HasSuffix(name, ".js") {
		w.Header().Set("content-type", "application/javascript; charset=utf-8")
	} else if strings.HasSuffix(name, ".css") {
		w.Header().Set("content-type", "text/css; charset=utf-8")
	}
	// Asset URLs are versioned in the page; prevent an intermediary from
	// keeping an unversioned stylesheet or script stale after an upgrade.
	w.Header().Set("Cache-Control", "no-cache, must-revalidate")
	w.Write(data)
}
func (s *Server) api(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api")
	switch {
	case path == "/auth/status" && r.Method == http.MethodGet:
		s.authStatus(w, r)
		return
	case path == "/auth/login" && r.Method == http.MethodPost:
		s.login(w, r)
		return
	case path == "/auth/password" && r.Method == http.MethodPost:
		s.changePassword(w, r)
		return
	case path == "/auth/logout" && r.Method == http.MethodPost:
		s.logout(w, r)
		return
	}
	_, authenticated, mustChange := s.authenticated(r)
	if !authenticated {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	if mustChange {
		http.Error(w, `{"error":"password_change_required"}`, http.StatusForbidden)
		return
	}
	switch {
	case path == "/overview" && r.Method == "GET":
		s.overview(w)
	case path == "/tokens" && r.Method == "GET":
		s.tokens(w, r)
	case path == "/keys" && r.Method == "GET":
		s.mu.RLock()
		keys := append([]string(nil), s.keys...)
		s.mu.RUnlock()
		writeJSON(w, 200, map[string]any{"keys": keys})
	case path == "/keys" && r.Method == "POST":
		s.createKey(w, r)
	case path == "/provider-keys":
		s.providerKeysAPI(w, r)
	case strings.HasPrefix(path, "/provider-keys/") && r.Method == http.MethodDelete:
		s.deleteProviderKey(w, strings.TrimPrefix(path, "/provider-keys/"))
	case path == "/model-settings":
		s.modelSettingsAPI(w, r)
	case path == "/mihomo" && r.Method == "GET":
		s.mihomoStatus(w)
	case path == "/mihomo/probe" && r.Method == "POST":
		s.mihomoProbe(w)
	case path == "/mihomo" && r.Method == "PUT":
		s.mihomoUpdate(w, r)
	case strings.HasPrefix(path, "/keys/") && r.Method == "DELETE":
		s.deleteKey(w, strings.TrimPrefix(path, "/keys/"))
	case path == "/slots" && r.Method == "POST":
		s.broadcast(w, r, "/admin/slots")
	case path == "/refresh" && r.Method == "POST":
		s.broadcast(w, r, "/admin/refresh")
	case path == "/instances" && r.Method == "GET":
		s.instancesList(w)
	case path == "/instances" && r.Method == "POST":
		s.instanceCreate(w, r)
	case strings.HasPrefix(path, "/instances/") && r.Method == "PUT":
		s.instanceUpdate(w, r, strings.TrimPrefix(path, "/instances/"))
	case strings.HasPrefix(path, "/instances/") && r.Method == "POST":
		s.instanceAction(w, r, strings.TrimPrefix(path, "/instances/"))
	case strings.HasPrefix(path, "/instances/") && r.Method == "DELETE":
		s.instanceDelete(w, strings.TrimPrefix(path, "/instances/"))
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) instancesList(w http.ResponseWriter) {
	instances, err := s.discoverInstances()
	if err != nil {
		http.Error(w, `{"error":"docker_unavailable"}`, http.StatusBadGateway)
		return
	}
	s.mu.Lock()
	s.instances = instances
	s.persistInstancesLocked()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"instances": instances})
}

func (s *Server) instanceCreate(w http.ResponseWriter, r *http.Request) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	var request instanceRequest
	if json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&request) != nil {
		http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
		return
	}
	if code, message := validateInstanceRequest(&request, true); code != 0 {
		http.Error(w, message, code)
		return
	}
	instances, err := s.discoverInstances()
	if err != nil {
		http.Error(w, `{"error":"docker_unavailable"}`, http.StatusBadGateway)
		return
	}
	request.Name = normalizeInstanceName(request.Name)
	if request.Name == "" {
		request.Name = nextInstanceName(instances)
	}
	if !instanceNamePattern.MatchString(request.Name) {
		http.Error(w, `{"error":"invalid_instance_name"}`, http.StatusBadRequest)
		return
	}
	for _, instance := range instances {
		if instance.Name == request.Name {
			http.Error(w, `{"error":"instance_exists"}`, http.StatusConflict)
			return
		}
	}
	if len(instances) >= s.cfg.MaxInstances {
		http.Error(w, `{"error":"max_instances_reached"}`, http.StatusConflict)
		return
	}
	if request.Provider == ProviderCline {
		request.ClineTaskID = newUUID()
	}
	if request.Provider == ProviderOpenCode {
		request.AuthMode = providerAuthModePublic
		request.UpstreamAPIKey = providerAuthModePublic
		request.UpstreamKeyID = ""
	} else if len(request.UpstreamKeyIDs) > 0 || request.UpstreamKeyID != "" {
		ids := request.UpstreamKeyIDs
		if len(ids) == 0 {
			ids = []string{request.UpstreamKeyID}
		}
		secrets := make([]string, 0, len(ids))
		for _, id := range ids {
			secret, found := s.providerKeySecret(id, request.Provider)
			if !found {
				http.Error(w, `{"error":"provider_key_not_found"}`, http.StatusBadRequest)
				return
			}
			secrets = append(secrets, secret)
		}
		request.UpstreamKeyIDs = ids
		request.UpstreamKeyID = ids[0]
		request.UpstreamAPIKey = strings.Join(secrets, ",")
		if request.UpstreamAPIKey == "" {
			http.Error(w, `{"error":"provider_key_not_found"}`, http.StatusBadRequest)
			return
		}
	} else if request.UpstreamAPIKey == "" {
		http.Error(w, `{"error":"upstream_key_id_required"}`, http.StatusBadRequest)
		return
	}
	if owner := proxyURLOwner(instances, request.Name, request.Provider, request.ProxyURLs); owner != "" {
		http.Error(w, `{"error":"proxy_url_in_use"}`, http.StatusConflict)
		return
	}
	instance := Instance{Name: request.Name, Container: request.Name, URL: "http://" + request.Name + ":13339", Managed: true, Status: "created", ProxyURLs: request.ProxyURLs, MaxConcurrency: request.MaxConcurrency, QueueSize: request.QueueSize, Provider: request.Provider, ClineTaskID: request.ClineTaskID, UpstreamKeyID: request.UpstreamKeyID, UpstreamKeyIDs: request.UpstreamKeyIDs}
	s.mu.RLock()
	keys := s.gatewayKeysLocked()
	s.mu.RUnlock()
	if err := s.docker.create(s.cfg, instance, keys, request.UpstreamAPIKey); err != nil {
		writeAPIError(w, http.StatusBadGateway, "create_failed", err)
		return
	}
	if err := s.docker.action(request.Name, "start"); err != nil {
		_ = s.docker.remove(request.Name)
		writeAPIError(w, http.StatusBadGateway, "start_failed", err)
		return
	}
	if err := s.docker.waitHealthy(request.Name); err != nil {
		_ = s.docker.remove(request.Name)
		writeAPIError(w, http.StatusBadGateway, "health_check_failed", err)
		return
	}
	if err := s.refreshInstances(); err != nil {
		_ = s.docker.remove(request.Name)
		_ = s.refreshInstances()
		writeAPIError(w, http.StatusBadGateway, "route_refresh_failed", err)
		return
	}
	instances, err = s.discoverInstances()
	if err != nil {
		_ = s.docker.remove(request.Name)
		_ = s.refreshInstances()
		writeAPIError(w, http.StatusBadGateway, "docker_verify_failed", err)
		return
	}
	var created Instance
	for _, current := range instances {
		if current.Name == request.Name {
			created = current
			break
		}
	}
	if created.ContainerID == "" || created.Status != "running" {
		_ = s.docker.remove(request.Name)
		_ = s.refreshInstances()
		writeAPIError(w, http.StatusBadGateway, "container_not_running", fmt.Errorf("container %q has status %q", request.Name, created.Status))
		return
	}
	s.mu.Lock()
	s.instances = instances
	if request.UpstreamKeyID == "" {
		s.upstreamKeys[request.Name] = request.UpstreamAPIKey
	} else {
		delete(s.upstreamKeys, request.Name)
	}
	s.persistInstancesLocked()
	s.persistUpstreamKeysLocked()
	s.mu.Unlock()
	s.addRotationLog("info", "instance created", request.Name, map[string]any{"proxy_slots": len(request.ProxyURLs), "max_concurrency": request.MaxConcurrency})
	go func() { _ = s.ReconcileEgresses() }()
	writeJSON(w, http.StatusCreated, map[string]any{"instance": created})
}

func (s *Server) instanceUpdate(w http.ResponseWriter, r *http.Request, name string) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if !instanceNamePattern.MatchString(name) {
		http.Error(w, `{"error":"invalid_instance_name"}`, http.StatusBadRequest)
		return
	}
	var request instanceRequest
	if json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&request) != nil {
		http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
		return
	}
	request.Name = name
	var currentInstance Instance
	if request.Provider == "" {
		s.mu.RLock()
		for _, instance := range s.instances {
			if instance.Name == name {
				currentInstance = instance
				request.Provider = instance.Provider
				break
			}
		}
		s.mu.RUnlock()
	} else {
		s.mu.RLock()
		for _, instance := range s.instances {
			if instance.Name == name {
				currentInstance = instance
				break
			}
		}
		s.mu.RUnlock()
	}
	s.mu.RLock()
	currentKey := s.upstreamKeys[name]
	s.mu.RUnlock()
	if request.UpstreamKeyID != "" {
		request.AuthMode = providerAuthModeCustom
	}
	if strings.TrimSpace(request.AuthMode) == "" && strings.TrimSpace(request.UpstreamAPIKey) == "" {
		request.AuthMode, request.UpstreamAPIKey = providerAuthModeForKey(currentKey), currentKey
		if request.AuthMode == "" {
			if request.Provider == ProviderOpenCode {
				request.AuthMode = providerAuthModePublic
			} else {
				request.AuthMode = providerAuthModeCustom
			}
		}
	}
	if request.AuthMode == providerAuthModeCustom && strings.TrimSpace(request.UpstreamAPIKey) == "" && currentKey != "" && currentKey != providerAuthModePublic {
		request.UpstreamAPIKey = currentKey
	}
	if code, message := validateInstanceRequest(&request, false); code != 0 {
		http.Error(w, message, code)
		return
	}
	if request.Provider == ProviderOpenCode {
		request.AuthMode = providerAuthModePublic
		request.UpstreamAPIKey = providerAuthModePublic
		request.UpstreamKeyID = ""
	} else {
		if request.UpstreamKeyID == "" && currentInstance.Provider == request.Provider {
			request.UpstreamKeyID = currentInstance.UpstreamKeyID
			request.UpstreamKeyIDs = append([]string(nil), currentInstance.UpstreamKeyIDs...)
		}
		if len(request.UpstreamKeyIDs) > 0 || request.UpstreamKeyID != "" {
			ids := request.UpstreamKeyIDs
			if len(ids) == 0 {
				ids = []string{request.UpstreamKeyID}
			}
			secrets := make([]string, 0, len(ids))
			for _, id := range ids {
				secret, found := s.providerKeySecret(id, request.Provider)
				if !found {
					http.Error(w, `{"error":"provider_key_not_found"}`, http.StatusBadRequest)
					return
				}
				secrets = append(secrets, secret)
			}
			request.UpstreamKeyIDs = ids
			request.UpstreamKeyID = ids[0]
			request.UpstreamAPIKey = strings.Join(secrets, ",")
			if request.UpstreamAPIKey == "" {
				http.Error(w, `{"error":"provider_key_not_found"}`, http.StatusBadRequest)
				return
			}
		} else if request.UpstreamAPIKey == "" {
			http.Error(w, `{"error":"upstream_key_id_required"}`, http.StatusBadRequest)
			return
		}
	}
	if request.Provider == ProviderCline {
		request.ClineTaskID = ""
		s.mu.RLock()
		for _, current := range s.instances {
			if current.Name == name {
				request.ClineTaskID = current.ClineTaskID
				break
			}
		}
		s.mu.RUnlock()
		if request.ClineTaskID == "" {
			request.ClineTaskID = newUUID()
		}
	}
	s.mu.RLock()
	knownInstances := append([]Instance(nil), s.instances...)
	s.mu.RUnlock()
	if owner := proxyURLOwner(knownInstances, name, request.Provider, request.ProxyURLs); owner != "" {
		http.Error(w, `{"error":"proxy_url_in_use"}`, http.StatusConflict)
		return
	}
	instance := Instance{Name: name, Container: name, URL: "http://" + name + ":13339", Managed: true, ProxyURLs: request.ProxyURLs, MaxConcurrency: request.MaxConcurrency, QueueSize: request.QueueSize, Provider: request.Provider, ClineTaskID: request.ClineTaskID, UpstreamKeyID: request.UpstreamKeyID, UpstreamKeyIDs: request.UpstreamKeyIDs}
	s.mu.RLock()
	keys := s.gatewayKeysLocked()
	s.mu.RUnlock()
	if err := s.docker.replace(s.cfg, instance, keys, request.UpstreamAPIKey); err != nil {
		http.Error(w, `{"error":"update_failed"}`, http.StatusBadGateway)
		return
	}
	if err := s.refreshInstances(); err != nil {
		http.Error(w, `{"error":"route_refresh_failed"}`, http.StatusBadGateway)
		return
	}
	s.mu.Lock()
	if request.UpstreamKeyID == "" {
		s.upstreamKeys[name] = request.UpstreamAPIKey
	} else {
		delete(s.upstreamKeys, name)
	}
	s.persistUpstreamKeysLocked()
	s.mu.Unlock()
	s.addRotationLog("info", "instance settings updated", name, map[string]any{"proxy_slots": len(request.ProxyURLs), "max_concurrency": request.MaxConcurrency})
	go func() { _ = s.ReconcileEgresses() }()
	writeJSON(w, http.StatusOK, map[string]any{"instance": instance})
}

func validateInstanceRequest(request *instanceRequest, defaultLimits bool) (int, string) {
	request.Provider = strings.ToLower(strings.TrimSpace(request.Provider))
	if request.Provider == "" {
		request.Provider = ProviderTokenRouter
	}
	if request.Provider != ProviderTokenRouter && request.Provider != ProviderOpenCode && request.Provider != ProviderCline && request.Provider != ProviderFreeBuff {
		return http.StatusBadRequest, `{"error":"invalid_provider"}`
	}
	request.AuthMode = strings.ToLower(strings.TrimSpace(request.AuthMode))
	request.UpstreamAPIKey = strings.TrimSpace(request.UpstreamAPIKey)
	if request.Provider == ProviderOpenCode {
		request.AuthMode = providerAuthModePublic
		request.UpstreamAPIKey = providerAuthModePublic
	} else if request.AuthMode == "" {
		if request.UpstreamAPIKey != "" {
			request.AuthMode = providerAuthModeCustom
		} else if defaultLimits {
			request.AuthMode = providerAuthModeCustom
		}
	}
	if request.AuthMode != "" && request.AuthMode != providerAuthModePublic && request.AuthMode != providerAuthModeCustom {
		return http.StatusBadRequest, `{"error":"invalid_auth_mode"}`
	}
	if request.AuthMode == providerAuthModePublic {
		if request.Provider != ProviderOpenCode {
			return http.StatusBadRequest, `{"error":"public_auth_not_supported"}`
		}
		request.UpstreamAPIKey = providerAuthModePublic
	}
	if request.AuthMode == providerAuthModeCustom && request.UpstreamAPIKey == "" && request.UpstreamKeyID == "" && len(request.UpstreamKeyIDs) == 0 {
		return http.StatusBadRequest, `{"error":"upstream_api_key_required"}`
	}
	if request.UpstreamAPIKey != "" {
		for _, key := range split(request.UpstreamAPIKey) {
			if len(key) > 512 || strings.IndexFunc(key, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
				return http.StatusBadRequest, `{"error":"invalid_upstream_api_key"}`
			}
		}
	}
	if request.AuthMode == providerAuthModeCustom && request.UpstreamAPIKey == providerAuthModePublic {
		return http.StatusBadRequest, `{"error":"custom_key_cannot_be_public"}`
	}
	if defaultLimits && request.MaxConcurrency == 0 {
		request.MaxConcurrency = 4
	}
	if defaultLimits && request.QueueSize == 0 {
		request.QueueSize = 8
	}
	if request.MaxConcurrency < 1 || request.MaxConcurrency > 64 || request.QueueSize < 0 || request.QueueSize > 256 {
		return http.StatusBadRequest, `{"error":"invalid_limits"}`
	}
	for i, raw := range request.ProxyURLs {
		request.ProxyURLs[i] = strings.TrimSpace(raw)
		parsed, err := url.Parse(request.ProxyURLs[i])
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https" && parsed.Scheme != "socks5" && parsed.Scheme != "socks5h") {
			return http.StatusBadRequest, `{"error":"proxy_urls_must_be_http_or_socks5"}`
		}
	}
	return 0, ""
}

func proxyURLOwner(instances []Instance, currentName, provider string, proxyURLs []string) string {
	wanted := make(map[string]struct{}, len(proxyURLs))
	for _, raw := range proxyURLs {
		if value := strings.TrimSpace(raw); value != "" {
			wanted[value] = struct{}{}
		}
	}
	if len(wanted) == 0 {
		return ""
	}
	provider = providerOrDefault(provider)
	for _, instance := range instances {
		if instance.Name == currentName || providerOrDefault(instance.Provider) != provider {
			continue
		}
		for _, raw := range instance.ProxyURLs {
			if _, exists := wanted[strings.TrimSpace(raw)]; exists {
				return instance.Name
			}
		}
	}
	return ""
}

func normalizeInstanceName(raw string) string {
	name := strings.ToLower(strings.TrimSpace(raw))
	name = strings.NewReplacer("_", "-", " ", "-").Replace(name)
	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}
	name = strings.Trim(name, "-")
	if name == "" {
		return ""
	}
	if !strings.HasPrefix(name, "gateway-") {
		name = "gateway-" + name
	}
	return name
}

func nextInstanceName(instances []Instance) string {
	used := make(map[string]struct{}, len(instances))
	for _, instance := range instances {
		used[instance.Name] = struct{}{}
	}
	for suffix := 'a'; suffix <= 'z'; suffix++ {
		candidate := "gateway-" + string(suffix)
		if _, exists := used[candidate]; !exists {
			return candidate
		}
	}
	for number := 2; ; number++ {
		candidate := "gateway-" + strconv.Itoa(number)
		if _, exists := used[candidate]; !exists {
			return candidate
		}
	}
}

func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		n := uint64(time.Now().UnixNano())
		return fmt.Sprintf("%08x-%04x-4%03x-8%03x-%012x", n>>32, n>>16&0xffff, n&0x0fff, n>>12&0x0fff, n&0xffffffffffff)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]), hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]), hex.EncodeToString(b[10:16]))
}

func (s *Server) instanceAction(w http.ResponseWriter, r *http.Request, name string) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) != 2 || !instanceNamePattern.MatchString(parts[0]) {
		http.Error(w, `{"error":"invalid_instance_name"}`, http.StatusBadRequest)
		return
	}
	name, action := parts[0], parts[1]
	if action != "start" && action != "stop" && action != "restart" && action != "rotate" {
		http.NotFound(w, r)
		return
	}
	if action == "rotate" {
		result, err := s.RotateInstanceEgress(name)
		if err != nil {
			writeAPIError(w, http.StatusConflict, "egress_rotation_failed", err)
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	if err := s.docker.action(name, action); err != nil {
		http.Error(w, `{"error":"action_failed"}`, http.StatusBadGateway)
		return
	}
	if action == "start" || action == "restart" {
		s.mu.RLock()
		keys := s.gatewayKeysLocked()
		s.mu.RUnlock()
		payload, _ := json.Marshal(map[string]any{"keys": keys})
		instance := Instance{Name: name, URL: "http://" + name + ":13339"}
		var syncErr error
		for attempt := 0; attempt < 20; attempt++ {
			syncErr = s.call(instance, http.MethodPut, "/admin/keys", strings.NewReader(string(payload)), nil)
			if syncErr == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if syncErr != nil {
			http.Error(w, `{"error":"key_resync_failed"}`, http.StatusBadGateway)
			return
		}
	}
	if err := s.refreshInstances(); err != nil {
		http.Error(w, `{"error":"route_refresh_failed"}`, http.StatusBadGateway)
		return
	}
	if action == "start" || action == "restart" {
		go func() { _ = s.ReconcileEgresses() }()
	}
	s.addRotationLog("info", "instance action completed", name, map[string]any{"action": action})
	writeJSON(w, http.StatusOK, map[string]string{"name": name, "action": action})
}

func (s *Server) instanceDelete(w http.ResponseWriter, name string) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if !instanceNamePattern.MatchString(name) {
		http.Error(w, `{"error":"invalid_instance_name"}`, http.StatusBadRequest)
		return
	}
	if err := s.docker.remove(name); err != nil {
		http.Error(w, `{"error":"delete_failed"}`, http.StatusBadGateway)
		return
	}
	_ = s.docker.removeDataVolume(name)
	s.mu.Lock()
	delete(s.upstreamKeys, name)
	s.persistUpstreamKeysLocked()
	s.mu.Unlock()
	if err := s.refreshInstances(); err != nil {
		http.Error(w, `{"error":"route_refresh_failed"}`, http.StatusBadGateway)
		return
	}
	s.addRotationLog("info", "instance deleted", name, nil)
	writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
}

func (s *Server) overview(w http.ResponseWriter) {
	instances, err := s.discoverInstances()
	if err == nil {
		s.mu.Lock()
		s.instances = instances
		s.persistInstancesLocked()
		s.mu.Unlock()
	} else {
		s.mu.RLock()
		instances = append([]Instance(nil), s.instances...)
		s.mu.RUnlock()
	}
	var wg sync.WaitGroup
	summaries := make([]Summary, len(instances))
	audits := make([][]AuditRecord, len(instances))
	logs := make([][]SystemLog, len(instances))
	containerLogs := make([][]SystemLog, len(instances))
	for i, instance := range instances {
		wg.Add(1)
		go func() {
			defer wg.Done()
			summaries[i] = s.summary(instance)
			if instance.Status != "" && instance.Status != "running" {
				return
			}
			audits[i] = s.audit(instance)
			logs[i] = s.logs(instance)
			containerLogs[i], _ = s.docker.logs(instance.Container, instance.Name, 100)
		}()
	}
	wg.Wait()
	for index := range audits {
		audits[index] = collapseAuditRecords(audits[index])
	}
	pooled := make(map[string]struct{})
	for _, instance := range selectTrafficPool(instances, s.cfg.DirectFallback) {
		pooled[instance.Name] = struct{}{}
	}
	for i := range summaries {
		_, summaries[i].InTrafficPool = pooled[summaries[i].Instance]
	}
	totals := map[string]uint64{}
	totalConcurrency := 0
	var records []AuditRecord
	var systemLogs []SystemLog
	for _, sum := range summaries {
		if sum.Online && sum.InTrafficPool {
			totalConcurrency += sum.MaxConcurrency
			for k, v := range sum.Stats {
				totals[k] += v
			}
		}
	}
	for _, list := range audits {
		records = append(records, list...)
	}
	for _, list := range logs {
		systemLogs = append(systemLogs, list...)
	}
	s.mu.RLock()
	controlLogs := append([]SystemLog(nil), s.rotationLogs...)
	s.mu.RUnlock()
	systemLogs = append(systemLogs, controlLogs...)
	sort.Slice(systemLogs, func(i, j int) bool { return systemLogs[i].At.Before(systemLogs[j].At) })
	if len(systemLogs) > 500 {
		systemLogs = systemLogs[len(systemLogs)-500:]
	}
	mihomoLogs, _ := s.docker.logs(s.cfg.MihomoContainer, "mihomo", 200)
	logSources := buildLogSources(instances, audits, logs, containerLogs, controlLogs, mihomoLogs)
	sort.Slice(records, func(i, j int) bool { return records[i].At.Before(records[j].At) })
	if len(records) > 500 {
		records = records[len(records)-500:]
	}
	writeJSON(w, 200, map[string]any{"instances": summaries, "stats": totals, "max_concurrency": totalConcurrency, "records": records, "logs": systemLogs, "log_sources": logSources})
}

func (s *Server) tokens(w http.ResponseWriter, r *http.Request) {
	instances, err := s.discoverInstances()
	if err == nil {
		s.mu.Lock()
		s.instances = instances
		s.persistInstancesLocked()
		s.mu.Unlock()
	} else {
		s.mu.RLock()
		instances = append([]Instance(nil), s.instances...)
		s.mu.RUnlock()
	}

	audits := make([][]AuditRecord, len(instances))
	var wg sync.WaitGroup
	for index, instance := range instances {
		if instance.Status != "" && instance.Status != "running" {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			audits[index] = s.audit(instance)
		}()
	}
	wg.Wait()
	for index := range audits {
		audits[index] = collapseAuditRecords(audits[index])
	}

	query := r.URL.Query()
	instanceFilter := strings.TrimSpace(query.Get("instance"))
	modelFilter := strings.TrimSpace(query.Get("model"))
	keyFilter := strings.TrimSpace(query.Get("key"))
	statusFilter := strings.TrimSpace(query.Get("status"))
	pathFilter := strings.TrimSpace(query.Get("path"))
	instanceProviders := make(map[string]string, len(instances))
	for _, instance := range instances {
		instanceProviders[instance.Name] = providerOrDefault(instance.Provider)
	}
	var records []AuditRecord
	summary := struct {
		Requests         int64   `json:"requests"`
		Success          int64   `json:"success"`
		Errors           int64   `json:"errors"`
		PromptTokens     int64   `json:"prompt_tokens"`
		CompletionTokens int64   `json:"completion_tokens"`
		TotalTokens      int64   `json:"total_tokens"`
		CachedTokens     int64   `json:"cached_tokens"`
		InputCostUSD     float64 `json:"input_cost_usd"`
		OutputCostUSD    float64 `json:"output_cost_usd"`
		CacheCostUSD     float64 `json:"cache_cost_usd"`
		TotalCostUSD     float64 `json:"total_cost_usd"`
	}{}
	for _, list := range audits {
		for _, record := range list {
			record.ClientModel = clientModelForAuditRecord(record, instanceProviders)
			if instanceFilter != "" && record.Instance != instanceFilter || modelFilter != "" && record.ClientModel != modelFilter || keyFilter != "" && record.ClientKey != keyFilter || statusFilter != "" && strconv.Itoa(record.Status) != statusFilter || pathFilter != "" && record.Path != pathFilter {
				continue
			}
			inputCost, outputCost, cacheCost := tokenCostsUSD(record.Model, record.PromptTokens, record.CompletionTokens, record.CachedTokens)
			record.InputCostUSD = inputCost
			record.OutputCostUSD = outputCost
			record.CacheCostUSD = cacheCost
			record.TotalCostUSD = inputCost + outputCost + cacheCost
			records = append(records, record)
			summary.Requests++
			if record.Status >= 200 && record.Status < 400 {
				summary.Success++
			} else {
				summary.Errors++
			}
			summary.PromptTokens += record.PromptTokens
			summary.CompletionTokens += record.CompletionTokens
			summary.TotalTokens += record.TotalTokens
			summary.CachedTokens += record.CachedTokens
			summary.InputCostUSD += record.InputCostUSD
			summary.OutputCostUSD += record.OutputCostUSD
			summary.CacheCostUSD += record.CacheCostUSD
			summary.TotalCostUSD += record.TotalCostUSD
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].At.After(records[j].At) })
	if len(records) > 500 {
		records = records[:500]
	}
	writeJSON(w, http.StatusOK, map[string]any{"summary": summary, "records": records})
}

func clientModelForAuditRecord(record AuditRecord, instanceProviders map[string]string) string {
	model := strings.TrimSpace(record.Model)
	if model == "" {
		return ""
	}
	provider, ok := instanceProviders[record.Instance]
	if !ok || provider == "" {
		return model
	}
	return clientModelFor(provider, model)
}

const tokensPerMillion = 1_000_000.0

func tokenCostsUSD(model string, promptTokens, completionTokens, cachedTokens int64) (input, output, cache float64) {
	if strings.TrimSuffix(strings.ToLower(strings.TrimSpace(model)), "-free") != "deepseek-v4-flash" {
		return 0, 0, 0
	}
	if cachedTokens < 0 {
		cachedTokens = 0
	}
	if cachedTokens > promptTokens {
		cachedTokens = promptTokens
	}
	nonCachedPrompt := promptTokens - cachedTokens
	return float64(nonCachedPrompt) * 0.14 / tokensPerMillion, float64(completionTokens) * 0.28 / tokensPerMillion, float64(cachedTokens) * 0.0028 / tokensPerMillion
}

func collapseAuditRecords(records []AuditRecord) []AuditRecord {
	if len(records) < 2 {
		return records
	}
	type group struct {
		key     string
		records []AuditRecord
	}
	groups := make([]group, 0, len(records))
	indexes := make(map[string]int, len(records))
	for index, record := range records {
		key := strings.TrimSpace(record.RequestID)
		if key == "" {
			key = fmt.Sprintf("legacy:%d", index)
		}
		groupIndex, exists := indexes[key]
		if !exists {
			groupIndex = len(groups)
			indexes[key] = groupIndex
			groups = append(groups, group{key: key})
		}
		groups[groupIndex].records = append(groups[groupIndex].records, record)
	}

	result := make([]AuditRecord, 0, len(groups))
	for _, current := range groups {
		attempts := current.records
		sort.SliceStable(attempts, func(i, j int) bool {
			if attempts[i].Attempts != attempts[j].Attempts {
				return attempts[i].Attempts < attempts[j].Attempts
			}
			return attempts[i].At.Before(attempts[j].At)
		})
		final := attempts[len(attempts)-1]
		earliestStart := final.At.Add(-time.Duration(final.LatencyMS) * time.Millisecond)
		hadFailure := false
		maxAttempt := final.Attempts
		history := make([]AuditAttempt, 0, len(attempts))
		for _, attempt := range attempts {
			started := attempt.At.Add(-time.Duration(attempt.LatencyMS) * time.Millisecond)
			if started.Before(earliestStart) {
				earliestStart = started
			}
			if attempt.Status < 200 || attempt.Status >= 400 {
				hadFailure = true
			}
			if attempt.Attempts > maxAttempt {
				maxAttempt = attempt.Attempts
			}
			history = append(history, AuditAttempt{At: attempt.At, Status: attempt.Status, Egress: attempt.Egress, Source: attempt.Source, Attempt: attempt.Attempts, LatencyMS: attempt.LatencyMS, RetryAfter: attempt.RetryAfter})
		}
		final.Attempts = maxAttempt
		final.LatencyMS = final.At.Sub(earliestStart).Milliseconds()
		final.Recovered = final.Status >= 200 && final.Status < 400 && hadFailure
		if len(history) > 1 {
			final.AttemptHistory = history
		}
		result = append(result, final)
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].At.Before(result[j].At) })
	return result
}

func buildLogSources(instances []Instance, audits [][]AuditRecord, gatewayLogs, containerLogs [][]SystemLog, controlLogs, mihomoLogs []SystemLog) []LogSource {
	sources := []LogSource{
		{ID: "control", Label: "控制面板", Entries: systemLogEntries(controlLogs, "control")},
		{ID: "mihomo", Label: "Mihomo", Entries: systemLogEntries(mihomoLogs, "container")},
	}
	for index, instance := range instances {
		entries := make([]map[string]any, 0, len(audits[index])+len(gatewayLogs[index])+len(containerLogs[index]))
		for _, record := range audits[index] {
			entries = append(entries, map[string]any{
				"kind": "audit", "at": record.At, "status": record.Status, "method": record.Method,
				"path": record.Path, "model": record.Model, "egress": record.Egress, "source": record.Source,
				"attempts": record.Attempts, "latency_ms": record.LatencyMS, "message": record.Path,
				"request_id": record.RequestID, "recovered": record.Recovered, "attempt_history": record.AttemptHistory,
			})
		}
		entries = append(entries, systemLogEntries(gatewayLogs[index], "gateway")...)
		entries = append(entries, systemLogEntries(containerLogs[index], "container")...)
		sort.Slice(entries, func(i, j int) bool { return logEntryTime(entries[i]).Before(logEntryTime(entries[j])) })
		if len(entries) > 300 {
			entries = entries[len(entries)-300:]
		}
		sources = append(sources, LogSource{ID: instance.Name, Label: instance.Name, Entries: entries})
	}
	return sources
}

func systemLogEntries(logs []SystemLog, kind string) []map[string]any {
	entries := make([]map[string]any, 0, len(logs))
	for _, log := range logs {
		entries = append(entries, map[string]any{"kind": kind, "at": log.At, "level": log.Level, "message": log.Message, "fields": log.Fields})
	}
	return entries
}

func logEntryTime(entry map[string]any) time.Time {
	switch value := entry["at"].(type) {
	case time.Time:
		return value
	case string:
		parsed, _ := time.Parse(time.RFC3339Nano, value)
		return parsed
	default:
		return time.Time{}
	}
}
func (s *Server) summary(instance Instance) Summary {
	var out Summary
	out.Instance = instance.Name
	out.InstanceURL = instance.URL
	out.Status = instance.Status
	out.Managed = instance.Managed
	out.Container = instance.Container
	out.ContainerID = instance.ContainerID
	s.mu.RLock()
	upstreamKey := s.upstreamKeys[instance.Name]
	out.UpstreamKeyID = instance.UpstreamKeyID
	out.UpstreamKeyIDs = append([]string(nil), instance.UpstreamKeyIDs...)
	keyIDs := instance.UpstreamKeyIDs
	if len(keyIDs) == 0 && instance.UpstreamKeyID != "" {
		keyIDs = []string{instance.UpstreamKeyID}
	}
	labels := make([]string, 0, len(keyIDs))
	for _, record := range s.providerKeys {
		for _, id := range keyIDs {
			if record.ID == id {
				if upstreamKey == "" {
					upstreamKey = record.Secret
				}
				labels = append(labels, record.Label)
			}
		}
	}
	out.UpstreamKeyLabel = strings.Join(labels, " + ")
	out.UpstreamKeyMasked = maskAPIKey(upstreamKey)
	out.UpstreamKeySet = upstreamKey != ""
	out.AuthMode = providerAuthModeForKey(upstreamKey)
	if out.AuthMode == providerAuthModePublic {
		out.UpstreamKeyMasked = "public（公共 Key）"
	}
	s.mu.RUnlock()
	out.MaxConcurrency = instance.MaxConcurrency
	out.ProxyURLs = append([]string(nil), instance.ProxyURLs...)
	out.QueueSize = instance.QueueSize
	out.Provider = instance.Provider
	if instance.Status != "" && instance.Status != "running" {
		return out
	}
	if err := s.call(instance, http.MethodGet, "/admin/summary", nil, &out); err != nil {
		out.Error = err.Error()
		return out
	}
	out.Online = true
	for _, slot := range out.Slots {
		slot["instance"] = instance.Name
		if groupName, ok := mihomoGroupForProxyURL(fmt.Sprint(slot["url"])); ok {
			slot["mihomo_group"] = groupName
			if group, err := s.getMihomoProxyGroup(groupName); err == nil {
				slot["mihomo_node"] = group.Now
			}
		}
	}
	if instance.Provider == ProviderFreeBuff {
		s.annotateFreeBuffEgress(out.Slots)
	}
	return out
}
func (s *Server) audit(instance Instance) []AuditRecord {
	var out struct {
		Records []AuditRecord `json:"records"`
	}
	if s.call(instance, "GET", "/admin/audit", nil, &out) != nil {
		return nil
	}
	for i := range out.Records {
		out.Records[i].Instance = instance.Name
	}
	return out.Records
}
func (s *Server) logs(instance Instance) []SystemLog {
	var out struct {
		Logs []SystemLog `json:"logs"`
	}
	if s.call(instance, "GET", "/admin/logs", nil, &out) != nil {
		return nil
	}
	for i := range out.Logs {
		out.Logs[i].Instance = instance.Name
	}
	return out.Logs
}
func (s *Server) call(instance Instance, method, path string, body io.Reader, out any) error {
	req, err := http.NewRequest(method, instance.URL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.InstanceToken)
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
func (s *Server) createKey(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&request); err != nil {
		http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
		return
	}
	key := strings.TrimSpace(request.Key)
	if key == "" {
		buf := make([]byte, 24)
		if _, err := rand.Read(buf); err != nil {
			http.Error(w, `{"error":"key_generation_failed"}`, http.StatusInternalServerError)
			return
		}
		key = "gw_" + hex.EncodeToString(buf)
	}
	if len(key) > 512 || strings.IndexFunc(key, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		http.Error(w, `{"error":"invalid_gateway_key"}`, http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	if s.hasGatewayKeyLocked(key) {
		s.mu.Unlock()
		http.Error(w, `{"error":"gateway_key_exists"}`, http.StatusConflict)
		return
	}
	s.keys = append(s.keys, key)
	result := s.syncKeysLocked()
	s.persistLocked()
	s.mu.Unlock()
	writeJSON(w, result.Status, map[string]any{"key": key, "sync": result})
}
func (s *Server) deleteKey(w http.ResponseWriter, key string) {
	s.mu.Lock()
	next := s.keys[:0]
	for _, item := range s.keys {
		if item != key {
			next = append(next, item)
		}
	}
	s.keys = next
	result := s.syncKeysLocked()
	s.persistLocked()
	s.mu.Unlock()
	writeJSON(w, result.Status, map[string]any{"deleted": true, "sync": result})
}

type syncResult struct {
	Status  int               `json:"-"`
	Updated []string          `json:"updated"`
	Failed  map[string]string `json:"failed"`
}

func (s *Server) syncKeysLocked() syncResult {
	result := syncResult{Status: 200, Failed: map[string]string{}}
	instances, err := s.discoverInstances()
	if err != nil {
		instances = append([]Instance(nil), s.instances...)
	}
	for _, instance := range instances {
		if instance.Status != "" && instance.Status != "running" {
			continue
		}
		payload, _ := json.Marshal(map[string]any{"keys": s.gatewayKeysLocked()})
		if err := s.call(instance, "PUT", "/admin/keys", strings.NewReader(string(payload)), nil); err != nil {
			result.Failed[instance.Name] = err.Error()
		} else {
			result.Updated = append(result.Updated, instance.Name)
		}
	}
	if len(result.Failed) > 0 {
		result.Status = 207
	}
	return result
}
func (s *Server) broadcast(w http.ResponseWriter, r *http.Request, path string) {
	data, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	result := syncResult{Status: 200, Failed: map[string]string{}}
	for _, instance := range s.currentInstances() {
		if instance.Status != "" && instance.Status != "running" {
			continue
		}
		if err := s.call(instance, "POST", path, strings.NewReader(string(data)), nil); err != nil {
			result.Failed[instance.Name] = err.Error()
		} else {
			result.Updated = append(result.Updated, instance.Name)
		}
	}
	if len(result.Failed) > 0 {
		result.Status = 207
	}
	writeJSON(w, result.Status, result)
}
func (s *Server) currentInstances() []Instance {
	instances, err := s.discoverInstances()
	if err == nil {
		return instances
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Instance(nil), s.instances...)
}
func (s *Server) loadKeys() {
	data, err := os.ReadFile(s.cfg.DataDir + "/keys.json")
	if err != nil || len(data) == 0 {
		return
	}
	var records []GatewayKey
	if json.Unmarshal(data, &records) == nil {
		for _, record := range records {
			if record.Key == "" {
				continue
			}
			s.keys = append(s.keys, record.Key)
		}
		return
	}
	var legacy []string
	if json.Unmarshal(data, &legacy) == nil {
		for _, key := range legacy {
			if key == "" {
				continue
			}
			s.keys = append(s.keys, key)
		}
	}
}

func (s *Server) loadInstances() {
	data, err := os.ReadFile(s.cfg.DataDir + "/instances.json")
	if err == nil {
		_ = json.Unmarshal(data, &s.instances)
	}
}
func (s *Server) loadUpstreamKeys() {
	data, err := os.ReadFile(s.cfg.DataDir + "/upstream-keys.json")
	if err != nil {
		// Migrate a previous provider-key filename once so existing instances keep working.
		if entries, readErr := os.ReadDir(s.cfg.DataDir); readErr == nil {
			for _, entry := range entries {
				name := entry.Name()
				if entry.IsDir() || name == "upstream-keys.json" || !strings.HasSuffix(name, "-keys.json") {
					continue
				}
				data, err = os.ReadFile(s.cfg.DataDir + "/" + name)
				if err == nil {
					break
				}
			}
		}
	}
	if err == nil {
		_ = json.Unmarshal(data, &s.upstreamKeys)
	}
	if s.upstreamKeys == nil {
		s.upstreamKeys = make(map[string]string)
	}
	if len(s.upstreamKeys) > 0 {
		s.persistUpstreamKeysLocked()
	}
}
func (s *Server) persistUpstreamKeysLocked() {
	data, _ := json.MarshalIndent(s.upstreamKeys, "", "  ")
	if err := writePrivateFileAtomic(s.cfg.DataDir+"/upstream-keys.json", data); err != nil {
		s.addPersistenceLog("instance upstream keys", err)
	}
}
func (s *Server) persistInstancesLocked() {
	data, _ := json.MarshalIndent(s.instances, "", "  ")
	if err := writePrivateFileAtomic(s.cfg.DataDir+"/instances.json", data); err != nil {
		s.addPersistenceLog("instances", err)
	}
}
func (s *Server) persistLocked() {
	data, _ := json.MarshalIndent(s.keys, "", "  ")
	if err := writePrivateFileAtomic(s.cfg.DataDir+"/keys.json", data); err != nil {
		s.addPersistenceLog("gateway keys", err)
	}
}

func (s *Server) addPersistenceLog(kind string, err error) {
	// Persistence is called while different state locks are held, so use the
	// process logger instead of taking another server lock for audit logging.
	slog.Error("control-plane persistence failed", "data", kind, "error", err)
}
func bearer(r *http.Request, expected string) bool {
	got := requestBearer(r)
	return len(got) == len(expected) && subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}
func requestBearer(r *http.Request) string {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(header) < 7 || !strings.EqualFold(header[:7], "Bearer ") {
		return ""
	}
	return strings.TrimSpace(header[7:])
}

func requestCredential(r *http.Request) string {
	if bearer := requestBearer(r); bearer != "" {
		return bearer
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}
func containsKey(keys []string, expected string) bool {
	for _, key := range keys {
		if len(key) == len(expected) && subtle.ConstantTimeCompare([]byte(key), []byte(expected)) == 1 {
			return true
		}
	}
	return false
}

func (s *Server) hasGatewayKeyLocked(key string) bool {
	for _, known := range s.keys {
		if len(known) == len(key) && subtle.ConstantTimeCompare([]byte(known), []byte(key)) == 1 {
			return true
		}
	}
	return false
}

func (s *Server) gatewayKeysLocked() []string {
	return append([]string(nil), s.keys...)
}

func instancesForProvider(instances []Instance, provider string) []Instance {
	provider = providerOrDefault(provider)
	matched := make([]Instance, 0, len(instances))
	for _, instance := range instances {
		if providerOrDefault(instance.Provider) == provider {
			matched = append(matched, instance)
		}
	}
	return matched
}
func maskAPIKey(key string) string {
	if len(key) <= 8 {
		return strings.Repeat("*", len(key))
	}
	return key[:4] + strings.Repeat("*", len(key)-8) + key[len(key)-4:]
}
func providerAuthModeForKey(key string) string {
	if strings.TrimSpace(key) == providerAuthModePublic {
		return providerAuthModePublic
	}
	if strings.TrimSpace(key) != "" {
		return providerAuthModeCustom
	}
	return ""
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
func writeAPIError(w http.ResponseWriter, status int, code string, err error) {
	writeJSON(w, status, map[string]string{"error": code, "detail": err.Error()})
}
func env(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}
func boolEnv(name string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
func compatibleBoolEnv(primary, legacy string, fallback bool) bool {
	if strings.TrimSpace(os.Getenv(primary)) != "" {
		return boolEnv(primary, fallback)
	}
	return boolEnv(legacy, fallback)
}
func split(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
