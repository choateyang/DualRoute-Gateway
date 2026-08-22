package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	ProviderTokenRouter = "tokenrouter"
	ProviderOpenCode    = "opencode"
	ProviderCline       = "cline"
	ProviderFreeBuff    = "freebuff"
	ProviderVertex      = "vertex"

	tokenRouterURL         = "https://api.tokenrouter.com/v1"
	openCodeZenURL         = "https://opencode.ai/zen"
	clineAPIURL            = "https://api.cline.bot/api/v1"
	freeBuffAPIURL         = "https://www.codebuff.com"
	vertexProxyURL         = "internal://vertex"
	tokenRouterModel       = "deepseek/deepseek-v4-pro-0813-free"
	openCodeModel          = "deepseek-v4-flash-free"
	tokenRouterClientModel = "TokenRouter/deepseek-v4-pro"
	openCodeClientModel    = "OpenCode/deepseek-v4-flash"
	clineClientModel       = "cline/deepseek-v4-flash"
	clineUpstreamModel     = "deepseek/deepseek-v4-flash"
	clineMaxOutputTokens   = 4096

	opencodeVersion   = "1.18.16"
	opencodeUserAgent = "opencode/" + opencodeVersion
	opencodeClient    = "cli"
	opencodeReferer   = "https://opencode.ai/"
	opencodeTitle     = "opencode"
)

// Fallback list used until/unless dynamic discovery covers a model. Keep in
// sync with the keyless free tier of https://opencode.ai/zen/v1/models.
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

type Config struct {
	ListenAddr               string
	UpstreamProvider         string
	UpstreamURL              string
	UpstreamAPIKey           string
	UpstreamAPIKeys          []string
	FreeBuffModelsURLs       []string
	ClineTaskID              string
	ForcedModel              string
	GatewayKeys              []string
	ProxyURLs                []string
	ProxyListFile            string
	ProxyRefresh             time.Duration
	ProxyProbeURLs           []string
	ProxyProbeWait           time.Duration
	ProxyProbeJobs           int
	TargetEgress             int
	DirectEnabled            bool
	MaxConcurrency           int
	QueueSize                int
	MaxRetries               int
	RequestTimeout           time.Duration
	StreamFirstOutputTimeout time.Duration
	StreamFailureCooldown    time.Duration
	CooldownBase             time.Duration
	CooldownMax              time.Duration
	FreeModelsOnly           bool
	DisableThinkingByDefault bool
	MinThinkingMaxTokens     int
	IsolateUpstreamState     bool
	AdminToken               string
	InstanceAdminToken       string
	DataDir                  string
}

func DefaultConfig() Config {
	return Config{
		ListenAddr: "0.0.0.0:13339", UpstreamProvider: ProviderTokenRouter, UpstreamURL: tokenRouterURL, ForcedModel: tokenRouterModel,
		MaxConcurrency: 4, QueueSize: 16, MaxRetries: 2,
		RequestTimeout: 5 * time.Minute, StreamFirstOutputTimeout: 20 * time.Second, StreamFailureCooldown: 10 * time.Minute, CooldownBase: 5 * time.Second,
		CooldownMax: 60 * time.Second, ProxyRefresh: 30 * time.Second,
		ProxyProbeURLs: []string{"https://api.ipify.org", "https://ifconfig.me/ip", "https://www.cloudflare.com/cdn-cgi/trace"}, ProxyProbeWait: 10 * time.Second,
		ProxyProbeJobs: 8, DirectEnabled: true,
		FreeModelsOnly: false, DisableThinkingByDefault: false, MinThinkingMaxTokens: 0, IsolateUpstreamState: true,
		FreeBuffModelsURLs: []string{freeBuffModelsURL},
		DataDir:            "/data",
	}
}

func LoadConfig() (Config, error) {
	c := DefaultConfig()
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		c.ListenAddr = v
	}
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("UPSTREAM_PROVIDER"))); v != "" {
		c.UpstreamProvider = v
	}
	if err := applyProviderDefaults(&c); err != nil {
		return c, err
	}
	if v := os.Getenv("UPSTREAM_URL"); v != "" {
		c.UpstreamURL = strings.TrimRight(v, "/")
	}
	c.UpstreamAPIKey = strings.TrimSpace(os.Getenv("UPSTREAM_API_KEY"))
	if v := os.Getenv("FREEBUFF_MODELS_URLS"); v != "" {
		c.FreeBuffModelsURLs = split(v)
	}
	if len(c.FreeBuffModelsURLs) == 0 {
		c.FreeBuffModelsURLs = []string{freeBuffModelsURL}
	}
	for _, raw := range c.FreeBuffModelsURLs {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "https" || u.Host == "" {
			return c, fmt.Errorf("FREEBUFF_MODELS_URLS must contain absolute HTTPS URLs")
		}
	}
	if c.UpstreamProvider == ProviderFreeBuff {
		c.UpstreamAPIKeys = split(c.UpstreamAPIKey)
		if len(c.UpstreamAPIKeys) > 0 {
			c.UpstreamAPIKey = c.UpstreamAPIKeys[0]
		}
	}
	c.ClineTaskID = strings.TrimSpace(os.Getenv("CLINE_TASK_ID"))
	if v := os.Getenv("GATEWAY_KEYS"); v != "" {
		c.GatewayKeys = split(v)
	}
	if v := os.Getenv("PROXY_URLS"); v != "" {
		c.ProxyURLs = split(v)
	}
	c.ProxyListFile = strings.TrimSpace(os.Getenv("PROXY_LIST_FILE"))
	if v := os.Getenv("PROXY_REFRESH_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("PROXY_REFRESH_INTERVAL: %w", err)
		}
		c.ProxyRefresh = d
	}
	if v := os.Getenv("PROXY_PROBE_URL"); v != "" {
		c.ProxyProbeURLs = split(v)
	}
	if v := os.Getenv("PROXY_PROBE_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("PROXY_PROBE_TIMEOUT: %w", err)
		}
		c.ProxyProbeWait = d
	}
	if v := os.Getenv("PROXY_PROBE_CONCURRENCY"); v != "" {
		c.ProxyProbeJobs = positiveInt(v, c.ProxyProbeJobs)
	}
	if v := os.Getenv("TARGET_EGRESS_SLOTS"); v != "" {
		c.TargetEgress = nonNegativeInt(v, c.TargetEgress)
	}
	if v := os.Getenv("DIRECT_ENABLED"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return c, fmt.Errorf("DIRECT_ENABLED: %w", err)
		}
		c.DirectEnabled = b
	}
	if v := os.Getenv("MAX_CONCURRENCY"); v != "" {
		c.MaxConcurrency = positiveInt(v, c.MaxConcurrency)
	}
	if v := os.Getenv("QUEUE_SIZE"); v != "" {
		c.QueueSize = nonNegativeInt(v, c.QueueSize)
	}
	if v := os.Getenv("MAX_RETRIES"); v != "" {
		c.MaxRetries = nonNegativeInt(v, c.MaxRetries)
	}
	if v := os.Getenv("REQUEST_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("REQUEST_TIMEOUT: %w", err)
		}
		c.RequestTimeout = d
	}
	if v := os.Getenv("STREAM_FIRST_OUTPUT_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return c, fmt.Errorf("STREAM_FIRST_OUTPUT_TIMEOUT must be a non-negative duration")
		}
		c.StreamFirstOutputTimeout = d
	}
	if v := os.Getenv("STREAM_FAILURE_COOLDOWN"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return c, fmt.Errorf("STREAM_FAILURE_COOLDOWN must be a non-negative duration")
		}
		c.StreamFailureCooldown = d
	}
	if v := os.Getenv("COOLDOWN_BASE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("COOLDOWN_BASE: %w", err)
		}
		c.CooldownBase = d
	}
	if v := os.Getenv("COOLDOWN_MAX"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("COOLDOWN_MAX: %w", err)
		}
		c.CooldownMax = d
	}
	if v := os.Getenv("FREE_MODELS_ONLY"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return c, fmt.Errorf("FREE_MODELS_ONLY: %w", err)
		}
		c.FreeModelsOnly = b
	}
	if v := os.Getenv("DISABLE_THINKING_BY_DEFAULT"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return c, fmt.Errorf("DISABLE_THINKING_BY_DEFAULT: %w", err)
		}
		c.DisableThinkingByDefault = b
	}
	if v := os.Getenv("MIN_THINKING_MAX_TOKENS"); v != "" {
		c.MinThinkingMaxTokens = nonNegativeInt(v, c.MinThinkingMaxTokens)
	}
	if v := os.Getenv("ISOLATE_UPSTREAM_STATE"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return c, fmt.Errorf("ISOLATE_UPSTREAM_STATE: %w", err)
		}
		c.IsolateUpstreamState = b
	}
	c.AdminToken = os.Getenv("ADMIN_TOKEN")
	c.InstanceAdminToken = os.Getenv("INSTANCE_ADMIN_TOKEN")
	if c.InstanceAdminToken == "" {
		c.InstanceAdminToken = c.AdminToken
	}
	if v := strings.TrimSpace(os.Getenv("DATA_DIR")); v != "" {
		c.DataDir = v
	}
	if c.UpstreamURL == "" {
		return c, fmt.Errorf("UPSTREAM_URL is required")
	}
	if u, err := url.Parse(c.UpstreamURL); err != nil || u.Scheme == "" || u.Host == "" {
		return c, fmt.Errorf("UPSTREAM_URL must be an absolute URL")
	}
	if c.UpstreamAPIKey == "" {
		return c, fmt.Errorf("UPSTREAM_API_KEY is required")
	}
	if c.UpstreamProvider == ProviderCline && c.ClineTaskID == "" {
		c.ClineTaskID = newUUID()
	}
	keys := c.UpstreamAPIKeys
	if len(keys) == 0 {
		keys = []string{c.UpstreamAPIKey}
	}
	for _, key := range keys {
		if len(key) > 512 || strings.IndexFunc(key, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
			return c, fmt.Errorf("UPSTREAM_API_KEY is invalid")
		}
	}
	if c.MaxConcurrency < 1 || c.QueueSize < 0 || c.MaxRetries < 0 || c.MinThinkingMaxTokens < 0 || c.CooldownBase <= 0 || c.CooldownMax < c.CooldownBase {
		return c, fmt.Errorf("invalid concurrency/retry/cooldown settings")
	}
	if c.ProxyRefresh <= 0 || c.ProxyProbeWait <= 0 || c.ProxyProbeJobs <= 0 {
		return c, fmt.Errorf("proxy refresh/probe durations must be positive")
	}
	if len(c.ProxyProbeURLs) == 0 {
		return c, fmt.Errorf("PROXY_PROBE_URL must contain at least one URL")
	}
	for _, raw := range c.ProxyProbeURLs {
		if u, err := url.Parse(raw); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return c, fmt.Errorf("PROXY_PROBE_URL must contain absolute HTTP(S) URLs")
		}
	}
	if !c.DirectEnabled && len(c.ProxyURLs) == 0 && c.ProxyListFile == "" {
		return c, fmt.Errorf("at least one proxy source is required when DIRECT_ENABLED=false")
	}
	return c, nil
}

func applyProviderDefaults(c *Config) error {
	switch c.UpstreamProvider {
	case ProviderTokenRouter:
		c.UpstreamURL = tokenRouterURL
		c.ForcedModel = tokenRouterModel
		c.FreeModelsOnly = false
		c.DisableThinkingByDefault = false
		c.MinThinkingMaxTokens = 0
	case ProviderOpenCode:
		c.UpstreamURL = openCodeZenURL
		c.ForcedModel = ""
		c.FreeModelsOnly = true
		c.DisableThinkingByDefault = true
		c.MinThinkingMaxTokens = 8192
	case ProviderCline:
		c.UpstreamURL = clineAPIURL
		c.ForcedModel = ""
		c.FreeModelsOnly = false
		c.DisableThinkingByDefault = false
		c.MinThinkingMaxTokens = 0
	case ProviderFreeBuff:
		c.UpstreamURL = freeBuffAPIURL
		c.ForcedModel = ""
		c.FreeModelsOnly = false
		c.DisableThinkingByDefault = false
		c.MinThinkingMaxTokens = 0
	case ProviderVertex:
		// Google is called directly by forwardVertex; no HTTP upstream URL.
		c.UpstreamURL = vertexProxyURL
		c.ForcedModel = ""
		c.FreeModelsOnly = false
		c.DisableThinkingByDefault = false
		c.MinThinkingMaxTokens = 0
	default:
		return fmt.Errorf("UPSTREAM_PROVIDER must be %q, %q, %q, %q, or %q", ProviderTokenRouter, ProviderOpenCode, ProviderCline, ProviderFreeBuff, ProviderVertex)
	}
	return nil
}

func openCodeUpstreamModel(model string) (string, bool) {
	model = strings.TrimSpace(model)
	for _, upstream := range openCodeModels {
		if model == upstream || model == "OpenCode/"+strings.TrimSuffix(upstream, "-free") {
			return upstream, true
		}
	}
	return "", false
}

func split(v string) []string {
	var out []string
	for _, item := range strings.Split(v, ",") {
		if s := strings.TrimSpace(item); s != "" {
			out = append(out, s)
		}
	}
	return out
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

func positiveInt(v string, fallback int) int {
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return fallback
	}
	return n
}
func nonNegativeInt(v string, fallback int) int {
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}
