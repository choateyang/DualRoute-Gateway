package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mathrand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type proxySlot struct {
	client         *http.Client
	url            string
	mu             sync.Mutex
	fails          int
	until          time.Time
	egress         string
	disabled       bool
	probeError     string
	modelCooldowns map[string]cooldownState
}

type cooldownState struct {
	Fails int
	Until time.Time
}

var (
	errNoUntriedSlot              = errors.New("no untried upstream slot")
	errUpstreamResponseRead       = errors.New("upstream response read")
	errUpstreamStreamEmpty        = errors.New("upstream stream ended before first output")
	errUpstreamFirstOutputTimeout = errors.New("upstream stream did not produce output before timeout")
	errUnsupportedResponsesInput  = errors.New("unsupported responses input")
)

const (
	maxModelCooldownsPerSlot = 256
	maxBufferedResponseBytes = 32 << 20
	instanceHealthyHeader    = "X-DualRoute-Instance-Healthy"
)

func (s *proxySlot) identity() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.egress != "" {
		return "egress:" + s.egress
	}
	if s.url != "" {
		return "proxy:" + s.url
	}
	return "direct"
}

func (s *proxySlot) available(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !now.Before(s.until)
}

func nextCooldown(fails int, base, max, minimum time.Duration) (int, time.Time, time.Duration) {
	fails++
	d := base
	for i := 1; i < fails && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	d += time.Duration(mathrand.Int64N(int64(d/5) + 1))
	if minimum > d {
		d = minimum
	}
	return fails, time.Now().Add(d), d
}

func (s *proxySlot) cooldown(base, max, minimum time.Duration) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	var d time.Duration
	s.fails, s.until, d = nextCooldown(s.fails, base, max, minimum)
	return d
}

func (s *proxySlot) cooldownModel(model string, base, max, minimum time.Duration) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.modelCooldowns == nil {
		s.modelCooldowns = make(map[string]cooldownState)
	}
	now := time.Now()
	var oldestModel string
	var oldestUntil time.Time
	for currentModel, current := range s.modelCooldowns {
		if !now.Before(current.Until) {
			delete(s.modelCooldowns, currentModel)
			continue
		}
		if oldestUntil.IsZero() || current.Until.Before(oldestUntil) {
			oldestModel, oldestUntil = currentModel, current.Until
		}
	}
	if _, exists := s.modelCooldowns[model]; !exists && len(s.modelCooldowns) >= maxModelCooldownsPerSlot {
		delete(s.modelCooldowns, oldestModel)
	}
	state := s.modelCooldowns[model]
	var d time.Duration
	state.Fails, state.Until, d = nextCooldown(state.Fails, base, max, minimum)
	s.modelCooldowns[model] = state
	return d
}

func (s *proxySlot) success(model string) {
	s.mu.Lock()
	s.fails = 0
	s.until = time.Time{}
	delete(s.modelCooldowns, model)
	s.mu.Unlock()
}

func (s *proxySlot) readiness(model string, now time.Time) (disabled bool, ready time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ready = s.until
	if state, exists := s.modelCooldowns[model]; exists {
		if now.Before(state.Until) && state.Until.After(ready) {
			ready = state.Until
		} else if !now.Before(state.Until) {
			delete(s.modelCooldowns, model)
		}
	}
	return s.disabled, ready
}

func (s *proxySlot) readyAt() time.Time { s.mu.Lock(); defer s.mu.Unlock(); return s.until }

type Stats struct {
	Requests    atomic.Uint64
	Success     atomic.Uint64
	Upstream429 atomic.Uint64
	Gateway429  atomic.Uint64
	Errors      atomic.Uint64
}
type Gateway struct {
	cfg                Config
	client             *http.Client
	// vertexExitRest 记录近期被上游 429 的出口及解禁时间（竞速跨请求记忆）。
	vertexExitRest     sync.Map
	// vertexExitHealth 记录各出口成功/失败计数，用于竞速候选排序（健康度学习）。
	vertexExitHealth   sync.Map
	slotsMu            sync.RWMutex
	slots              []*proxySlot
	base               []*proxySlot
	sem                chan struct{}
	queue              chan struct{}
	active             atomic.Uint64
	stats              Stats
	log                *slog.Logger
	keyMu              sync.RWMutex
	keys               map[string]struct{}
	auditMu            sync.RWMutex
	audits             []auditRecord
	logsMu             sync.RWMutex
	logs               []systemLog
	freeBuffAccounts   []*freeBuffAccount
	freeBuffAccountIdx atomic.Uint64
	freeBuffModelMu    sync.RWMutex
	freeBuffModels     map[string]freeBuffModelInfo
	freeBuffModelsAt   time.Time
}

type freeBuffAccount struct {
	token         string
	mu            sync.Mutex
	lastCall      time.Time
	cooldownUntil time.Time
	sessions      map[string]freeBuffSession
	runs          map[string]freeBuffRun
	behaviorAt    time.Time
}

type auditRecord struct {
	At               time.Time `json:"at"`
	RequestID        string    `json:"request_id,omitempty"`
	Method           string    `json:"method"`
	Path             string    `json:"path"`
	Model            string    `json:"model,omitempty"`
	Status           int       `json:"status"`
	Slot             string    `json:"slot"`
	Egress           string    `json:"egress,omitempty"`
	LatencyMS        int64     `json:"latency_ms"`
	Source           string    `json:"source"`
	Attempts         int       `json:"attempts"`
	RetryAfter       string    `json:"retry_after,omitempty"`
	ClientKey        string    `json:"client_key,omitempty"`
	Stream           bool      `json:"stream"`
	PromptTokens     int64     `json:"prompt_tokens,omitempty"`
	CompletionTokens int64     `json:"completion_tokens,omitempty"`
	TotalTokens      int64     `json:"total_tokens,omitempty"`
	CachedTokens     int64     `json:"cached_tokens,omitempty"`
	FirstTokenMS     int64     `json:"first_token_ms,omitempty"`
}

type tokenUsage struct {
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	CachedTokens     int64
}

type auditMetadata struct {
	ClientKey string
	RequestID string
	Stream    bool
}

type auditMetadataKey struct{}

type systemLog struct {
	At      time.Time      `json:"at"`
	Level   string         `json:"level"`
	Message string         `json:"message"`
	Fields  map[string]any `json:"fields,omitempty"`
}

func New(cfg Config, logger *slog.Logger) (*Gateway, error) {
	if cfg.UpstreamProvider == ProviderCline && strings.TrimSpace(cfg.ClineTaskID) == "" {
		cfg.ClineTaskID = newUUID()
	}
	g := &Gateway{cfg: cfg, client: &http.Client{Timeout: cfg.RequestTimeout}, sem: make(chan struct{}, cfg.MaxConcurrency), queue: make(chan struct{}, cfg.MaxConcurrency+cfg.QueueSize), log: logger, keys: make(map[string]struct{}), freeBuffModels: make(map[string]freeBuffModelInfo)}
	if cfg.UpstreamProvider == ProviderFreeBuff {
		accounts := cfg.UpstreamAPIKeys
		if len(accounts) == 0 && cfg.UpstreamAPIKey != "" {
			accounts = split(cfg.UpstreamAPIKey)
		}
		for _, token := range accounts {
			if token = strings.TrimSpace(token); token != "" {
				g.freeBuffAccounts = append(g.freeBuffAccounts, &freeBuffAccount{token: token, sessions: make(map[string]freeBuffSession), runs: make(map[string]freeBuffRun)})
			}
		}
	}
	for _, key := range cfg.GatewayKeys {
		g.keys[key] = struct{}{}
	}
	g.loadKeys()
	var directSlot *proxySlot
	if cfg.DirectEnabled {
		directSlot = &proxySlot{client: g.client}
		g.slots = append(g.slots, directSlot)
	}
	staticSlots, err := g.buildProxySlots(cfg.ProxyURLs)
	if err != nil {
		return nil, err
	}
	g.slots = append(g.slots, staticSlots...)
	if len(staticSlots) > 0 {
		for _, slot := range staticSlots {
			slot.disabled = true
		}
		// Do not make a pre-deduplicated static list available to a later
		// dynamic refresh. The initial probe is parallel internally.
		g.probeAndDeduplicateStaticSlots()
	} else if directSlot != nil && len(cfg.ProxyProbeURLs) > 0 {
		g.probeDirectSlot(directSlot)
	}
	g.base = append([]*proxySlot(nil), g.slots...)
	if cfg.ProxyListFile != "" {
		if err := g.refreshProxyList(); err != nil {
			g.log.Warn("initial dynamic proxy refresh failed", "error", err)
			g.addLog("warn", "initial dynamic proxy refresh failed", map[string]any{"error": err.Error()})
		}
		go g.proxyRefreshLoop()
	}
	if len(g.snapshotSlots()) == 0 {
		return nil, errors.New("no usable direct or proxy slots")
	}
	return g, nil
}

func (g *Gateway) probeDirectSlot(slot *proxySlot) {
	if slot == nil || slot.url != "" {
		return
	}
	ip, err := g.refreshSlotEgress(slot)
	if err != nil {
		g.addLog("warn", "direct egress probe failed", map[string]any{"error": err.Error()})
		return
	}
	_ = ip
}

func (g *Gateway) probeAndDeduplicateStaticSlots() {
	slots := g.snapshotSlots()
	type probeResult struct {
		slot *proxySlot
		ip   string
		err  error
	}
	results := make(chan probeResult, len(slots))
	jobs := make(chan *proxySlot)
	workerCount := min(g.cfg.ProxyProbeJobs, len(slots))
	var wg sync.WaitGroup
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for slot := range jobs {
				ip, err := g.probeSlot(slot)
				if err != nil {
					g.addLog("warn", "proxy egress probe failed", map[string]any{"proxy": safeProxyURL(slot.url), "error": err.Error()})
				}
				results <- probeResult{slot: slot, ip: ip, err: err}
			}
		}()
	}
	go func() {
		for _, slot := range slots {
			jobs <- slot
		}
		close(jobs)
	}()
	wg.Wait()
	close(results)
	probed := make(map[*proxySlot]string, len(slots))
	for result := range results {
		result.slot.mu.Lock()
		if result.err != nil {
			result.slot.disabled = true
			result.slot.probeError = result.err.Error()
			result.slot.mu.Unlock()
			continue
		}
		result.slot.egress = result.ip
		result.slot.disabled = false
		result.slot.probeError = ""
		result.slot.mu.Unlock()
		probed[result.slot] = result.ip
	}
	seen := make(map[string]int, len(slots))
	deduplicated := make([]*proxySlot, 0, len(slots))
	for _, slot := range slots {
		slot.mu.Lock()
		disabled, egress := slot.disabled, slot.egress
		slot.mu.Unlock()
		if disabled || egress == "" {
			deduplicated = append(deduplicated, slot)
			continue
		}
		key := slot.identity()
		if ip := probed[slot]; ip != "" {
			key = "egress:" + ip
		}
		if index, exists := seen[key]; exists {
			deduplicated[index].mu.Lock()
			if deduplicated[index].url == "" && slot.url != "" {
				deduplicated[index].disabled = true
				deduplicated[index].probeError = "duplicate egress"
				deduplicated[index].mu.Unlock()
				seen[key] = len(deduplicated)
				deduplicated = append(deduplicated, slot)
			} else {
				deduplicated[index].mu.Unlock()
				slot.mu.Lock()
				slot.disabled = true
				slot.probeError = "duplicate egress"
				slot.mu.Unlock()
				deduplicated = append(deduplicated, slot)
			}
			continue
		}
		seen[key] = len(deduplicated)
		deduplicated = append(deduplicated, slot)
	}
	g.slotsMu.Lock()
	g.slots = deduplicated
	g.slotsMu.Unlock()
}

func (g *Gateway) buildProxySlots(rawURLs []string) ([]*proxySlot, error) {
	var slots []*proxySlot
	for _, raw := range rawURLs {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5" && u.Scheme != "socks5h") {
			return nil, errors.New("proxy URL must use http, https, socks5, or socks5h: " + raw)
		}
		transport := &http.Transport{MaxIdleConns: 64, MaxIdleConnsPerHost: 16, IdleConnTimeout: 90 * time.Second}
		if u.Scheme == "http" || u.Scheme == "https" {
			transport.Proxy = http.ProxyURL(u)
		} else {
			username, password := "", ""
			if u.User != nil {
				username = u.User.Username()
				password, _ = u.User.Password()
			}
			proxyAddress := u.Host
			transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				return dialSOCKS5(ctx, network, proxyAddress, address, username, password)
			}
		}
		slots = append(slots, &proxySlot{client: &http.Client{Transport: transport, Timeout: g.cfg.RequestTimeout}, url: raw})
	}
	return slots, nil
}

func dialSOCKS5(ctx context.Context, network, proxyAddress, target, username, password string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("socks5 supports tcp only")
	}
	dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", proxyAddress)
	if err != nil {
		return nil, err
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = conn.Close()
		}
	}()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	methods := []byte{0x00}
	if username != "" || password != "" {
		methods = append(methods, 0x02)
	}
	if _, err = conn.Write(append([]byte{0x05, byte(len(methods))}, methods...)); err != nil {
		return nil, err
	}
	choice := make([]byte, 2)
	if _, err = io.ReadFull(conn, choice); err != nil {
		return nil, err
	}
	if choice[0] != 0x05 || choice[1] == 0xff {
		return nil, fmt.Errorf("socks5 proxy rejected authentication methods")
	}
	switch choice[1] {
	case 0x00:
	case 0x02:
		if len(username) > 255 || len(password) > 255 {
			return nil, fmt.Errorf("socks5 credentials too long")
		}
		auth := append([]byte{0x01, byte(len(username))}, []byte(username)...)
		auth = append(auth, byte(len(password)))
		auth = append(auth, []byte(password)...)
		if _, err = conn.Write(auth); err != nil {
			return nil, err
		}
		response := make([]byte, 2)
		if _, err = io.ReadFull(conn, response); err != nil {
			return nil, err
		}
		if response[1] != 0x00 {
			return nil, fmt.Errorf("socks5 username/password authentication failed")
		}
	default:
		return nil, fmt.Errorf("socks5 proxy selected unsupported authentication method %d", choice[1])
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return nil, err
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return nil, fmt.Errorf("invalid target port %q", port)
	}
	request := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if ipv4 := ip.To4(); ipv4 != nil {
			request = append(request, 0x01)
			request = append(request, ipv4...)
		} else {
			request = append(request, 0x04)
			request = append(request, ip.To16()...)
		}
	} else {
		if len(host) > 255 {
			return nil, fmt.Errorf("target hostname too long")
		}
		request = append(request, 0x03, byte(len(host)))
		request = append(request, []byte(host)...)
	}
	var portBytes [2]byte
	binary.BigEndian.PutUint16(portBytes[:], uint16(portNumber))
	request = append(request, portBytes[:]...)
	if _, err = conn.Write(request); err != nil {
		return nil, err
	}
	header := make([]byte, 4)
	if _, err = io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	if header[1] != 0x00 {
		return nil, fmt.Errorf("socks5 connect failed with code %d", header[1])
	}
	var skip int
	switch header[3] {
	case 0x01:
		skip = 4
	case 0x04:
		skip = 16
	case 0x03:
		length := make([]byte, 1)
		if _, err = io.ReadFull(conn, length); err != nil {
			return nil, err
		}
		skip = int(length[0])
	default:
		return nil, fmt.Errorf("socks5 returned invalid address type")
	}
	if _, err = io.CopyN(io.Discard, conn, int64(skip+2)); err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	closeOnError = false
	return conn, nil
}

func (g *Gateway) snapshotSlots() []*proxySlot {
	g.slotsMu.RLock()
	defer g.slotsMu.RUnlock()
	return append([]*proxySlot(nil), g.slots...)
}

func (g *Gateway) proxyRefreshLoop() {
	ticker := time.NewTicker(g.cfg.ProxyRefresh)
	defer ticker.Stop()
	for range ticker.C {
		if err := g.refreshProxyList(); err != nil {
			g.log.Warn("dynamic proxy refresh failed", "error", err)
			g.addLog("warn", "dynamic proxy refresh failed", map[string]any{"error": err.Error()})
		}
	}
}

func (g *Gateway) refreshProxyList() error {
	data, err := os.ReadFile(g.cfg.ProxyListFile)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{})
	var rawURLs []string
	for _, line := range strings.Split(string(data), "\n") {
		raw := strings.TrimSpace(strings.SplitN(line, "#", 2)[0])
		if raw == "" {
			continue
		}
		if _, ok := seen[raw]; !ok {
			seen[raw] = struct{}{}
			rawURLs = append(rawURLs, raw)
		}
	}
	candidates, err := g.buildProxySlots(rawURLs)
	if err != nil {
		return err
	}
	existing := make(map[string]*proxySlot)
	for _, slot := range g.snapshotSlots() {
		if slot.url != "" {
			existing[slot.url] = slot
		}
	}
	for i, slot := range candidates {
		if previous := existing[slot.url]; previous != nil {
			candidates[i] = previous
		}
	}
	type probeResult struct {
		slot *proxySlot
		ip   string
	}
	results := make(chan probeResult, len(candidates))
	jobs := make(chan *proxySlot)
	var wg sync.WaitGroup
	workerCount := min(g.cfg.ProxyProbeJobs, len(candidates))
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range jobs {
				ip, err := g.probeSlot(s)
				if err == nil {
					results <- probeResult{slot: s, ip: ip}
				}
			}
		}()
	}
	go func() {
		for _, slot := range candidates {
			jobs <- slot
		}
		close(jobs)
	}()
	wg.Wait()
	close(results)

	// Multiple local ports can resolve to the same tunnel egress. Keep one slot per observed IP.
	byEgress := make(map[string]*proxySlot)
	for result := range results {
		result.slot.mu.Lock()
		result.slot.egress = result.ip
		result.slot.mu.Unlock()
		key := result.ip
		if key == "" {
			key = result.slot.url
		}
		if _, exists := byEgress[key]; !exists {
			byEgress[key] = result.slot
		}
	}
	var dynamic []*proxySlot
	for _, raw := range rawURLs {
		for _, slot := range byEgress {
			if slot.url == raw {
				dynamic = append(dynamic, slot)
				break
			}
		}
	}
	target := g.cfg.TargetEgress
	if target == 0 {
		target = g.cfg.MaxConcurrency
	}
	if len(dynamic) > target {
		dynamic = dynamic[:target]
	}

	base := append(make([]*proxySlot, 0, len(g.base)+len(dynamic)), g.base...)
	base = append(base, dynamic...)
	if len(base) == 0 {
		return fmt.Errorf("no proxy in %s passed the health probe", g.cfg.ProxyListFile)
	}
	g.slotsMu.Lock()
	g.slots = base
	g.slotsMu.Unlock()
	g.log.Info("dynamic proxy slots refreshed", "candidates", len(candidates), "healthy_unique_egress", len(dynamic), "total_slots", len(base))
	g.addLog("info", "dynamic proxy slots refreshed", map[string]any{"candidates": len(candidates), "healthy_unique_egress": len(dynamic), "total_slots": len(base)})
	return nil
}

func (g *Gateway) probeSlot(slot *proxySlot) (string, error) {
	var failures []string
	for _, probeURL := range g.cfg.ProxyProbeURLs {
		ctx, cancel := context.WithTimeout(context.Background(), g.cfg.ProxyProbeWait)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
		if err == nil {
			var resp *http.Response
			resp, err = slot.client.Do(req)
			if err == nil {
				body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
				resp.Body.Close()
				if readErr == nil && resp.StatusCode >= 200 && resp.StatusCode < 400 {
					if ip := parseProbeIP(body); ip != "" {
						cancel()
						return ip, nil
					}
					err = fmt.Errorf("invalid IP response")
				} else if readErr != nil {
					err = readErr
				} else {
					err = fmt.Errorf("HTTP %d", resp.StatusCode)
				}
			}
		}
		cancel()
		failures = append(failures, probeURL+": "+err.Error())
	}
	return "", fmt.Errorf("probe via %s failed (%s)", safeProxyURL(slot.url), strings.Join(failures, "; "))
}

func (g *Gateway) refreshSlotEgress(slot *proxySlot) (string, error) {
	if slot == nil || slot.client == nil {
		return "", errors.New("proxy slot is unavailable")
	}
	// A Mihomo selector switch only affects new connections. Close any pooled
	// upstream connection before probing so subsequent requests use the new node.
	slot.client.CloseIdleConnections()
	ip, err := g.probeSlot(slot)
	if err != nil {
		slot.mu.Lock()
		slot.disabled = true
		slot.probeError = err.Error()
		slot.mu.Unlock()
		return "", err
	}
	slot.mu.Lock()
	previous := slot.egress
	slot.egress = ip
	slot.fails = 0
	slot.until = time.Time{}
	slot.disabled = false
	slot.probeError = ""
	if previous != ip {
		slot.modelCooldowns = nil
	}
	slot.mu.Unlock()
	if previous != ip {
		g.addLog("info", "proxy slot egress changed", map[string]any{"url": safeProxyURL(slot.url), "previous": previous, "egress": ip})
	}
	return ip, nil
}

func safeProxyURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "[redacted-proxy]"
	}
	parsed.User = nil
	return parsed.String()
}

func parseProbeIP(body []byte) string {
	text := strings.TrimSpace(string(body))
	if net.ParseIP(text) != nil {
		return text
	}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ip=") {
			if ip := strings.TrimSpace(strings.TrimPrefix(line, "ip=")); net.ParseIP(ip) != nil {
				return ip
			}
		}
	}
	return ""
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/", g.admin)
	mux.HandleFunc("/healthz", g.health)
	mux.HandleFunc("/metrics", g.metrics)
	mux.HandleFunc("/v1/", g.serve)
	mux.HandleFunc("/openai/", g.serve)
	mux.HandleFunc("/anthropic/", g.serve)
	mux.HandleFunc("/codex/", g.serve)
	return mux
}

func (g *Gateway) admin(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin")
	if !g.adminAuthorized(r) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	switch {
	case path == "/summary" && r.Method == http.MethodGet:
		g.writeJSON(w, http.StatusOK, g.summary())
	case path == "/keys" && r.Method == http.MethodGet:
		g.keyMu.RLock()
		keys := make([]string, 0, len(g.keys))
		for key := range g.keys {
			keys = append(keys, key)
		}
		g.keyMu.RUnlock()
		g.writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
	case path == "/keys" && r.Method == http.MethodPost:
		key, err := newGatewayKey()
		if err != nil {
			http.Error(w, `{"error":"key_generation_failed"}`, http.StatusInternalServerError)
			return
		}
		g.keyMu.Lock()
		g.keys[key] = struct{}{}
		g.persistKeysLocked()
		g.keyMu.Unlock()
		g.writeJSON(w, http.StatusCreated, map[string]string{"key": key})
	case path == "/keys" && r.Method == http.MethodPut:
		var body struct {
			Keys []string `json:"keys"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&body) != nil {
			http.Error(w, `{"error":"invalid_keys"}`, http.StatusBadRequest)
			return
		}
		replacement := make(map[string]struct{}, len(body.Keys))
		for _, key := range body.Keys {
			if key = strings.TrimSpace(key); key != "" {
				replacement[key] = struct{}{}
			}
		}
		g.keyMu.Lock()
		g.keys = replacement
		g.persistKeysLocked()
		g.keyMu.Unlock()
		g.writeJSON(w, http.StatusOK, map[string]int{"count": len(replacement)})
	case strings.HasPrefix(path, "/keys/") && r.Method == http.MethodDelete:
		key := strings.TrimPrefix(path, "/keys/")
		g.keyMu.Lock()
		delete(g.keys, key)
		g.persistKeysLocked()
		g.keyMu.Unlock()
		g.writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
	case path == "/audit" && r.Method == http.MethodGet:
		g.auditMu.RLock()
		records := append([]auditRecord(nil), g.audits...)
		g.auditMu.RUnlock()
		g.writeJSON(w, http.StatusOK, map[string]any{"records": records})
	case path == "/logs" && r.Method == http.MethodGet:
		g.logsMu.RLock()
		logs := append([]systemLog(nil), g.logs...)
		g.logsMu.RUnlock()
		g.writeJSON(w, http.StatusOK, map[string]any{"logs": logs})
	case path == "/slots" && r.Method == http.MethodPost:
		var body struct {
			URL     string `json:"url"`
			Enabled bool   `json:"enabled"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body) != nil {
			http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
			return
		}
		found := false
		for _, slot := range g.snapshotSlots() {
			slot.mu.Lock()
			if slot.url == body.URL {
				slot.disabled = !body.Enabled
				found = true
			}
			slot.mu.Unlock()
		}
		if !found {
			http.Error(w, `{"error":"slot_not_found"}`, http.StatusNotFound)
			return
		}
		g.writeJSON(w, http.StatusOK, map[string]bool{"enabled": body.Enabled})
		g.addLog("info", "proxy slot state changed", map[string]any{"url": body.URL, "enabled": body.Enabled})
	case path == "/probe" && r.Method == http.MethodPost:
		var body struct {
			URL string `json:"url"`
		}
		if r.Body != nil && r.ContentLength != 0 && json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body) != nil {
			http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
			return
		}
		results := make([]map[string]string, 0)
		matched := false
		for _, slot := range g.snapshotSlots() {
			if body.URL != "" && slot.url != body.URL {
				continue
			}
			matched = true
			ip, err := g.refreshSlotEgress(slot)
			if err != nil {
				g.writeJSON(w, http.StatusBadGateway, map[string]string{"error": "probe_failed", "url": slot.url, "detail": err.Error()})
				return
			}
			results = append(results, map[string]string{"url": slot.url, "egress": ip})
		}
		if !matched {
			http.Error(w, `{"error":"slot_not_found"}`, http.StatusNotFound)
			return
		}
		g.writeJSON(w, http.StatusOK, map[string]any{"slots": results})
	case path == "/rotate" && r.Method == http.MethodPost:
		var body struct {
			Forbidden []string `json:"forbidden"`
		}
		if r.Body != nil && r.ContentLength != 0 && json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&body) != nil {
			http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
			return
		}
		slots := g.snapshotSlots()
		if len(slots) < 2 {
			http.Error(w, `{"error":"no_alternative_slot"}`, http.StatusConflict)
			return
		}
		forbidden := make(map[string]struct{}, len(body.Forbidden))
		for _, ip := range body.Forbidden {
			if ip = strings.TrimSpace(ip); ip != "" {
				forbidden[ip] = struct{}{}
			}
		}
		current := int(g.active.Load() % uint64(len(slots)))
		previous := slots[current]
		previous.mu.Lock()
		previousURL, previousEgress := previous.url, previous.egress
		previous.mu.Unlock()
		now := time.Now()
		for offset := 1; offset < len(slots); offset++ {
			index := (current + offset) % len(slots)
			slot := slots[index]
			disabled, ready := slot.readiness("", now)
			if disabled || now.Before(ready) {
				continue
			}
			slot.mu.Lock()
			egress, raw := slot.egress, slot.url
			slot.mu.Unlock()
			if egress == "" {
				var err error
				egress, err = g.refreshSlotEgress(slot)
				if err != nil {
					continue
				}
			}
			if egress == previousEgress {
				continue
			}
			if _, occupied := forbidden[egress]; occupied {
				continue
			}
			slot.client.CloseIdleConnections()
			g.active.Store(uint64(index))
			g.addLog("info", "active proxy slot changed", map[string]any{"previous_url": previousURL, "previous_egress": previousEgress, "url": raw, "egress": egress})
			g.writeJSON(w, http.StatusOK, map[string]any{"previous_url": previousURL, "previous_egress": previousEgress, "url": raw, "egress": egress, "index": index})
			return
		}
		http.Error(w, `{"error":"no_unique_alternative_slot"}`, http.StatusConflict)
	case path == "/refresh" && r.Method == http.MethodPost:
		if g.cfg.ProxyListFile == "" {
			http.Error(w, `{"error":"dynamic_proxy_list_disabled"}`, http.StatusConflict)
			return
		}
		if err := g.refreshProxyList(); err != nil {
			http.Error(w, `{"error":"refresh_failed"}`, http.StatusBadGateway)
			return
		}
		g.writeJSON(w, http.StatusOK, g.summary())
	default:
		http.NotFound(w, r)
	}
}

func (g *Gateway) adminAuthorized(r *http.Request) bool {
	return (g.cfg.AdminToken != "" && constantTimeBearer(r, g.cfg.AdminToken)) ||
		(g.cfg.InstanceAdminToken != "" && constantTimeBearer(r, g.cfg.InstanceAdminToken))
}

func (g *Gateway) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (g *Gateway) summary() map[string]any {
	type modelCooldownInfo struct {
		Model   string    `json:"model"`
		Fails   int       `json:"fails"`
		ReadyAt time.Time `json:"ready_at"`
	}
	type slotInfo struct {
		URL            string              `json:"url"`
		Egress         string              `json:"egress,omitempty"`
		ReadyAt        time.Time           `json:"ready_at,omitempty"`
		Fails          int                 `json:"fails"`
		Direct         bool                `json:"direct"`
		Enabled        bool                `json:"enabled"`
		Healthy        bool                `json:"healthy"`
		Active         bool                `json:"active"`
		ModelCooldowns []modelCooldownInfo `json:"model_cooldowns,omitempty"`
	}
	items := make([]slotInfo, 0)
	now := time.Now()
	slots := g.snapshotSlots()
	activeIndex := 0
	if len(slots) > 0 {
		activeIndex = int(g.active.Load() % uint64(len(slots)))
	}
	for index, slot := range slots {
		slot.mu.Lock()
		cooldowns := make([]modelCooldownInfo, 0, len(slot.modelCooldowns))
		for model, state := range slot.modelCooldowns {
			if now.Before(state.Until) {
				cooldowns = append(cooldowns, modelCooldownInfo{Model: model, Fails: state.Fails, ReadyAt: state.Until})
			} else {
				delete(slot.modelCooldowns, model)
			}
		}
		sort.Slice(cooldowns, func(i, j int) bool { return cooldowns[i].Model < cooldowns[j].Model })
		items = append(items, slotInfo{URL: slot.url, Egress: slot.egress, ReadyAt: slot.until, Fails: slot.fails, Direct: slot.url == "", Enabled: !slot.disabled, Healthy: !slot.disabled && (slot.url == "" || slot.egress != ""), Active: index == activeIndex, ModelCooldowns: cooldowns})
		slot.mu.Unlock()
	}
	return map[string]any{
		"slots":           items,
		"stats":           map[string]uint64{"requests": g.stats.Requests.Load(), "success": g.stats.Success.Load(), "upstream429": g.stats.Upstream429.Load(), "gateway429": g.stats.Gateway429.Load(), "errors": g.stats.Errors.Load()},
		"max_concurrency": g.cfg.MaxConcurrency,
	}
}

func (g *Gateway) loadKeys() {
	data, err := os.ReadFile(g.cfg.DataDir + "/keys.json")
	if err != nil {
		return
	}
	var keys []string
	if json.Unmarshal(data, &keys) != nil {
		return
	}
	g.keyMu.Lock()
	defer g.keyMu.Unlock()
	for _, key := range keys {
		if key != "" {
			g.keys[key] = struct{}{}
		}
	}
}

func (g *Gateway) persistKeysLocked() {
	keys := make([]string, 0, len(g.keys))
	for key := range g.keys {
		keys = append(keys, key)
	}
	data, _ := json.MarshalIndent(keys, "", "  ")
	if err := writeGatewayPrivateFileAtomic(g.cfg.DataDir+"/keys.json", data); err != nil && g.log != nil {
		g.log.Error("persist gateway keys failed", "error", err)
	}
}

func writeGatewayPrivateFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(dir, ".write-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func (g *Gateway) addLog(level, message string, fields map[string]any) {
	g.logsMu.Lock()
	g.logs = append(g.logs, systemLog{At: time.Now(), Level: level, Message: message, Fields: fields})
	if len(g.logs) > 500 {
		g.logs = g.logs[len(g.logs)-500:]
	}
	g.logsMu.Unlock()
}

func newGatewayKey() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "gw_" + hex.EncodeToString(buf), nil
}
func (g *Gateway) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "application/json")
	now := time.Now()
	configured := false
	for _, slot := range g.snapshotSlots() {
		slot.mu.Lock()
		usable := !slot.disabled && (slot.url == "" || slot.egress != "")
		healthy := usable && !now.Before(slot.until)
		slot.mu.Unlock()
		configured = configured || usable
		if healthy {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"status":"ok"}`)
			return
		}
	}
	if configured {
		// Cooling is upstream state, not process health. Keep the instance in
		// the control-plane pool so callers receive a retryable 429 instead of
		// a misleading no_healthy_gateway_instances response.
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"cooling"}`)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = io.WriteString(w, `{"status":"probing","error":"no_healthy_proxy_slot"}`)
}
func (g *Gateway) metrics(w http.ResponseWriter, r *http.Request) {
	if g.cfg.AdminToken != "" && !constantTimeBearer(r, g.cfg.AdminToken) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	w.Header().Set("content-type", "application/json")
	_, _ = io.WriteString(w, `{"requests":`+strconv.FormatUint(g.stats.Requests.Load(), 10)+`,"success":`+strconv.FormatUint(g.stats.Success.Load(), 10)+`,"upstream429":`+strconv.FormatUint(g.stats.Upstream429.Load(), 10)+`,"gateway429":`+strconv.FormatUint(g.stats.Gateway429.Load(), 10)+`,"errors":`+strconv.FormatUint(g.stats.Errors.Load(), 10)+`,"slots":`+strconv.Itoa(len(g.snapshotSlots()))+`}`)
}

func (g *Gateway) serve(w http.ResponseWriter, r *http.Request) {
	g.stats.Requests.Add(1)
	if !g.authorized(r) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	r = r.WithContext(context.WithValue(r.Context(), auditMetadataKey{}, auditMetadata{ClientKey: maskGatewayKey(requestCredential(r))}))
	if allowed, allow := methodAllowed(r.URL.Path, r.Method); !allowed {
		w.Header().Set("Allow", allow)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		payload, _ := json.Marshal(map[string]string{
			"error":  "method_not_allowed",
			"method": r.Method,
			"path":   r.URL.Path,
			"allow":  allow,
		})
		_, _ = w.Write(payload)
		return
	}
	select {
	case g.queue <- struct{}{}:
		defer func() { <-g.queue }()
	default:
		g.stats.Gateway429.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, `{"error":"gateway_overloaded"}`, http.StatusTooManyRequests)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), g.cfg.RequestTimeout)
	defer cancel()
	select {
	case g.sem <- struct{}{}:
		defer func() { <-g.sem }()
	case <-ctx.Done():
		g.stats.Gateway429.Add(1)
		http.Error(w, `{"error":"queue_timeout"}`, http.StatusGatewayTimeout)
		return
	}
	if err := g.forward(ctx, w, r); err != nil {
		g.stats.Errors.Add(1)
		g.addLog("error", "forward failed", map[string]any{"error": err.Error(), "path": r.URL.Path})
		if !errors.Is(err, context.Canceled) {
			g.log.Error("forward failed", "error", err)
		}
	}
}

// methodAllowed prevents malformed method/path combinations from being sent
// to the upstream, where they otherwise appear as misleading 404 responses.
func methodAllowed(path, method string) (bool, string) {
	for _, prefix := range []string{"/openai", "/anthropic", "/codex"} {
		path = strings.TrimPrefix(path, prefix)
	}
	switch path {
	case "/v1/chat/completions", "/v1/responses":
		return method == http.MethodPost, http.MethodPost
	case "/v1/models":
		return method == http.MethodGet, http.MethodGet
	default:
		return true, ""
	}
}

func constantTimeBearer(r *http.Request, expected string) bool {
	got := requestBearer(r)
	return len(got) == len(expected) && subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}
func (g *Gateway) authorized(r *http.Request) bool {
	got := requestCredential(r)
	g.keyMu.RLock()
	defer g.keyMu.RUnlock()
	for key := range g.keys {
		if len(got) == len(key) && subtle.ConstantTimeCompare([]byte(got), []byte(key)) == 1 {
			return true
		}
	}
	return false
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

func maskGatewayKey(key string) string {
	if key == "" {
		return ""
	}
	if len(key) <= 8 {
		return "key_***"
	}
	return key[:3] + "..." + key[len(key)-4:]
}

func auditMetadataFor(r *http.Request) auditMetadata {
	meta, _ := r.Context().Value(auditMetadataKey{}).(auditMetadata)
	return meta
}

func (g *Gateway) forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<20))
	if err != nil {
		http.Error(w, `{"error":"request_body_too_large"}`, http.StatusRequestEntityTooLarge)
		return nil
	}
	path := r.URL.Path
	anthropicRequest := strings.HasPrefix(path, "/anthropic/")
	for _, prefix := range []string{"/openai", "/anthropic", "/codex"} {
		path = strings.TrimPrefix(path, prefix)
	}
	if path == "" {
		path = "/"
	}
	body, err = g.normalizeRequestBodyChecked(path, body)
	if err != nil {
		g.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported_responses_input", "message": err.Error()})
		return nil
	}
	model := requestModel(body)
	streaming := streamingRequest(body)
	gatewayRequestID := requestID()
	meta := auditMetadataFor(r)
	meta.Stream = streaming
	meta.RequestID = gatewayRequestID
	r = r.WithContext(context.WithValue(r.Context(), auditMetadataKey{}, meta))
	w.Header().Set("X-Gateway-Request-ID", gatewayRequestID)
	if g.cfg.UpstreamProvider == ProviderFreeBuff {
		return g.forwardFreeBuff(ctx, w, r, path, body, model, streaming)
	}
	if g.cfg.UpstreamProvider == ProviderVertex {
		return g.forwardVertex(ctx, w, r, path, body, model, streaming)
	}
	baseResponseHeaders := w.Header().Clone()
	target := upstreamTargetURL(g.cfg.UpstreamURL, path)
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	tried := make(map[string]struct{})
	for attempt := 0; attempt <= g.cfg.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if g.cfg.UpstreamProvider == ProviderCline {
			if slot, wait := g.selectSlotExcluding(model, tried); slot == nil && wait >= 0 {
				retryAfter := max(1, int((wait+time.Second-1)/time.Second))
				g.stats.Gateway429.Add(1)
				w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
				http.Error(w, `{"error":"upstream_cooling"}`, http.StatusTooManyRequests)
				return nil
			}
		}
		slot, err := g.waitForSlotExcluding(ctx, model, tried)
		if err != nil {
			if errors.Is(err, errNoUntriedSlot) {
				http.Error(w, `{"error":"no_untried_upstream"}`, http.StatusBadGateway)
			} else {
				http.Error(w, `{"error":"upstream_cooldown_timeout"}`, http.StatusGatewayTimeout)
			}
			return nil
		}
		tried[slot.identity()] = struct{}{}
		req, err := http.NewRequestWithContext(ctx, r.Method, target, bytes.NewReader(body))
		if err != nil {
			return err
		}
		copyHeaders(req.Header, r.Header)
		req.Header.Set("Authorization", "Bearer "+g.cfg.UpstreamAPIKey)
		// The gateway can add SSE lifecycle events, so it must receive a body it
		// can forward or transform without preserving a stale encoded length.
		req.Header.Set("Accept-Encoding", "identity")
		for _, header := range []string{"User-Agent", "HTTP-Referer", "X-Title", "X-OpenCode-Client", "X-OpenCode-Request", "X-IS-MULTIROOT", "X-CLIENT-TYPE", "X-CLIENT-VERSION", "X-PLATFORM", "X-PLATFORM-VERSION", "X-CORE-VERSION", "X-Task-ID"} {
			req.Header.Del(header)
		}
		if g.cfg.UpstreamProvider == ProviderOpenCode {
			req.Header.Set("User-Agent", opencodeUserAgent)
			req.Header.Set("HTTP-Referer", opencodeReferer)
			req.Header.Set("X-Title", opencodeTitle)
			req.Header.Set("X-OpenCode-Client", opencodeClient)
			if req.Header.Get("X-OpenCode-Request") == "" {
				req.Header.Set("X-OpenCode-Request", gatewayRequestID)
			}
		} else if g.cfg.UpstreamProvider == ProviderCline {
			req.Header.Set("User-Agent", "Cline/4.1.4")
			req.Header.Set("HTTP-Referer", "https://cline.bot")
			req.Header.Set("X-Title", "Cline")
			req.Header.Set("X-IS-MULTIROOT", "false")
			req.Header.Set("X-CLIENT-TYPE", "cline-cli")
			req.Header.Set("X-CLIENT-VERSION", "4.1.4")
			req.Header.Set("X-PLATFORM", "cli")
			req.Header.Set("X-PLATFORM-VERSION", "4.1.4")
			req.Header.Set("X-CORE-VERSION", "0.0.70")
			req.Header.Set("X-Task-ID", newUUID())
		}
		if anthropicRequest {
			// Use the configured upstream credential, never the client's gateway key.
			req.Header.Set("X-API-Key", g.cfg.UpstreamAPIKey)
		}
		req.Header.Del("Host")
		req.Host = ""
		started := time.Now()
		resp, err := slot.client.Do(req)
		if err != nil {
			g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", attempt+1, "")
			slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, 0)
			if attempt < g.cfg.MaxRetries && g.hasUntriedSlot(model, tried) {
				continue
			}
			http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
			return nil
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfterHeader := resp.Header.Get("Retry-After")
			retryAfter := parseRetryAfter(retryAfterHeader)
			if retryAfterHeader == "" && g.cfg.CooldownMax > 0 {
				retryAfterHeader = strconv.Itoa(max(1, int(g.cfg.CooldownMax/time.Second)))
				resp.Header.Set("Retry-After", retryAfterHeader)
			}
			g.stats.Upstream429.Add(1)
			if g.cfg.UpstreamProvider == ProviderOpenCode || g.cfg.UpstreamProvider == ProviderCline {
				// OpenCode public/free limits can be tied to the current egress.
				// Cool only this slot so the next retry can use another IP.
				slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, retryAfter)
				slot.cooldownModel(model, g.cfg.CooldownBase, g.cfg.CooldownMax, retryAfter)
				g.recordAudit(r, model, resp.StatusCode, slot, started, "upstream", attempt+1, retryAfterHeader)
				if attempt < g.cfg.MaxRetries && g.hasUntriedSlot(model, tried) {
					resp.Body.Close()
					continue
				}
			} else {
				// TokenRouter limits are account-scoped. Cooling every slot keeps
				// this instance from retrying the same account and lets the control
				// plane select another instance/key.
				g.cooldownModelAllSlots(model, retryAfter)
				g.recordAudit(r, model, resp.StatusCode, slot, started, "upstream", attempt+1, retryAfterHeader)
			}
		} else if resp.StatusCode >= 500 {
			data, readErr := readBufferedResponse(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				g.handleUpstreamBodyFailure(r, slot, model, readErr)
				g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", attempt+1, "")
				if attempt < g.cfg.MaxRetries && g.hasUntriedSlot(model, tried) {
					continue
				}
				http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
				return fmt.Errorf("upstream response body: %w", readErr)
			}
			if g.cfg.UpstreamProvider == ProviderCline && isClineEmptyResponse(resp.StatusCode, data) {
				// This is a completed Cline inference with no answer, not an exit or
				// instance failure. Keeping the slot healthy prevents one empty model
				// result from cascading into health-check 503s for later requests.
				slot.success(model)
				g.recordAudit(r, model, resp.StatusCode, slot, started, "upstream", attempt+1, "")
				copyResponseHeaders(w.Header(), resp.Header)
				setNoStoreResponseHeaders(w.Header())
				w.Header().Set(instanceHealthyHeader, "true")
				w.WriteHeader(resp.StatusCode)
				_, _ = w.Write(data)
				return nil
			}
			retryAfterHeader := resp.Header.Get("Retry-After")
			retryAfter := parseRetryAfter(retryAfterHeader)
			slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, retryAfter)
			g.recordAudit(r, model, resp.StatusCode, slot, started, "upstream", attempt+1, retryAfterHeader)
			if attempt < g.cfg.MaxRetries && g.hasUntriedSlot(model, tried) {
				continue
			}
			copyResponseHeaders(w.Header(), resp.Header)
			setNoStoreResponseHeaders(w.Header())
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write(data)
			return nil
		}
		defer resp.Body.Close()
		if g.cfg.FreeModelsOnly && path == "/v1/models" && resp.StatusCode == http.StatusOK {
			if err := g.copyFilteredModels(w, resp); err != nil {
				g.handleUpstreamBodyFailure(r, slot, model, err)
				g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", attempt+1, "")
				if errors.Is(err, errUpstreamResponseRead) {
					http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
				}
				return fmt.Errorf("upstream response body: %w", err)
			}
			slot.success(model)
			g.stats.Success.Add(1)
			g.recordAudit(r, model, resp.StatusCode, slot, started, "upstream", attempt+1, "")
			return nil
		}
		if !streaming && resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
			data, readErr := readBufferedResponse(resp.Body)
			if readErr != nil {
				g.handleUpstreamBodyFailure(r, slot, model, readErr)
				g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", attempt+1, "")
				if attempt < g.cfg.MaxRetries && g.hasUntriedSlot(model, tried) && errors.Is(readErr, errUpstreamResponseRead) {
					resp.Body.Close()
					continue
				}
				http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
				return fmt.Errorf("upstream response body: %w", readErr)
			}
			if g.cfg.UpstreamProvider == ProviderCline && path == "/v1/chat/completions" && resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
				var changed bool
				data, changed = normalizeClineBufferedResponse(data)
				if changed {
					resp.Header.Del("Content-Length")
					resp.Header.Del("Content-Encoding")
					resp.Header.Set("Content-Type", "application/json")
				}
			}
			copyResponseHeaders(w.Header(), resp.Header)
			setNoStoreResponseHeaders(w.Header())
			w.WriteHeader(resp.StatusCode)
			if _, writeErr := w.Write(data); writeErr != nil {
				return fmt.Errorf("client response write: %w", writeErr)
			}
			slot.success(model)
			if resp.StatusCode >= 200 && resp.StatusCode < 400 {
				g.stats.Success.Add(1)
			}
			g.recordAuditWithUsage(r, model, resp.StatusCode, slot, started, "upstream", attempt+1, "", parseTokenUsage(data), 0)
			return nil
		}
		copyResponseHeaders(w.Header(), resp.Header)
		setNoStoreResponseHeaders(w.Header())
		delayStreamCommit := streaming && resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
		if delayStreamCommit {
			w.Header().Del("Content-Length")
			w.Header().Del("Content-Encoding")
		}
		if !delayStreamCommit {
			w.WriteHeader(resp.StatusCode)
		}
		usage, firstTokenMS, committed, err := copyStreamResponse(w, resp.Body, started, delayStreamCommit, path, model, g.cfg.StreamFirstOutputTimeout)
		if err != nil {
			g.handleUpstreamBodyFailure(r, slot, model, err)
			if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
				g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", attempt+1, "")
			}
			if delayStreamCommit && !committed && attempt < g.cfg.MaxRetries && g.hasUntriedSlot(model, tried) {
				resp.Body.Close()
				restoreResponseHeaders(w.Header(), baseResponseHeaders)
				continue
			}
			if delayStreamCommit && !committed {
				restoreResponseHeaders(w.Header(), baseResponseHeaders)
				http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
			}
			return fmt.Errorf("upstream response body: %w", err)
		}
		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
			slot.success(model)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 400 {
			g.stats.Success.Add(1)
		}
		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
			g.recordAuditWithUsage(r, model, resp.StatusCode, slot, started, "upstream", attempt+1, "", usage, firstTokenMS)
		}
		return err
	}
	return nil
}

func upstreamTargetURL(baseURL, requestPath string) string {
	baseURL = strings.TrimRight(baseURL, "/")
	if strings.HasSuffix(baseURL, "/v1") && strings.HasPrefix(requestPath, "/v1/") {
		return baseURL + strings.TrimPrefix(requestPath, "/v1")
	}
	return baseURL + requestPath
}

// normalizeRequestBody is retained for focused compatibility tests. The
// forwarding path uses normalizeRequestBodyChecked so unsupported native
// Responses input is rejected instead of silently changing its meaning.
func (g *Gateway) normalizeRequestBody(path string, body []byte) []byte {
	normalized, _ := g.normalizeRequestBodyChecked(path, body)
	return normalized
}

func (g *Gateway) normalizeRequestBodyChecked(path string, body []byte) ([]byte, error) {
	if !supportsReasoningControls(path) || len(body) == 0 {
		return body, nil
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return body, nil
	}
	changed := false
	model, _ := payload["model"].(string)
	if g.cfg.UpstreamProvider == ProviderFreeBuff {
		if upstreamModel := g.freeBuffUpstreamModel(model); upstreamModel != model {
			model = upstreamModel
			payload["model"] = model
			changed = true
		}
	} else if upstreamModel := stripClientModelAlias(g.cfg.UpstreamProvider, model); upstreamModel != model {
		model = upstreamModel
		payload["model"] = model
		changed = true
	}
	if g.cfg.ForcedModel != "" && model != g.cfg.ForcedModel {
		model = g.cfg.ForcedModel
		payload["model"] = model
		changed = true
	}
	if g.cfg.UpstreamProvider == ProviderOpenCode && path == "/v1/responses" {
		converted, err := normalizeResponsesInput(payload)
		if err != nil {
			return body, err
		}
		if converted {
			changed = true
		}
		if normalizeResponsesMessageIDs(payload) {
			changed = true
		}
	}
	if g.cfg.UpstreamProvider == ProviderFreeBuff && path == "/v1/responses" {
		converted, err := normalizeResponsesInput(payload)
		if err != nil {
			return body, err
		}
		if converted {
			changed = true
		}
		if normalizeResponsesMessageIDs(payload) {
			changed = true
		}
	}
	if g.cfg.UpstreamProvider == ProviderTokenRouter && g.cfg.IsolateUpstreamState {
		if isolateTokenRouterState(payload) {
			changed = true
		}
	}
	if g.cfg.UpstreamProvider == ProviderCline {
		if clampOutputTokenBudget(path, payload, clineMaxOutputTokens) {
			changed = true
		}
	}
	if path == "/v1/responses" {
		converted, err := normalizeResponsesTools(payload)
		if err != nil {
			return body, err
		}
		if converted {
			changed = true
		}
		converted, err = normalizeResponsesToolChoice(payload)
		if err != nil {
			return body, err
		}
		if converted {
			changed = true
		}
	}
	if g.cfg.FreeModelsOnly && model != "" && model != "big-pickle" && !strings.HasSuffix(model, "-free") {
		model += "-free"
		payload["model"] = model
		changed = true
	}
	if g.cfg.DisableThinkingByDefault && isDeepSeekFlashFree(model) {
		_, hasReasoningEffort := payload["reasoning_effort"]
		thinking, hasThinking := payload["thinking"]
		if !hasReasoningEffort && (!hasThinking || thinkingIsDisabled(thinking)) {
			payload["reasoning_effort"] = "none"
			changed = true
		}
		if !hasThinking && !hasReasoningEffort {
			payload["thinking"] = map[string]string{"type": "disabled"}
			changed = true
		}
	}
	if g.cfg.MinThinkingMaxTokens > 0 && isDeepSeekFlashFree(model) && reasoningIsEnabled(payload) {
		if raiseThinkingTokenBudget(path, payload, g.cfg.MinThinkingMaxTokens) {
			changed = true
		}
	}
	if !changed {
		return body, nil
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return body, nil
	}
	return encoded, nil
}

func (g *Gateway) freeBuffUpstreamModel(model string) string {
	model = strings.TrimSpace(model)
	if !strings.HasPrefix(strings.ToLower(model), "freebuff/") {
		return model
	}
	alias := model[len("FreeBuff/"):]
	g.refreshFreeBuffModelMap()
	g.freeBuffModelMu.RLock()
	defer g.freeBuffModelMu.RUnlock()
	for id := range g.freeBuffModels {
		candidate := id
		if index := strings.IndexByte(candidate, '/'); index >= 0 && index+1 < len(candidate) {
			candidate = candidate[index+1:]
		}
		if candidate == alias {
			return id
		}
	}
	return alias
}

func stripClientModelAlias(provider, model string) string {
	switch provider {
	case ProviderTokenRouter:
		if model == tokenRouterClientModel {
			return tokenRouterModel
		}
	case ProviderOpenCode:
		if upstream, ok := openCodeUpstreamModel(model); ok {
			return upstream
		}
		// Unknown alias (e.g. a free model added upstream after this build):
		// strip the client prefix so the FreeModelsOnly suffixing produces a
		// valid upstream model name instead of forwarding the alias verbatim.
		if strings.HasPrefix(strings.ToLower(model), "opencode/") {
			return strings.TrimSpace(model[len("opencode/"):])
		}
	case ProviderCline:
		if model == clineClientModel {
			return clineUpstreamModel
		}
		if strings.HasPrefix(strings.ToLower(model), "cline/") {
			return model[len("cline/"):]
		}
	case ProviderFreeBuff:
		if strings.HasPrefix(strings.ToLower(model), "freebuff/") {
			return model[len("FreeBuff/"):]
		}
	case ProviderVertex:
		// Generic prefix strip so Gemini models added to vproxy after this
		// build resolve without a worker update.
		if strings.HasPrefix(strings.ToLower(model), "vertex/") {
			return strings.TrimSpace(model[len("vertex/"):])
		}
	}
	return model
}

// TokenRouter is used as a stateless upstream by this gateway. Its optional
// session fields can bind a request to provider-side worker/KV state, so they
// are removed unless stateful forwarding is explicitly enabled.
func isolateTokenRouterState(payload map[string]any) bool {
	changed := false
	for _, key := range []string{"session_id", "previous_response_id", "conversation_id"} {
		if _, exists := payload[key]; exists {
			delete(payload, key)
			changed = true
		}
	}
	return changed
}

func setNoStoreResponseHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store, private")
	header.Set("Pragma", "no-cache")
}

func supportsReasoningControls(path string) bool {
	return path == "/v1/chat/completions" || path == "/v1/responses"
}

// Responses clients define function tools with name/description/parameters at
// the tool's top level. TokenRouter's OpenAI-compatible decoder expects the
// Chat Completions shape, where those fields live under function. Normalize
// both forms so native Responses clients can use the gateway.
func normalizeResponsesTools(payload map[string]any) (bool, error) {
	rawTools, exists := payload["tools"]
	if !exists {
		return false, nil
	}
	tools, ok := rawTools.([]any)
	if !ok {
		return false, fmt.Errorf("responses tools must be an array")
	}
	changed := false
	for index, rawTool := range tools {
		tool, ok := rawTool.(map[string]any)
		if !ok {
			return false, fmt.Errorf("responses tool %d must be an object", index)
		}
		typeName, _ := tool["type"].(string)
		if typeName != "function" {
			continue
		}

		if rawFunction, hasFunction := tool["function"]; hasFunction {
			function, ok := rawFunction.(map[string]any)
			if !ok {
				return false, fmt.Errorf("responses tool %d function must be an object", index)
			}
			name, _ := function["name"].(string)
			if strings.TrimSpace(name) == "" {
				if topName, ok := tool["name"].(string); ok && strings.TrimSpace(topName) != "" {
					function["name"] = topName
					changed = true
				} else {
					return false, fmt.Errorf("responses tool %d function.name is required", index)
				}
			}
			for _, field := range []string{"description", "parameters", "strict"} {
				if value, exists := tool[field]; exists {
					if _, nestedExists := function[field]; !nestedExists {
						function[field] = value
					}
					delete(tool, field)
					changed = true
				}
			}
			if _, exists := tool["name"]; exists {
				delete(tool, "name")
				changed = true
			}
			continue
		}

		name, _ := tool["name"].(string)
		if strings.TrimSpace(name) == "" {
			return false, fmt.Errorf("responses tool %d name is required", index)
		}
		function := map[string]any{"name": name}
		for _, field := range []string{"description", "parameters", "strict"} {
			if value, exists := tool[field]; exists {
				function[field] = value
				delete(tool, field)
			}
		}
		delete(tool, "name")
		tool["function"] = function
		changed = true
	}
	return changed, nil
}

func normalizeResponsesToolChoice(payload map[string]any) (bool, error) {
	rawChoice, exists := payload["tool_choice"]
	if !exists {
		return false, nil
	}
	choice, ok := rawChoice.(map[string]any)
	if !ok {
		return false, nil
	}
	typeName, _ := choice["type"].(string)
	if typeName != "function" {
		return false, nil
	}
	if rawFunction, hasFunction := choice["function"]; hasFunction {
		function, ok := rawFunction.(map[string]any)
		if !ok {
			return false, fmt.Errorf("responses tool_choice function must be an object")
		}
		name, _ := function["name"].(string)
		if strings.TrimSpace(name) == "" {
			if topName, ok := choice["name"].(string); ok && strings.TrimSpace(topName) != "" {
				function["name"] = topName
				delete(choice, "name")
				return true, nil
			}
			return false, fmt.Errorf("responses tool_choice function.name is required")
		}
		if _, exists := choice["name"]; exists {
			delete(choice, "name")
			return true, nil
		}
		return false, nil
	}
	name, _ := choice["name"].(string)
	if strings.TrimSpace(name) == "" {
		return false, fmt.Errorf("responses tool_choice name is required")
	}
	delete(choice, "name")
	choice["function"] = map[string]any{"name": name}
	return true, nil
}

// Zen's /v1/responses endpoint emits Responses-shaped output but accepts the
// chat-style messages request contract. Translate standard Responses input at
// the edge so native Responses clients can use the gateway unchanged.
func normalizeResponsesInput(payload map[string]any) (bool, error) {
	input, hasInput := payload["input"]
	if !hasInput {
		return false, nil
	}
	if _, hasMessages := payload["messages"]; hasMessages {
		// Legacy clients may deliberately send messages to this endpoint. Keep
		// that contract intact and only discard the otherwise-invalid input key.
		delete(payload, "input")
		return true, nil
	}
	messages, err := responsesInputMessages(input)
	if err != nil {
		return false, err
	}
	if instructions, ok := payload["instructions"].(string); ok && strings.TrimSpace(instructions) != "" {
		messages = append([]any{map[string]any{"role": "system", "content": instructions}}, messages...)
	}
	payload["messages"] = messages
	delete(payload, "input")
	delete(payload, "instructions")
	return true, nil
}

func responsesInputMessages(input any) ([]any, error) {
	switch value := input.(type) {
	case string:
		return []any{map[string]any{"role": "user", "content": value}}, nil
	case []any:
		messages := make([]any, 0, len(value))
		for _, item := range value {
			message, ok, err := responsesInputMessage(item)
			if err != nil {
				return nil, err
			}
			if ok {
				messages = append(messages, message)
			} else {
				return nil, fmt.Errorf("%w: input item type is not supported", errUnsupportedResponsesInput)
			}
		}
		return messages, nil
	default:
		message, ok, err := responsesInputMessage(value)
		if err != nil {
			return nil, err
		}
		if ok {
			return []any{message}, nil
		}
		return nil, fmt.Errorf("%w: input value is not supported", errUnsupportedResponsesInput)
	}
}

func responsesInputMessage(raw any) (map[string]any, bool, error) {
	item, ok := raw.(map[string]any)
	if !ok {
		if text, ok := raw.(string); ok {
			return map[string]any{"role": "user", "content": text}, true, nil
		}
		return nil, false, nil
	}
	typeName, _ := item["type"].(string)
	role, hasRole := item["role"].(string)
	if hasRole || typeName == "message" {
		if role == "" {
			role = "user"
		}
		content, err := responsesContent(item["content"])
		if err != nil {
			return nil, false, err
		}
		message := map[string]any{"role": role, "content": content}
		if id, ok := item["id"].(string); ok && strings.TrimSpace(id) != "" {
			message["id"] = id
		}
		return message, true, nil
	}
	switch typeName {
	case "function_call_output":
		content := item["output"]
		if content == nil {
			content = item["content"]
		}
		converted, err := responsesContent(content)
		if err != nil {
			return nil, false, err
		}
		message := map[string]any{"role": "tool", "content": converted}
		if callID, ok := item["call_id"].(string); ok && callID != "" {
			message["tool_call_id"] = callID
		}
		return message, true, nil
	case "function_call":
		function := map[string]any{"name": item["name"], "arguments": item["arguments"]}
		toolCall := map[string]any{"type": "function", "function": function}
		if callID, ok := item["call_id"].(string); ok && callID != "" {
			toolCall["id"] = callID
		}
		return map[string]any{"role": "assistant", "content": "", "tool_calls": []any{toolCall}}, true, nil
	default:
		if text, ok := item["text"].(string); ok {
			return map[string]any{"role": "user", "content": text}, true, nil
		}
		return nil, false, nil
	}
}

func responsesContent(content any) (any, error) {
	parts, ok := content.([]any)
	if !ok {
		if content == nil {
			return "", nil
		}
		return content, nil
	}
	var text strings.Builder
	for _, raw := range parts {
		part, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		typeName, _ := part["type"].(string)
		if typeName != "input_text" && typeName != "output_text" && typeName != "text" {
			return nil, fmt.Errorf("%w: content part %q", errUnsupportedResponsesInput, typeName)
		}
		if value, ok := part["text"].(string); ok {
			text.WriteString(value)
		}
	}
	return text.String(), nil
}

// Some Responses clients keep conversation history as message-like input
// objects without IDs. Zen's Responses bridge deserializes that history into
// its internal messages type, where an ID is mandatory. Add IDs only to those
// top-level message records and preserve every existing client-provided ID.
func normalizeResponsesMessageIDs(payload map[string]any) bool {
	changed := false
	for _, field := range []string{"messages", "input"} {
		items, ok := payload[field].([]any)
		if !ok {
			continue
		}
		for index, raw := range items {
			message, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			_, hasRole := message["role"]
			messageType, _ := message["type"].(string)
			if field == "input" && !hasRole && messageType != "message" {
				continue
			}
			if id, ok := message["id"].(string); ok && strings.TrimSpace(id) != "" {
				continue
			}
			message["id"] = fmt.Sprintf("msg_gateway_%s_%d", field, index)
			changed = true
		}
	}
	return changed
}

func thinkingIsDisabled(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		mode, _ := typed["type"].(string)
		return strings.EqualFold(strings.TrimSpace(mode), "disabled")
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "disabled")
	case bool:
		return !typed
	default:
		return false
	}
}

func reasoningIsEnabled(payload map[string]any) bool {
	if value, exists := payload["reasoning_effort"]; exists {
		effort, _ := value.(string)
		effort = strings.ToLower(strings.TrimSpace(effort))
		return effort != "" && effort != "none"
	}
	value, exists := payload["thinking"]
	if !exists {
		return false
	}
	switch typed := value.(type) {
	case map[string]any:
		mode, _ := typed["type"].(string)
		return strings.EqualFold(strings.TrimSpace(mode), "enabled")
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "enabled")
	case bool:
		return typed
	default:
		return false
	}
}

func raiseThinkingTokenBudget(path string, payload map[string]any, minimum int) bool {
	keys := []string{"max_tokens", "max_completion_tokens"}
	defaultKey := "max_tokens"
	if path == "/v1/responses" {
		keys = []string{"max_output_tokens"}
		defaultKey = "max_output_tokens"
	}
	changed := false
	found := false
	for _, key := range keys {
		value, exists := payload[key]
		if !exists {
			continue
		}
		found = true
		if current, ok := value.(float64); ok && current < float64(minimum) {
			payload[key] = minimum
			changed = true
		}
	}
	if !found {
		payload[defaultKey] = minimum
		changed = true
	}
	return changed
}

func clampOutputTokenBudget(path string, payload map[string]any, maximum int) bool {
	keys := []string{"max_tokens", "max_completion_tokens"}
	if path == "/v1/responses" {
		keys = []string{"max_output_tokens"}
	}
	changed := false
	for _, key := range keys {
		if current, ok := payload[key].(float64); ok && current > float64(maximum) {
			payload[key] = maximum
			changed = true
		}
	}
	return changed
}

func isClineEmptyResponse(status int, data []byte) bool {
	if status != http.StatusInternalServerError {
		return false
	}
	var payload struct {
		Error   any  `json:"error"`
		Success bool `json:"success"`
	}
	if json.Unmarshal(data, &payload) != nil || payload.Success {
		return false
	}
	message := ""
	switch value := payload.Error.(type) {
	case string:
		message = value
	case map[string]any:
		message, _ = value["message"].(string)
	}
	return strings.EqualFold(strings.TrimSpace(message), "empty response content")
}

func addReasoningContent(payload map[string]any, containerKey string) bool {
	changed := false
	choices, _ := payload["choices"].([]any)
	for _, rawChoice := range choices {
		choice, _ := rawChoice.(map[string]any)
		container, _ := choice[containerKey].(map[string]any)
		reasoning, _ := container["reasoning"].(string)
		if reasoning != "" {
			if _, exists := container["reasoning_content"]; !exists {
				container["reasoning_content"] = reasoning
				changed = true
			}
		}
	}
	return changed
}

func normalizeClineBufferedResponse(data []byte) ([]byte, bool) {
	var payload map[string]any
	if json.Unmarshal(data, &payload) != nil {
		return data, false
	}
	changed := false
	if success, _ := payload["success"].(bool); success {
		if unwrapped, ok := payload["data"].(map[string]any); ok {
			payload = unwrapped
			changed = true
		}
	}
	if addReasoningContent(payload, "message") {
		changed = true
	}
	if !changed {
		return data, false
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return data, false
	}
	return encoded, true
}

func normalizeClineSSELine(line string) string {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "data:") {
		return line
	}
	data := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
	if data == "" || data == "[DONE]" {
		return line
	}
	var payload map[string]any
	if json.Unmarshal([]byte(data), &payload) != nil || !addReasoningContent(payload, "delta") {
		return line
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return line
	}
	ending := ""
	if strings.HasSuffix(line, "\r\n") {
		ending = "\r\n"
	} else if strings.HasSuffix(line, "\n") {
		ending = "\n"
	}
	return "data: " + string(encoded) + ending
}

func isDeepSeekFlashFree(model string) bool {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(model)), "-free") == "deepseek-v4-flash"
}

func requestModel(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var payload struct {
		Model string `json:"model"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return ""
	}
	return strings.TrimSpace(payload.Model)
}

func streamingRequest(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	var payload struct {
		Stream bool `json:"stream"`
	}
	return json.Unmarshal(body, &payload) == nil && payload.Stream
}

func (g *Gateway) handleUpstreamBodyFailure(r *http.Request, slot *proxySlot, model string, err error) {
	if slot == nil || !errors.Is(err, errUpstreamResponseRead) {
		return
	}
	// A truncated upstream response is tied to this connection or exit. Close
	// the pooled connection and cool the slot so the next request uses another
	// healthy candidate instead of immediately repeating the same failure.
	slot.client.CloseIdleConnections()
	minimum := time.Duration(0)
	if errors.Is(err, errUpstreamStreamEmpty) || errors.Is(err, errUpstreamFirstOutputTimeout) {
		minimum = g.cfg.StreamFailureCooldown
	}
	slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, minimum)
	if model != "" {
		slot.cooldownModel(model, g.cfg.CooldownBase, g.cfg.CooldownMax, minimum)
	}
	failedEgress := ""
	slot.mu.Lock()
	failedEgress = slot.egress
	slot.mu.Unlock()
	if next, _ := g.selectSlotExcluding(model, map[string]struct{}{slot.identity(): {}}); next != nil {
		next.mu.Lock()
		nextEgress := next.egress
		next.mu.Unlock()
		g.addLog("warn", "upstream response truncated; switched exit", map[string]any{"request_id": auditMetadataFor(r).RequestID, "previous_egress": failedEgress, "egress": nextEgress, "model": model})
	}
}

func (g *Gateway) copyFilteredModels(w http.ResponseWriter, resp *http.Response) error {
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("%w: %w", errUpstreamResponseRead, err)
	}
	var payload struct {
		Object string           `json:"object,omitempty"`
		Data   []map[string]any `json:"data"`
	}
	if json.Unmarshal(data, &payload) != nil {
		w.WriteHeader(resp.StatusCode)
		_, err = w.Write(data)
		if err != nil {
			return fmt.Errorf("client response write: %w", err)
		}
		return nil
	}
	filtered := payload.Data[:0]
	for _, model := range payload.Data {
		id, _ := model["id"].(string)
		if strings.HasSuffix(id, "-free") || id == "big-pickle" {
			filtered = append(filtered, model)
		}
	}
	payload.Data = filtered
	copyResponseHeaders(w.Header(), resp.Header)
	setNoStoreResponseHeaders(w.Header())
	w.Header().Del("Content-Length")
	w.Header().Del("Content-Encoding")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		return fmt.Errorf("client response write: %w", err)
	}
	return nil
}

func readBufferedResponse(src io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(src, maxBufferedResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errUpstreamResponseRead, err)
	}
	if len(data) > maxBufferedResponseBytes {
		return nil, fmt.Errorf("upstream response exceeds %d bytes", maxBufferedResponseBytes)
	}
	return data, nil
}

func requestID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(bytes[:])
}

func (g *Gateway) recordAudit(r *http.Request, model string, status int, slot *proxySlot, started time.Time, source string, attempts int, retryAfter string) {
	g.recordAuditWithUsage(r, model, status, slot, started, source, attempts, retryAfter, tokenUsage{}, 0)
}

func (g *Gateway) recordAuditWithUsage(r *http.Request, model string, status int, slot *proxySlot, started time.Time, source string, attempts int, retryAfter string, usage tokenUsage, firstTokenMS int64) {
	slot.mu.Lock()
	egress := slot.egress
	slot.mu.Unlock()
	meta := auditMetadataFor(r)
	record := auditRecord{At: time.Now(), RequestID: meta.RequestID, Method: r.Method, Path: r.URL.Path, Model: model, Status: status, Slot: slot.url, Egress: egress, LatencyMS: time.Since(started).Milliseconds(), Source: source, Attempts: attempts, RetryAfter: retryAfter, ClientKey: meta.ClientKey, Stream: meta.Stream, PromptTokens: usage.PromptTokens, CompletionTokens: usage.CompletionTokens, TotalTokens: usage.TotalTokens, CachedTokens: usage.CachedTokens, FirstTokenMS: firstTokenMS}
	g.auditMu.Lock()
	g.audits = append(g.audits, record)
	if len(g.audits) > 500 {
		g.audits = g.audits[len(g.audits)-500:]
	}
	g.auditMu.Unlock()
}

func (g *Gateway) selectSlot() (*proxySlot, time.Duration) {
	return g.selectSlotExcluding("", nil)
}

func (g *Gateway) selectSlotExcluding(model string, excluded map[string]struct{}) (*proxySlot, time.Duration) {
	now := time.Now()
	slots := g.snapshotSlots()
	if len(slots) == 0 {
		return nil, time.Second
	}
	start := int(g.active.Load() % uint64(len(slots)))
	var earliest time.Time
	for i := 0; i < len(slots); i++ {
		index := (start + i) % len(slots)
		s := slots[index]
		if _, tried := excluded[s.identity()]; tried {
			continue
		}
		disabled, ready := s.readiness(model, now)
		if disabled {
			continue
		}
		if !now.Before(ready) {
			g.active.Store(uint64(index))
			return s, 0
		}
		if earliest.IsZero() || ready.Before(earliest) {
			earliest = ready
		}
	}
	if earliest.IsZero() {
		return nil, -1
	}
	return nil, time.Until(earliest)
}
func (g *Gateway) waitForSlot(ctx context.Context) (*proxySlot, error) {
	for {
		slot, wait := g.selectSlot()
		if slot != nil {
			return slot, nil
		}
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
func (g *Gateway) waitForSlotExcluding(ctx context.Context, model string, excluded map[string]struct{}) (*proxySlot, error) {
	for {
		slot, wait := g.selectSlotExcluding(model, excluded)
		if slot != nil {
			return slot, nil
		}
		if wait < 0 {
			return nil, errNoUntriedSlot
		}
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
func (g *Gateway) hasUntriedSlot(model string, excluded map[string]struct{}) bool {
	for _, slot := range g.snapshotSlots() {
		disabled, _ := slot.readiness(model, time.Now())
		if disabled {
			continue
		}
		if _, tried := excluded[slot.identity()]; !tried {
			return true
		}
	}
	return false
}

func (g *Gateway) cooldownModelAllSlots(model string, retryAfter time.Duration) {
	if model == "" {
		return
	}
	minimum := retryAfter
	if minimum <= 0 {
		minimum = g.cfg.CooldownMax
	}
	for _, slot := range g.snapshotSlots() {
		slot.cooldownModel(model, g.cfg.CooldownBase, g.cfg.CooldownMax, minimum)
	}
}

func copyHeaders(dst, src http.Header) {
	for _, key := range []string{"content-type", "accept", "x-opencode-client", "x-opencode-session", "x-opencode-project", "x-opencode-request", "anthropic-version", "anthropic-beta"} {
		if v := src.Values(key); len(v) > 0 {
			dst.Del(key)
			for _, item := range v {
				dst.Add(key, item)
			}
		}
	}
}
func copyResponseHeaders(dst, src http.Header) {
	for key, values := range src {
		switch strings.ToLower(key) {
		case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func restoreResponseHeaders(dst, snapshot http.Header) {
	clear(dst)
	for key, values := range snapshot {
		dst[key] = append([]string(nil), values...)
	}
}

// copyResponse remains as a chat-completions compatibility wrapper for callers
// outside the request forwarder and for focused protocol tests.
func copyResponse(w http.ResponseWriter, src io.Reader, started time.Time, delayUntilFirstOutput bool) (tokenUsage, int64, bool, error) {
	return copyStreamResponse(w, src, started, delayUntilFirstOutput, "/v1/chat/completions", "", 0)
}

type sseLineResult struct {
	line string
	err  error
}

// responsesStreamState fills the minimum lifecycle snapshot expected by strict
// Responses SDKs when an OpenAI-compatible upstream only emits delta events.
// Its synthetic response ID is kept stable through the terminal event.
type responsesStreamState struct {
	id               string
	model            string
	createdAt        int64
	created          bool
	upstreamCreated  bool
	outputAdded      bool
	contentAdded     bool
	textDone         bool
	contentDone      bool
	outputDone       bool
	functionArgsDone bool
	outputIndex      int
	contentIndex     int
	itemID           string
	text             strings.Builder
	items            map[int]map[string]any
}

func newResponsesStreamState(model string, started time.Time) *responsesStreamState {
	model = strings.TrimSpace(model)
	if model == "" {
		model = "unknown"
	}
	id := "resp_" + requestID()
	return &responsesStreamState{id: id, model: model, createdAt: started.Unix(), itemID: "msg_" + id, items: make(map[int]map[string]any)}
}

func responsesSSE(event string, payload any) string {
	data, _ := json.Marshal(payload)
	return "event: " + event + "\n" + "data: " + string(data) + "\n\n"
}

func (s *responsesStreamState) textPart() map[string]any {
	return map[string]any{"type": "output_text", "text": s.text.String(), "annotations": []any{}}
}

func (s *responsesStreamState) messageItem(status string, includeContent bool) map[string]any {
	item := map[string]any{"id": s.itemID, "type": "message", "status": status, "role": "assistant"}
	if includeContent {
		item["content"] = []any{s.textPart()}
	} else {
		item["content"] = []any{}
	}
	return item
}

func cloneResponseItem(item map[string]any) map[string]any {
	data, err := json.Marshal(item)
	if err != nil {
		return map[string]any{}
	}
	var cloned map[string]any
	if json.Unmarshal(data, &cloned) != nil {
		return map[string]any{}
	}
	return cloned
}

func (s *responsesStreamState) currentItem() map[string]any {
	if item := s.items[s.outputIndex]; item != nil {
		return cloneResponseItem(item)
	}
	return s.messageItem("in_progress", false)
}

func (s *responsesStreamState) outputSnapshot(complete bool) []any {
	if len(s.items) == 0 {
		if complete && s.text.Len() > 0 {
			return []any{s.messageItem("completed", true)}
		}
		return []any{}
	}
	indexes := make([]int, 0, len(s.items))
	for index := range s.items {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	output := make([]any, 0, len(indexes))
	for _, index := range indexes {
		item := cloneResponseItem(s.items[index])
		if complete {
			item["status"] = "completed"
		}
		if index == s.outputIndex && item["type"] == "message" && s.text.Len() > 0 {
			item["content"] = []any{s.textPart()}
		}
		output = append(output, item)
	}
	return output
}

func (s *responsesStreamState) snapshot(status string, usage tokenUsage, complete bool) map[string]any {
	output := []any{}
	if complete {
		output = s.outputSnapshot(true)
	}
	response := map[string]any{
		"id": s.id, "object": "response", "created_at": s.createdAt, "status": status, "model": s.model,
		"output": output, "error": nil, "incomplete_details": nil, "instructions": nil,
		"max_output_tokens": nil, "parallel_tool_calls": true, "previous_response_id": nil,
		"reasoning": map[string]any{}, "store": true, "temperature": 1, "tool_choice": "auto",
		"tools": []any{}, "top_p": 1, "truncation": "disabled", "user": nil, "metadata": map[string]any{},
	}
	if complete {
		response["usage"] = map[string]any{
			"input_tokens": usage.PromptTokens, "output_tokens": usage.CompletionTokens, "total_tokens": usage.TotalTokens,
			"input_tokens_details": map[string]any{"cached_tokens": usage.CachedTokens},
		}
	} else {
		response["usage"] = nil
	}
	return response
}

func (s *responsesStreamState) createdEvent() string {
	return responsesSSE("response.created", map[string]any{"type": "response.created", "response": s.snapshot("in_progress", tokenUsage{}, false)})
}

func (s *responsesStreamState) outputItemAddedEvent() string {
	return responsesSSE("response.output_item.added", map[string]any{"type": "response.output_item.added", "output_index": s.outputIndex, "item": s.currentItem()})
}

func (s *responsesStreamState) contentPartAddedEvent() string {
	part := map[string]any{"type": "output_text", "text": "", "annotations": []any{}}
	return responsesSSE("response.content_part.added", map[string]any{"type": "response.content_part.added", "item_id": s.itemID, "output_index": s.outputIndex, "content_index": s.contentIndex, "part": part})
}

func (s *responsesStreamState) completionEvents() string {
	if len(s.items) == 0 && s.text.Len() == 0 {
		return ""
	}
	var events strings.Builder
	if s.text.Len() > 0 && !s.textDone {
		events.WriteString(responsesSSE("response.output_text.done", map[string]any{"type": "response.output_text.done", "item_id": s.itemID, "output_index": s.outputIndex, "content_index": s.contentIndex, "text": s.text.String()}))
		s.textDone = true
	}
	if s.text.Len() > 0 && !s.contentDone {
		events.WriteString(responsesSSE("response.content_part.done", map[string]any{"type": "response.content_part.done", "item_id": s.itemID, "output_index": s.outputIndex, "content_index": s.contentIndex, "part": s.textPart()}))
		s.contentDone = true
	}
	current := s.currentItem()
	if current["type"] == "function_call" && !s.functionArgsDone {
		arguments, _ := current["arguments"].(string)
		events.WriteString(responsesSSE("response.function_call_arguments.done", map[string]any{"type": "response.function_call_arguments.done", "item_id": current["id"], "output_index": s.outputIndex, "arguments": arguments}))
		s.functionArgsDone = true
	}
	if !s.outputDone {
		item := current
		item["status"] = "completed"
		if item["type"] == "message" && s.text.Len() > 0 {
			item["content"] = []any{s.textPart()}
		}
		events.WriteString(responsesSSE("response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": s.outputIndex, "item": item}))
		s.outputDone = true
	}
	return events.String()
}

func (s *responsesStreamState) completedEvent(usage tokenUsage) string {
	return responsesSSE("response.completed", map[string]any{"type": "response.completed", "response": s.snapshot("completed", usage, true)})
}

func (s *responsesStreamState) observe(line string) {
	var payload struct {
		Type         string `json:"type"`
		Delta        string `json:"delta"`
		ItemID       string `json:"item_id"`
		OutputIndex  *int   `json:"output_index"`
		ContentIndex *int   `json:"content_index"`
		Item         struct {
			ID string `json:"id"`
		} `json:"item"`
	}
	if !parseSSEData(line, &payload) {
		return
	}
	if payload.ItemID != "" {
		s.itemID = payload.ItemID
	} else if payload.Item.ID != "" {
		s.itemID = payload.Item.ID
	}
	if payload.OutputIndex != nil {
		s.outputIndex = *payload.OutputIndex
	}
	if payload.ContentIndex != nil {
		s.contentIndex = *payload.ContentIndex
	}
	switch payload.Type {
	case "response.output_item.added":
		s.outputAdded = true
	case "response.content_part.added":
		s.contentAdded = true
	case "response.output_text.delta":
		if payload.Delta != "" {
			s.text.WriteString(payload.Delta)
		}
	case "response.output_text.done":
		s.textDone = true
	case "response.content_part.done":
		s.contentDone = true
	case "response.output_item.done":
		s.outputDone = true
	case "response.function_call_arguments.done":
		s.functionArgsDone = true
	}
	var raw map[string]any
	if !parseSSEData(line, &raw) {
		return
	}
	item, _ := raw["item"].(map[string]any)
	switch payload.Type {
	case "response.output_item.added", "response.output_item.done":
		if item != nil {
			s.items[s.outputIndex] = cloneResponseItem(item)
		}
	case "response.function_call_arguments.delta":
		current := s.items[s.outputIndex]
		if current == nil {
			current = map[string]any{"id": s.itemID, "type": "function_call", "status": "in_progress", "call_id": s.itemID, "name": "", "arguments": ""}
			s.items[s.outputIndex] = current
		}
		arguments, _ := current["arguments"].(string)
		current["arguments"] = arguments + payload.Delta
	}
}

func (s *responsesStreamState) ensureTextLifecycle() string {
	var events strings.Builder
	if !s.outputAdded {
		events.WriteString(s.outputItemAddedEvent())
		s.outputAdded = true
	}
	if !s.contentAdded {
		events.WriteString(s.contentPartAddedEvent())
		s.contentAdded = true
	}
	return events.String()
}

func (s *responsesStreamState) ensureOutputLifecycle() string {
	if s.outputAdded {
		return ""
	}
	s.outputAdded = true
	return s.outputItemAddedEvent()
}

func (s *responsesStreamState) normalizeTextDelta(line string) string {
	var payload map[string]any
	if !parseSSEData(line, &payload) || payload["type"] != "response.output_text.delta" {
		return line
	}
	if _, exists := payload["item_id"]; !exists {
		payload["item_id"] = s.itemID
	}
	if _, exists := payload["output_index"]; !exists {
		payload["output_index"] = s.outputIndex
	}
	if _, exists := payload["content_index"]; !exists {
		payload["content_index"] = s.contentIndex
	}
	data, _ := json.Marshal(payload)
	return "data: " + string(data) + "\n"
}

func (s *responsesStreamState) observeCreated(line string) {
	var payload struct {
		Response struct {
			ID        string `json:"id"`
			Model     string `json:"model"`
			CreatedAt int64  `json:"created_at"`
		} `json:"response"`
	}
	if !parseSSEData(line, &payload) {
		return
	}
	if payload.Response.ID != "" {
		s.id = payload.Response.ID
		s.itemID = "msg_" + s.id
	}
	if payload.Response.Model != "" {
		s.model = payload.Response.Model
	}
	if payload.Response.CreatedAt > 0 {
		s.createdAt = payload.Response.CreatedAt
	}
}

func parseSSEData(line string, target any) bool {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "data:") {
		return false
	}
	return json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), target) == nil
}

func responsesLineType(line string) string {
	var payload struct {
		Type string `json:"type"`
	}
	if !parseSSEData(line, &payload) {
		return ""
	}
	return payload.Type
}

func responsesLineIsCreated(line string) bool {
	line = strings.TrimSpace(line)
	return line == "event: response.created" || responsesLineType(line) == "response.created"
}

func responsesLineIsCompleted(line string) bool {
	return responsesLineType(line) == "response.completed"
}

func responsesLineIsTextDelta(line string) bool {
	return responsesEventName(line) == "response.output_text.delta" || responsesLineType(line) == "response.output_text.delta"
}

func responsesLineIsFunctionCallDelta(line string) bool {
	return responsesEventName(line) == "response.function_call_arguments.delta" || responsesLineType(line) == "response.function_call_arguments.delta"
}

func responsesEventName(line string) string {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "event:") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(line, "event:"))
}

func responsesLineNeedsCreated(line string) bool {
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "event: response.") {
		return line != "event: response.created"
	}
	return strings.HasPrefix(responsesLineType(line), "response.") && responsesLineType(line) != "response.created"
}

type sseFrame struct {
	event string
	data  string
	raw   string
}

func readSSEFrame(reader *bufio.Reader) (string, error) {
	var frame strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			frame.WriteString(line)
		}
		if strings.TrimSpace(line) == "" && frame.Len() > 0 {
			return frame.String(), err
		}
		if err != nil {
			return frame.String(), err
		}
	}
}

func parseSSEFrame(raw string) sseFrame {
	frame := sseFrame{raw: raw}
	var data []string
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSuffix(line, "\r")
		switch {
		case strings.HasPrefix(line, "event:"):
			frame.event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	frame.data = strings.Join(data, "\n")
	return frame
}

func responseEventPayload(frame sseFrame) (map[string]any, string, bool) {
	if frame.data == "" || frame.data == "[DONE]" {
		return nil, "", false
	}
	var payload map[string]any
	if json.Unmarshal([]byte(frame.data), &payload) != nil {
		return nil, "", false
	}
	typeName, _ := payload["type"].(string)
	if typeName == "" {
		typeName = frame.event
	}
	return payload, typeName, strings.HasPrefix(typeName, "response.")
}

func responsesCompletedHasOutput(payload map[string]any) bool {
	response, _ := payload["response"].(map[string]any)
	output, _ := response["output"].([]any)
	return len(output) > 0
}

func (s *responsesStreamState) normalizeTextDeltaPayload(payload map[string]any) map[string]any {
	if _, exists := payload["item_id"]; !exists {
		payload["item_id"] = s.itemID
	}
	if _, exists := payload["output_index"]; !exists {
		payload["output_index"] = s.outputIndex
	}
	if _, exists := payload["content_index"]; !exists {
		payload["content_index"] = s.contentIndex
	}
	return payload
}

// copyResponsesStreamResponse operates on complete SSE events rather than
// independent lines. It keeps event headers coupled to their data payloads,
// which is required when lifecycle events are inserted for strict SDKs.
func copyResponsesStreamResponse(w http.ResponseWriter, src io.Reader, started time.Time, delayUntilFirstOutput bool, model string, firstOutputTimeout time.Duration) (tokenUsage, int64, bool, error) {
	reader := bufio.NewReaderSize(src, 32<<10)
	flusher, _ := w.(http.Flusher)
	responses := newResponsesStreamState(model, started)
	var usage tokenUsage
	var firstTokenMS int64
	committed := false
	seenCompleted := false
	seenTerminalFailure := false
	var pending strings.Builder
	write := func(data string) error {
		if _, err := io.WriteString(w, data); err != nil {
			return fmt.Errorf("client response write: %w", err)
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}
	readFrame := func() (string, error) {
		if !delayUntilFirstOutput || committed || firstOutputTimeout <= 0 {
			return readSSEFrame(reader)
		}
		remaining := time.Until(started.Add(firstOutputTimeout))
		if remaining <= 0 {
			return "", errUpstreamFirstOutputTimeout
		}
		result := make(chan sseLineResult, 1)
		go func() {
			raw, err := readSSEFrame(reader)
			result <- sseLineResult{line: raw, err: err}
		}()
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		select {
		case result := <-result:
			return result.line, result.err
		case <-timer.C:
			return "", errUpstreamFirstOutputTimeout
		}
	}
	for {
		raw, readErr := readFrame()
		if raw != "" {
			frame := parseSSEFrame(raw)
			payload, typeName, isResponseEvent := responseEventPayload(frame)
			out := raw
			hasOutput := false
			if isResponseEvent {
				dataLine := "data: " + frame.data
				if typeName == "response.created" {
					responses.created = true
					responses.upstreamCreated = true
					responses.observeCreated(dataLine)
				} else {
					responses.observe(dataLine)
					var injected strings.Builder
					if !responses.created {
						injected.WriteString(responses.createdEvent())
						responses.created = true
					}
					switch typeName {
					case "response.output_text.delta":
						injected.WriteString(responses.ensureTextLifecycle())
						payload = responses.normalizeTextDeltaPayload(payload)
					case "response.function_call_arguments.delta":
						injected.WriteString(responses.ensureOutputLifecycle())
					case "response.completed":
						seenCompleted = true
						usage = mergeTokenUsage(usage, parseSSETokenUsage(dataLine))
						if !responsesCompletedHasOutput(payload) {
							injected.WriteString(responses.completionEvents())
							out = injected.String() + responses.completedEvent(usage)
						} else {
							injected.WriteString(responses.completionEvents())
							if !responses.upstreamCreated {
								if response, ok := payload["response"].(map[string]any); ok {
									response["id"] = responses.id
									if _, hasModel := response["model"]; !hasModel {
										response["model"] = responses.model
									}
								}
							}
							out = injected.String() + responsesSSE(typeName, payload)
						}
					case "response.failed", "response.incomplete":
						seenTerminalFailure = true
					}
					if typeName != "response.completed" {
						out = injected.String() + responsesSSE(typeName, payload)
					}
				}
				usage = mergeTokenUsage(usage, parseSSETokenUsage("data: "+frame.data))
				hasOutput = sseLineHasOutputForPath("data: "+frame.data, true)
			}
			if delayUntilFirstOutput && !committed {
				pending.WriteString(out)
				if hasOutput {
					firstTokenMS = time.Since(started).Milliseconds()
					if err := write(pending.String()); err != nil {
						return usage, firstTokenMS, false, err
					}
					pending.Reset()
					committed = true
				}
			} else {
				if firstTokenMS == 0 && hasOutput {
					firstTokenMS = time.Since(started).Milliseconds()
				}
				if err := write(out); err != nil {
					return usage, firstTokenMS, committed, err
				}
				committed = true
			}
		}
		if readErr == io.EOF {
			if delayUntilFirstOutput && !committed {
				return usage, firstTokenMS, false, fmt.Errorf("%w: %w", errUpstreamResponseRead, errUpstreamStreamEmpty)
			}
			if seenTerminalFailure {
				return usage, firstTokenMS, committed, fmt.Errorf("responses upstream terminated with failure")
			}
			if delayUntilFirstOutput && !seenCompleted {
				return usage, firstTokenMS, committed, fmt.Errorf("%w: responses stream ended without response.completed", errUpstreamResponseRead)
			}
			return usage, firstTokenMS, committed, nil
		}
		if readErr != nil {
			if errors.Is(readErr, errUpstreamFirstOutputTimeout) {
				return usage, firstTokenMS, committed, fmt.Errorf("%w: %w", errUpstreamResponseRead, readErr)
			}
			return usage, firstTokenMS, committed, fmt.Errorf("%w: %w", errUpstreamResponseRead, readErr)
		}
	}
}

func copyStreamResponse(w http.ResponseWriter, src io.Reader, started time.Time, delayUntilFirstOutput bool, path, model string, firstOutputTimeout time.Duration) (tokenUsage, int64, bool, error) {
	if path == "/v1/responses" {
		return copyResponsesStreamResponse(w, src, started, delayUntilFirstOutput, model, firstOutputTimeout)
	}
	reader := bufio.NewReaderSize(src, 32<<10)
	flusher, _ := w.(http.Flusher)
	var usage tokenUsage
	var firstTokenMS int64
	seenFinishReason := false
	seenDone := false
	seenResponsesCompleted := false
	var pending strings.Builder
	committed := false
	responsesAPI := path == "/v1/responses"
	pendingFunctionEventHeader := ""
	var responses *responsesStreamState
	if responsesAPI {
		responses = newResponsesStreamState(model, started)
	}
	write := func(data string) error {
		if _, writeErr := io.WriteString(w, data); writeErr != nil {
			return fmt.Errorf("client response write: %w", writeErr)
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}
	readLine := func() (string, error) {
		if !delayUntilFirstOutput || committed || firstOutputTimeout <= 0 {
			return reader.ReadString('\n')
		}
		remaining := time.Until(started.Add(firstOutputTimeout))
		if remaining <= 0 {
			return "", errUpstreamFirstOutputTimeout
		}
		result := make(chan sseLineResult, 1)
		go func() {
			line, err := reader.ReadString('\n')
			result <- sseLineResult{line: line, err: err}
		}()
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		select {
		case result := <-result:
			return result.line, result.err
		case <-timer.C:
			return "", errUpstreamFirstOutputTimeout
		}
	}
	for {
		line, err := readLine()
		if model == clineUpstreamModel {
			line = normalizeClineSSELine(line)
		}
		// The terminal event header must travel with its reconstructed data
		// payload. Forwarding the original header here would create two adjacent
		// response.completed events when the next line is normalized below.
		if responsesAPI && responsesEventName(line) == "response.completed" {
			continue
		}
		if responsesAPI && responsesEventName(line) == "response.function_call_arguments.delta" {
			// Hold this header until its data line identifies the output item. The
			// injected output_item.added event must precede both lines as one SSE
			// event, otherwise strict clients associate the added payload with this
			// function-call header.
			pendingFunctionEventHeader = line
			continue
		}
		if len(line) > 0 {
			chatDone := !responsesAPI && sseLineIsDone(line)
			responsesHasOutput := false
			if responsesAPI {
				responsesHasOutput = sseLineHasOutputForPath(line, true)
				if responsesLineIsCreated(line) {
					responses.created = true
					responses.observeCreated(line)
				} else {
					responses.observe(line)
					var injected strings.Builder
					if !responses.created && responsesLineNeedsCreated(line) {
						injected.WriteString(responses.createdEvent())
						responses.created = true
					}
					if responsesLineIsTextDelta(line) {
						injected.WriteString(responses.ensureTextLifecycle())
						line = responses.normalizeTextDelta(line)
					}
					if responsesLineIsFunctionCallDelta(line) && strings.HasPrefix(strings.TrimSpace(line), "data:") {
						injected.WriteString(responses.ensureOutputLifecycle())
						if pendingFunctionEventHeader != "" {
							injected.WriteString(pendingFunctionEventHeader)
							pendingFunctionEventHeader = ""
						}
					}
					if responsesLineIsCompleted(line) {
						seenResponsesCompleted = true
						usage = mergeTokenUsage(usage, parseSSETokenUsage(line))
						injected.WriteString(responses.completionEvents())
						line = responses.completedEvent(usage)
					}
					line = injected.String() + line
				}
			}
			usage = mergeTokenUsage(usage, parseSSETokenUsage(line))
			hasOutput := sseLineHasOutputForPath(line, responsesAPI)
			if responsesHasOutput {
				hasOutput = true
			}
			if !responsesAPI && sseLineHasFinishReason(line) {
				seenFinishReason = true
			}
			if chatDone && !seenFinishReason {
				// Some OpenAI-compatible upstreams emit [DONE] without a final
				// choice carrying finish_reason. Preserve the declared completion
				// while giving strict clients a standards-compatible terminal chunk.
				line = `data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n" + line
				seenFinishReason = true
			}
			if chatDone {
				seenDone = true
			}
			if delayUntilFirstOutput && !committed {
				pending.WriteString(line)
				if hasOutput {
					firstTokenMS = time.Since(started).Milliseconds()
					if writeErr := write(pending.String()); writeErr != nil {
						return usage, firstTokenMS, false, writeErr
					}
					pending.Reset()
					committed = true
				}
			} else {
				if firstTokenMS == 0 && hasOutput {
					firstTokenMS = time.Since(started).Milliseconds()
				}
				if writeErr := write(line); writeErr != nil {
					return usage, firstTokenMS, committed, writeErr
				}
				committed = true
			}
		}
		if err == io.EOF {
			if delayUntilFirstOutput && !committed {
				return usage, firstTokenMS, false, fmt.Errorf("%w: %w", errUpstreamResponseRead, errUpstreamStreamEmpty)
			}
			if delayUntilFirstOutput && responsesAPI && !seenResponsesCompleted {
				return usage, firstTokenMS, committed, fmt.Errorf("%w: responses stream ended without response.completed", errUpstreamResponseRead)
			}
			if delayUntilFirstOutput && !responsesAPI && !seenDone {
				return usage, firstTokenMS, committed, fmt.Errorf("%w: chat stream ended without [DONE]", errUpstreamResponseRead)
			}
			return usage, firstTokenMS, committed, nil
		}
		if err != nil {
			if errors.Is(err, errUpstreamFirstOutputTimeout) {
				return usage, firstTokenMS, committed, fmt.Errorf("%w: %w", errUpstreamResponseRead, err)
			}
			return usage, firstTokenMS, committed, fmt.Errorf("%w: %w", errUpstreamResponseRead, err)
		}
	}
}

func sseLineIsDone(line string) bool {
	line = strings.TrimSpace(line)
	return strings.HasPrefix(line, "data:") && strings.TrimSpace(strings.TrimPrefix(line, "data:")) == "[DONE]"
}

func sseLineHasFinishReason(line string) bool {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "data:") {
		return false
	}
	var payload struct {
		Choices []struct {
			FinishReason json.RawMessage `json:"finish_reason"`
		} `json:"choices"`
	}
	if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &payload) != nil {
		return false
	}
	for _, choice := range payload.Choices {
		if value := strings.TrimSpace(string(choice.FinishReason)); value != "" && value != "null" && value != `""` {
			return true
		}
	}
	return false
}

func sseLineHasOutput(line string) bool {
	return sseLineHasOutputForPath(line, false) || sseLineHasOutputForPath(line, true)
}

func sseLineHasOutputForPath(line string, responsesAPI bool) bool {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "data:") {
		return false
	}
	var payload struct {
		Type    string `json:"type"`
		Delta   string `json:"delta"`
		Choices []struct {
			Delta struct {
				Content          string          `json:"content"`
				ReasoningContent string          `json:"reasoning_content"`
				Reasoning        string          `json:"reasoning"`
				ToolCalls        json.RawMessage `json:"tool_calls"`
				FunctionCall     json.RawMessage `json:"function_call"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &payload) != nil {
		return false
	}
	if responsesAPI {
		switch payload.Type {
		case "response.output_text.delta", "response.reasoning_summary_text.delta", "response.function_call_arguments.delta":
			return payload.Delta != ""
		case "response.output_item.added", "response.content_part.added", "response.function_call_arguments.done", "response.output_text.done", "response.completed", "response.failed", "response.incomplete":
			return true
		}
	}
	for _, choice := range payload.Choices {
		if choice.Delta.Content != "" || choice.Delta.ReasoningContent != "" || choice.Delta.Reasoning != "" || len(choice.Delta.ToolCalls) > 0 || len(choice.Delta.FunctionCall) > 0 {
			return true
		}
	}
	return false
}

func parseSSETokenUsage(line string) tokenUsage {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "data:") {
		return tokenUsage{}
	}
	payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
	if payload == "" || payload == "[DONE]" {
		return tokenUsage{}
	}
	return parseTokenUsage([]byte(payload))
}

func parseTokenUsage(data []byte) tokenUsage {
	type usagePayload struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
		InputTokens      int64 `json:"input_tokens"`
		OutputTokens     int64 `json:"output_tokens"`
		TotalTokens      int64 `json:"total_tokens"`
		PromptCacheHit   int64 `json:"prompt_cache_hit_tokens"`
		PromptCacheMiss  int64 `json:"prompt_cache_miss_tokens"`
		PromptDetails    struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
		InputDetails struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"input_tokens_details"`
	}
	var payload struct {
		Usage usagePayload `json:"usage"`
		Data  struct {
			Usage usagePayload `json:"usage"`
		} `json:"data"`
		Response struct {
			Usage usagePayload `json:"usage"`
		} `json:"response"`
	}
	if json.Unmarshal(data, &payload) != nil {
		return tokenUsage{}
	}
	usage := payload.Usage
	if usage.PromptTokens == 0 && usage.CompletionTokens == 0 && usage.InputTokens == 0 && usage.OutputTokens == 0 && usage.TotalTokens == 0 {
		usage = payload.Response.Usage
	}
	if usage.PromptTokens == 0 && usage.CompletionTokens == 0 && usage.InputTokens == 0 && usage.OutputTokens == 0 && usage.TotalTokens == 0 {
		usage = payload.Data.Usage
	}
	promptTokens := usage.PromptTokens
	if promptTokens == 0 {
		promptTokens = usage.InputTokens
	}
	completionTokens := usage.CompletionTokens
	if completionTokens == 0 {
		completionTokens = usage.OutputTokens
	}
	cachedTokens := usage.PromptDetails.CachedTokens
	if cachedTokens == 0 {
		cachedTokens = usage.InputDetails.CachedTokens
	}
	if cachedTokens == 0 {
		cachedTokens = usage.PromptCacheHit
	}
	if promptTokens == 0 && usage.PromptCacheHit+usage.PromptCacheMiss > 0 {
		promptTokens = usage.PromptCacheHit + usage.PromptCacheMiss
	}
	totalTokens := usage.TotalTokens
	if totalTokens == 0 && (promptTokens != 0 || completionTokens != 0) {
		totalTokens = promptTokens + completionTokens
	}
	return tokenUsage{PromptTokens: promptTokens, CompletionTokens: completionTokens, TotalTokens: totalTokens, CachedTokens: cachedTokens}
}

func mergeTokenUsage(current, next tokenUsage) tokenUsage {
	if next.PromptTokens != 0 || next.CompletionTokens != 0 || next.TotalTokens != 0 || next.CachedTokens != 0 {
		return next
	}
	return current
}
func parseRetryAfter(v string) time.Duration {
	if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
