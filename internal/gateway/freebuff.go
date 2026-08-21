package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	freeBuffSystemPrompt = "You are Buffy, the strategic coding assistant."
	freeBuffUserAgent    = "ai-sdk/openai-compatible/0.0.141/codebuff"
	freeBuffChainGap     = 300 * time.Millisecond
	freeBuffRunTTL       = 10 * time.Minute
	freeBuffModelsURL    = "https://github.com/pingmike2/freebuff2api-wokers/releases/download/models-cache/freebuff-models.json"
	freeBuffModelsTTL    = 30 * time.Minute
	freeBuffBehaviorTTL  = 30 * time.Minute
)

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}

func (g *Gateway) acquireFreeBuffAccount(ctx context.Context, model string) (*freeBuffAccount, error) {
	if len(g.freeBuffAccounts) == 0 {
		return nil, fmt.Errorf("no FreeBuff account is configured")
	}
	for {
		now := time.Now()
		start := int(g.freeBuffAccountIdx.Load() % uint64(len(g.freeBuffAccounts)))
		for pass := 0; pass < 2; pass++ {
			for offset := range g.freeBuffAccounts {
				index := (start + offset) % len(g.freeBuffAccounts)
				account := g.freeBuffAccounts[index]
				if !account.mu.TryLock() {
					continue
				}
				if now.Before(account.cooldownUntil) {
					account.mu.Unlock()
					continue
				}
				cached, hasSession := account.sessions[model]
				if pass == 0 && (!hasSession || cached.InstanceID == "" || time.Until(cached.ExpiresAt) <= time.Minute) {
					account.mu.Unlock()
					continue
				}
				g.freeBuffAccountIdx.Store(uint64(index + 1))
				return account, nil
			}
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
}

type freeBuffModelInfo struct {
	ID       string
	Session  string
	Agent    string
	Upstream string
}

type freeBuffSession struct {
	InstanceID string
	Model      string
	ExpiresAt  time.Time
}

type freeBuffRun struct {
	RunID     string
	ChildID   string
	CreatedAt time.Time
}

type freeBuffHTTPError struct {
	Status int
	Body   []byte
}

func (e *freeBuffHTTPError) Error() string {
	return fmt.Sprintf("FreeBuff upstream returned HTTP %d: %s", e.Status, strings.TrimSpace(string(e.Body)))
}

var freeBuffAgents = map[string]string{
	"mimo/mimo-v2.5":                   "base2-free-mimo",
	"minimax/minimax-m3":               "base2-free-minimax-m3",
	"openai/gpt-5.6-luna":              "base2-free-luna",
	"deepseek/deepseek-v4-pro":         "base2-free-deepseek",
	"deepseek/deepseek-v4-flash":       "base2-free-deepseek-flash",
	"z-ai/glm-5.2":                     "base2-free-glm",
	"poolside/laguna-s-2.1":            "base2-free-laguna-s-2-1",
	"openrouter/poolside/laguna-s-2.1": "base2-free-laguna-s-2-1-openrouter",
	"crof/kimi-k3-eco":                 "base2-free-kimi-k3-eco",
	"anthropic/claude-fable-5":         "base2-free-fable",
	"meta/muse-spark-1.2-contributor":  "base2-free-muse-spark",
}

func (g *Gateway) forwardFreeBuff(ctx context.Context, w http.ResponseWriter, r *http.Request, path string, body []byte, model string, streaming bool) error {
	responsesMode := path == "/v1/responses"
	if path != "/v1/chat/completions" && !responsesMode {
		g.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "freebuff_endpoint_not_supported", "message": "FreeBuff supports /v1/chat/completions and /v1/responses"})
		return nil
	}
	model = strings.TrimSpace(model)
	if !strings.Contains(model, "/") {
		g.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported_model", "message": "FreeBuff model is not in the discovered catalog"})
		return nil
	}
	modelInfo := g.freeBuffModelInfo(model)

	account, err := g.acquireFreeBuffAccount(ctx, model)
	if err != nil {
		g.writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "freebuff_accounts_cooling_down", "message": err.Error()})
		return nil
	}
	defer account.mu.Unlock()
	if wait := freeBuffChainGap - time.Since(account.lastCall); wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer func() { account.lastCall = time.Now() }()

	slot, err := g.waitForSlotExcluding(ctx, model, nil)
	if err != nil {
		http.Error(w, `{"error":"upstream_cooldown_timeout"}`, http.StatusGatewayTimeout)
		return nil
	}
	started := time.Now()
	var session freeBuffSession
	var run freeBuffRun
	for lifecycleAttempt := 0; lifecycleAttempt < 2; lifecycleAttempt++ {
		// A 409 can be returned by agent-runs when the account still has a
		// run/session owned by an older client or gateway process. Rebuild the
		// complete chain once so a stale run cannot poison subsequent requests.
		session, err = g.freeBuffSessionFor(ctx, account, slot, model, lifecycleAttempt > 0)
		if err == nil {
			run, err = g.freeBuffRunFor(ctx, account, slot, model)
		}
		if err == nil {
			break
		}
		if lifecycleAttempt == 0 && freeBuffConflictError(err) {
			g.clearFreeBuffAccountSessions(ctx, account, slot)
			_ = g.recoverFreeBuffSession(ctx, account, slot)
			continue
		}
		return g.writeFreeBuffLifecycleError(w, r, account, slot, model, started, err)
	}
	if err != nil {
		return g.writeFreeBuffLifecycleError(w, r, account, slot, model, started, err)
	}

	var response *http.Response
	for chatAttempt := 0; chatAttempt < 2; chatAttempt++ {
		payload, payloadErr := buildFreeBuffPayload(body, modelInfo.Upstream, session.InstanceID, run.RunID, account.token)
		if payloadErr != nil {
			g.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request", "message": payloadErr.Error()})
			return nil
		}
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(g.cfg.UpstreamURL, "/")+"/api/v1/chat/completions", bytes.NewReader(payload))
		if requestErr != nil {
			return requestErr
		}
		request.Header.Set("Authorization", "Bearer "+account.token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "text/event-stream")
		request.Header.Set("Accept-Encoding", "identity")
		request.Header.Set("User-Agent", freeBuffUserAgent)
		request.Header.Set("x-freebuff-instance-id", session.InstanceID)
		response, err = slot.client.Do(request)
		if err != nil {
			slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, 0)
			g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", chatAttempt+1, "")
			http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
			return err
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			break
		}
		errorBody, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if chatAttempt == 0 && (response.StatusCode == http.StatusConflict || response.StatusCode == 428) {
			if session.InstanceID != "" {
				_, _, _ = g.freeBuffJSON(ctx, account, slot, http.MethodDelete, "/api/v1/freebuff/session", nil, map[string]string{"x-freebuff-instance-id": session.InstanceID}, nil)
			}
			delete(account.sessions, model)
			session, err = g.freeBuffSessionFor(ctx, account, slot, model, true)
			if err == nil {
				continue
			}
		}
		if response.StatusCode == http.StatusTooManyRequests {
			g.stats.Upstream429.Add(1)
			retryAfter := parseRetryAfter(response.Header.Get("Retry-After"))
			slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, retryAfter)
			account.cooldownUntil = time.Now().Add(maxDuration(retryAfter, g.cfg.CooldownMax))
		}
		copyResponseHeaders(w.Header(), response.Header)
		setNoStoreResponseHeaders(w.Header())
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(errorBody)
		g.recordAudit(r, model, response.StatusCode, slot, started, "upstream", chatAttempt+1, response.Header.Get("Retry-After"))
		return nil
	}
	if response == nil {
		http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
		return nil
	}
	defer response.Body.Close()
	setNoStoreResponseHeaders(w.Header())
	if streaming && responsesMode {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		usage, firstToken, streamErr := copyFreeBuffResponsesStream(w, response.Body, started, model)
		if streamErr != nil {
			slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, 0)
			account.cooldownUntil = time.Now().Add(g.cfg.CooldownMax)
			g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", 1, "")
			return streamErr
		}
		slot.success(model)
		g.stats.Success.Add(1)
		g.recordAuditWithUsage(r, model, http.StatusOK, slot, started, "upstream", 1, "", usage, firstToken)
		return nil
	}
	if streaming {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		usage, firstToken, streamErr := copyFreeBuffStream(w, response.Body, started)
		if streamErr != nil {
			slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, 0)
			account.cooldownUntil = time.Now().Add(g.cfg.CooldownMax)
			g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", 1, "")
			return streamErr
		}
		slot.success(model)
		g.stats.Success.Add(1)
		g.recordAuditWithUsage(r, model, http.StatusOK, slot, started, "upstream", 1, "", usage, firstToken)
		return nil
	}
	var result []byte
	var usage tokenUsage
	if responsesMode {
		result, usage, err = aggregateFreeBuffResponses(response.Body, model)
	} else {
		result, usage, err = aggregateFreeBuffStream(response.Body, model)
	}
	if err != nil {
		slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, 0)
		account.cooldownUntil = time.Now().Add(g.cfg.CooldownMax)
		http.Error(w, `{"error":"empty_response_content"}`, http.StatusBadGateway)
		g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", 1, "")
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(result)
	slot.success(model)
	g.stats.Success.Add(1)
	g.recordAuditWithUsage(r, model, http.StatusOK, slot, started, "upstream", 1, "", usage, 0)
	return nil
}

func (g *Gateway) writeFreeBuffLifecycleError(w http.ResponseWriter, r *http.Request, account *freeBuffAccount, slot *proxySlot, model string, started time.Time, err error) error {
	status := http.StatusBadGateway
	var upstream *freeBuffHTTPError
	if errorsAs(err, &upstream) {
		status = upstream.Status
	}
	if status == http.StatusTooManyRequests {
		g.stats.Upstream429.Add(1)
		slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, g.cfg.CooldownMax)
		account.cooldownUntil = time.Now().Add(g.cfg.CooldownMax)
	}
	g.recordAudit(r, model, status, slot, started, "upstream", 1, "")
	g.writeJSON(w, status, map[string]string{"error": "freebuff_lifecycle_failed", "message": err.Error()})
	return nil
}

func freeBuffConflictError(err error) bool {
	var upstream *freeBuffHTTPError
	return errorsAs(err, &upstream) && upstream.Status == http.StatusConflict
}

// errorsAs is kept local so the lifecycle path does not expose upstream body
// details elsewhere in the gateway.
func errorsAs(err error, target **freeBuffHTTPError) bool {
	for err != nil {
		if typed, ok := err.(*freeBuffHTTPError); ok {
			*target = typed
			return true
		}
		type unwrapper interface{ Unwrap() error }
		unwrapped, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = unwrapped.Unwrap()
	}
	return false
}

func (g *Gateway) freeBuffSessionFor(ctx context.Context, account *freeBuffAccount, slot *proxySlot, model string, force bool) (freeBuffSession, error) {
	g.runFreeBuffNormalClientBehavior(ctx, account, slot)
	modelInfo := g.freeBuffModelInfo(model)
	sessionModel := modelInfo.Session
	if cached, exists := account.sessions[model]; !force && exists && cached.InstanceID != "" && time.Until(cached.ExpiresAt) > time.Minute {
		return cached, nil
	}
	if !force {
		current, reusable, err := g.currentFreeBuffSession(ctx, account, slot, model, sessionModel)
		if err != nil {
			return freeBuffSession{}, err
		}
		if reusable {
			account.sessions[model] = current
			return current, nil
		}
	}
	g.clearFreeBuffAccountSessions(ctx, account, slot)
	var created map[string]any
	var status int
	var raw []byte
	var err error
	var createdInstanceID string
	for attempt := 0; attempt < 2; attempt++ {
		createdInstanceID = newUUID()
		headers := map[string]string{"x-freebuff-model": sessionModel, "x-freebuff-instance-id": createdInstanceID}
		created = nil
		status, raw, err = g.freeBuffJSON(ctx, account, slot, http.MethodPost, "/api/v1/freebuff/session", nil, headers, &created)
		if err != nil {
			return freeBuffSession{}, err
		}
		if status == http.StatusOK {
			break
		}
		if attempt == 0 && status == http.StatusConflict && freeBuffSessionMismatch(raw) && g.recoverFreeBuffSession(ctx, account, slot) {
			continue
		}
		return freeBuffSession{}, &freeBuffHTTPError{Status: status, Body: raw}
	}
	if strings.EqualFold(fmt.Sprint(created["status"]), "queued") {
		queuedID := strings.TrimSpace(fmt.Sprint(created["instanceId"]))
		if queuedID == "" {
			queuedID = createdInstanceID
		}
		for range 8 {
			select {
			case <-time.After(1500 * time.Millisecond):
			case <-ctx.Done():
				return freeBuffSession{}, ctx.Err()
			}
			created = nil
			status, raw, err = g.freeBuffJSON(ctx, account, slot, http.MethodGet, "/api/v1/freebuff/session", nil, map[string]string{"x-freebuff-instance-id": queuedID}, &created)
			if err == nil && status == http.StatusOK && strings.EqualFold(fmt.Sprint(created["status"]), "active") {
				break
			}
		}
	}
	if !strings.EqualFold(fmt.Sprint(created["status"]), "active") {
		return freeBuffSession{}, &freeBuffHTTPError{Status: status, Body: raw}
	}
	session := parseFreeBuffSession(created, model)
	if session.InstanceID == "" {
		session.InstanceID = createdInstanceID
	}
	account.sessions[model] = session
	return session, nil
}

// FreeBuff allows only one active session for each account. Match the Desktop
// client lifecycle: reuse an active session for the requested session model,
// or remove a different model before creating a new session. Posting blindly
// while an active session exists returns a 409 session lifecycle error.
func (g *Gateway) currentFreeBuffSession(ctx context.Context, account *freeBuffAccount, slot *proxySlot, model, sessionModel string) (freeBuffSession, bool, error) {
	var current map[string]any
	status, _, err := g.freeBuffJSON(ctx, account, slot, http.MethodGet, "/api/v1/freebuff/session", nil, map[string]string{"x-freebuff-include-unused-rate-limits": "1"}, &current)
	if err != nil {
		return freeBuffSession{}, false, err
	}
	instanceID := strings.TrimSpace(fmt.Sprint(current["instanceId"]))
	if status != http.StatusOK || !strings.EqualFold(fmt.Sprint(current["status"]), "active") || instanceID == "" {
		return freeBuffSession{}, false, nil
	}
	currentModel := strings.TrimSpace(fmt.Sprint(current["model"]))
	if currentModel == "" || currentModel == sessionModel {
		session := parseFreeBuffSession(current, model)
		session.InstanceID = instanceID
		return session, true, nil
	}
	// A failed delete will be surfaced by the following POST as a 409, where
	// the explicit conflict recovery path performs one more cleanup attempt.
	_, _, _ = g.freeBuffJSON(ctx, account, slot, http.MethodDelete, "/api/v1/freebuff/session", nil, map[string]string{"x-freebuff-instance-id": instanceID}, nil)
	return freeBuffSession{}, false, nil
}

func freeBuffSessionMismatch(body []byte) bool {
	return strings.Contains(strings.ToLower(string(body)), "session_model_mismatch")
}

// A mismatch means this dedicated account still has a session created by an
// older gateway process or a previous deployment. Recover only on that
// explicit error; normal requests never inspect or remove unknown sessions.
func (g *Gateway) recoverFreeBuffSession(ctx context.Context, account *freeBuffAccount, slot *proxySlot) bool {
	var current map[string]any
	status, _, err := g.freeBuffJSON(ctx, account, slot, http.MethodGet, "/api/v1/freebuff/session", nil, map[string]string{"x-freebuff-include-unused-rate-limits": "1"}, &current)
	if err != nil || status != http.StatusOK {
		return false
	}
	instanceID := strings.TrimSpace(fmt.Sprint(current["instanceId"]))
	if instanceID == "" {
		return false
	}
	deleteHeaders := map[string]string{"x-freebuff-instance-id": instanceID}
	deleteStatus, _, deleteErr := g.freeBuffJSON(ctx, account, slot, http.MethodDelete, "/api/v1/freebuff/session", nil, deleteHeaders, nil)
	return deleteErr == nil && deleteStatus >= 200 && deleteStatus < 300
}

// FreeBuff permits one active session per account. Only entries created by
// this gateway are removed; unknown upstream sessions are never inspected.
func (g *Gateway) clearFreeBuffAccountSessions(ctx context.Context, account *freeBuffAccount, slot *proxySlot) {
	for model, cached := range account.sessions {
		if cached.InstanceID != "" {
			_, _, _ = g.freeBuffJSON(ctx, account, slot, http.MethodDelete, "/api/v1/freebuff/session", nil, map[string]string{"x-freebuff-instance-id": cached.InstanceID}, nil)
		}
		delete(account.sessions, model)
	}
	account.runs = make(map[string]freeBuffRun)
}

// FreeBuff's official client performs a lightweight ad/usage handshake before
// creating a free session. Failures are deliberately non-fatal: the session
// endpoint remains the authority for access and quota decisions.
func (g *Gateway) runFreeBuffNormalClientBehavior(ctx context.Context, account *freeBuffAccount, slot *proxySlot) {
	if time.Since(account.behaviorAt) < freeBuffBehaviorTTL {
		return
	}
	account.behaviorAt = time.Now()

	fingerprint := freeBuffFingerprint(account.token, "behavior")
	adHeaders := map[string]string{"User-Agent": "Freebuff-CLI/0.0.138"}
	var ad struct {
		Ads []struct {
			ImpURL string `json:"impUrl"`
		} `json:"ads"`
	}
	status, _, err := g.freeBuffJSON(ctx, account, slot, http.MethodPost, "/api/v1/ads", map[string]any{
		"provider":  "gravity",
		"sessionId": newUUID(),
		"surface":   "waiting_room",
		"device":    map[string]string{"os": "linux", "timezone": "Asia/Shanghai", "locale": "zh-CN"},
		"userAgent": "Freebuff-CLI/0.0.138",
	}, adHeaders, &ad)
	if err == nil && status == http.StatusOK && len(ad.Ads) > 0 && strings.TrimSpace(ad.Ads[0].ImpURL) != "" {
		_, _, _ = g.freeBuffJSON(ctx, account, slot, http.MethodPost, "/api/v1/ads/impression", map[string]string{"impUrl": ad.Ads[0].ImpURL, "mode": "free"}, adHeaders, nil)
	}
	_, _, _ = g.freeBuffJSON(ctx, account, slot, http.MethodPost, "/api/v1/usage", map[string]string{"fingerprintId": fingerprint}, nil, nil)
}

func parseFreeBuffSession(payload map[string]any, model string) freeBuffSession {
	expires := time.Now().Add(55 * time.Minute)
	if raw := strings.TrimSpace(fmt.Sprint(payload["expiresAt"])); raw != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			expires = parsed
		}
	}
	return freeBuffSession{InstanceID: strings.TrimSpace(fmt.Sprint(payload["instanceId"])), Model: model, ExpiresAt: expires}
}

func (g *Gateway) freeBuffRunFor(ctx context.Context, account *freeBuffAccount, slot *proxySlot, model string) (freeBuffRun, error) {
	agent := g.freeBuffModelInfo(model).Agent
	if cached, exists := account.runs[agent]; exists && time.Since(cached.CreatedAt) < freeBuffRunTTL {
		return cached, nil
	}
	runID, err := g.startFreeBuffRun(ctx, account, slot, agent, nil)
	if err != nil {
		return freeBuffRun{}, err
	}
	childID, err := g.startFreeBuffRun(ctx, account, slot, "context-pruner", []string{runID})
	if err != nil {
		return freeBuffRun{}, err
	}
	run := freeBuffRun{RunID: runID, ChildID: childID, CreatedAt: time.Now()}
	account.runs[agent] = run
	return run, nil
}

func (g *Gateway) freeBuffModelInfo(model string) freeBuffModelInfo {
	model = strings.TrimSpace(model)
	g.refreshFreeBuffModelMap()
	g.freeBuffModelMu.RLock()
	info, exists := g.freeBuffModels[model]
	g.freeBuffModelMu.RUnlock()
	if exists {
		return info
	}
	return freeBuffModelInfo{ID: model, Session: model, Agent: freeBuffAgent(model), Upstream: model}
}

func (g *Gateway) refreshFreeBuffModelMap() {
	if g.cfg.UpstreamProvider != ProviderFreeBuff || strings.TrimRight(g.cfg.UpstreamURL, "/") != freeBuffAPIURL {
		return
	}
	g.freeBuffModelMu.RLock()
	fresh := time.Since(g.freeBuffModelsAt) < freeBuffModelsTTL
	g.freeBuffModelMu.RUnlock()
	if fresh {
		return
	}
	client := *g.client
	client.Timeout = 5 * time.Second
	for _, sourceURL := range g.cfg.FreeBuffModelsURLs {
		response, err := client.Get(sourceURL)
		if err != nil {
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			response.Body.Close()
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, 2<<20))
		response.Body.Close()
		if readErr != nil {
			continue
		}
		models, parseErr := parseFreeBuffModelMap(data)
		if parseErr != nil || len(models) == 0 {
			continue
		}
		g.freeBuffModelMu.Lock()
		g.freeBuffModels = models
		g.freeBuffModelsAt = time.Now()
		g.freeBuffModelMu.Unlock()
		return
	}
}

func parseFreeBuffModelMap(data []byte) (map[string]freeBuffModelInfo, error) {
	var payload struct {
		Models []struct {
			ID       string `json:"id"`
			Session  string `json:"session"`
			Agent    string `json:"agent"`
			Upstream string `json:"upstream"`
		} `json:"models"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	result := make(map[string]freeBuffModelInfo, len(payload.Models))
	for _, item := range payload.Models {
		id := strings.TrimSpace(item.ID)
		if id == "" {
			continue
		}
		session := strings.TrimSpace(item.Session)
		if session == "" {
			session = id
		}
		agent := strings.TrimSpace(item.Agent)
		if agent == "" {
			agent = freeBuffAgent(id)
		}
		upstream := strings.TrimSpace(item.Upstream)
		if upstream == "" {
			upstream = id
		}
		result[id] = freeBuffModelInfo{ID: id, Session: session, Agent: agent, Upstream: upstream}
	}
	return result, nil
}

func freeBuffAgent(model string) string {
	if agent := freeBuffAgents[model]; agent != "" {
		return agent
	}
	parts := strings.Split(strings.ToLower(strings.TrimSpace(model)), "/")
	name := parts[len(parts)-1]
	name = strings.NewReplacer(".", "-", "_", "-", " ", "-").Replace(name)
	return "base2-free-" + name
}

func (g *Gateway) startFreeBuffRun(ctx context.Context, account *freeBuffAccount, slot *proxySlot, agent string, ancestors []string) (string, error) {
	if ancestors == nil {
		ancestors = []string{}
	}
	var result map[string]any
	status, raw, err := g.freeBuffJSON(ctx, account, slot, http.MethodPost, "/api/v1/agent-runs", map[string]any{"action": "START", "agentId": agent, "ancestorRunIds": ancestors}, nil, &result)
	if err != nil {
		return "", err
	}
	runID := strings.TrimSpace(fmt.Sprint(result["runId"]))
	if status != http.StatusOK || runID == "" {
		return "", &freeBuffHTTPError{Status: status, Body: raw}
	}
	return runID, nil
}

func (g *Gateway) freeBuffJSON(ctx context.Context, account *freeBuffAccount, slot *proxySlot, method, path string, payload any, headers map[string]string, out any) (int, []byte, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(g.cfg.UpstreamURL, "/")+path, body)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+account.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("User-Agent", freeBuffUserAgent)
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := slot.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return response.StatusCode, raw, err
	}
	if out != nil && len(raw) > 0 {
		var wrapped struct {
			Data json.RawMessage `json:"data"`
		}
		decode := raw
		if json.Unmarshal(raw, &wrapped) == nil && len(wrapped.Data) > 0 && string(wrapped.Data) != "null" {
			decode = wrapped.Data
		}
		if err := json.Unmarshal(decode, out); err != nil && response.StatusCode >= 200 && response.StatusCode < 300 {
			return response.StatusCode, raw, err
		}
	}
	return response.StatusCode, raw, nil
}

func buildFreeBuffPayload(body []byte, model, instanceID, runID, token string) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("request body must be a JSON object")
	}
	payload["model"] = model
	payload["stream"] = true
	payload["provider"] = map[string]string{"data_collection": "deny"}
	if _, exists := payload["stop"]; !exists {
		payload["stop"] = []string{`"cb_easp"`}
	}
	payload["messages"] = normalizeFreeBuffMessages(payload["messages"])
	payload["codebuff_metadata"] = map[string]any{
		"freebuff_instance_id": instanceID,
		"trace_session_id":     newUUID(),
		"run_id":               runID,
		"client_id":            freeBuffFingerprint(token, runID),
		"cost_mode":            "free",
	}
	if tools, ok := payload["tools"].([]any); ok && len(tools) > 0 && !hasFreeBuffToolSignature(tools) {
		payload["tools"] = append(tools, map[string]any{"type": "function", "function": map[string]any{"name": "end_turn", "description": "Signal the end of the current task.", "parameters": map[string]any{"type": "object", "properties": map[string]any{}}}})
	}
	return json.Marshal(payload)
}

func normalizeFreeBuffMessages(raw any) []any {
	messages, _ := raw.([]any)
	result := make([]any, 0, len(messages)+1)
	hasSystem := false
	for _, value := range messages {
		message, ok := value.(map[string]any)
		if !ok {
			continue
		}
		copy := make(map[string]any, len(message)+1)
		for key, item := range message {
			copy[key] = item
		}
		role, _ := copy["role"].(string)
		if role == "developer" {
			role = "system"
			copy["role"] = role
		}
		if role == "system" {
			hasSystem = true
			copy["cache_control"] = map[string]string{"type": "ephemeral"}
			if content, ok := copy["content"].(string); ok && !strings.HasPrefix(content, freeBuffSystemPrompt) {
				copy["content"] = freeBuffSystemPrompt + content
			}
		}
		result = append(result, copy)
	}
	if !hasSystem {
		result = append([]any{map[string]any{"role": "system", "content": freeBuffSystemPrompt, "cache_control": map[string]string{"type": "ephemeral"}}}, result...)
	}
	return result
}

func hasFreeBuffToolSignature(tools []any) bool {
	for _, raw := range tools {
		tool, _ := raw.(map[string]any)
		function, _ := tool["function"].(map[string]any)
		if function["name"] == "end_turn" {
			return true
		}
	}
	return false
}

func freeBuffFingerprint(token, seed string) string {
	sum := sha256.Sum256([]byte("freebuff-fp-v2:" + token + ":" + seed))
	return "enhanced-" + hex.EncodeToString(sum[:8])
}

func unwrapFreeBuffEvent(payload []byte) ([]byte, map[string]any, bool) {
	var value map[string]any
	if json.Unmarshal(payload, &value) != nil {
		return payload, nil, false
	}
	if nested, ok := value["data"].(map[string]any); ok {
		if _, choices := nested["choices"]; choices {
			value = nested
		} else if _, usage := nested["usage"]; usage {
			value = nested
		}
	}
	normalized, err := json.Marshal(value)
	return normalized, value, err == nil
}

func copyFreeBuffStream(w http.ResponseWriter, src io.Reader, started time.Time) (tokenUsage, int64, error) {
	reader := bufio.NewScanner(src)
	reader.Buffer(make([]byte, 64<<10), maxBufferedResponseBytes)
	flusher, _ := w.(http.Flusher)
	var usage tokenUsage
	var firstToken int64
	seenOutput := false
	for reader.Scan() {
		line := reader.Text()
		if strings.HasPrefix(line, "data:") {
			data := bytes.TrimSpace([]byte(strings.TrimPrefix(line, "data:")))
			if string(data) != "" && string(data) != "[DONE]" {
				if normalized, value, ok := unwrapFreeBuffEvent(data); ok {
					data = normalized
					encoded, _ := json.Marshal(value)
					if parsed := parseTokenUsage(encoded); parsed.TotalTokens > 0 {
						usage = parsed
					}
					if !seenOutput && freeBuffEventHasOutput(value) {
						seenOutput = true
						firstToken = time.Since(started).Milliseconds()
					}
				}
				line = "data: " + string(data)
			}
		}
		if _, err := io.WriteString(w, line+"\n"); err != nil {
			return usage, firstToken, err
		}
		if line == "" && flusher != nil {
			flusher.Flush()
		}
	}
	if err := reader.Err(); err != nil {
		return usage, firstToken, err
	}
	if !seenOutput {
		return usage, firstToken, errUpstreamStreamEmpty
	}
	return usage, firstToken, nil
}

func freeBuffEventHasOutput(value map[string]any) bool {
	choices, _ := value["choices"].([]any)
	if len(choices) == 0 {
		return false
	}
	choice, _ := choices[0].(map[string]any)
	delta, _ := choice["delta"].(map[string]any)
	return strings.TrimSpace(fmt.Sprint(delta["content"])) != "" || strings.TrimSpace(fmt.Sprint(delta["reasoning_content"])) != "" || delta["tool_calls"] != nil
}

func aggregateFreeBuffStream(src io.Reader, model string) ([]byte, tokenUsage, error) {
	reader := bufio.NewScanner(src)
	reader.Buffer(make([]byte, 64<<10), maxBufferedResponseBytes)
	var content, reasoning, id string
	finishReason := "stop"
	var usage tokenUsage
	for reader.Scan() {
		line := reader.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := bytes.TrimSpace([]byte(strings.TrimPrefix(line, "data:")))
		if len(data) == 0 || string(data) == "[DONE]" {
			continue
		}
		_, value, ok := unwrapFreeBuffEvent(data)
		if !ok {
			continue
		}
		if valueID, ok := value["id"].(string); ok && valueID != "" {
			id = valueID
		}
		encoded, _ := json.Marshal(value)
		if parsed := parseTokenUsage(encoded); parsed.TotalTokens > 0 {
			usage = parsed
		}
		choices, _ := value["choices"].([]any)
		if len(choices) == 0 {
			continue
		}
		choice, _ := choices[0].(map[string]any)
		delta, _ := choice["delta"].(map[string]any)
		if text, ok := delta["content"].(string); ok {
			content += text
		}
		if text, ok := delta["reasoning_content"].(string); ok {
			reasoning += text
		}
		if reason, ok := choice["finish_reason"].(string); ok && reason != "" {
			finishReason = reason
		}
	}
	if err := reader.Err(); err != nil {
		return nil, usage, err
	}
	if content == "" && reasoning == "" {
		return nil, usage, errUpstreamStreamEmpty
	}
	message := map[string]any{"role": "assistant", "content": content}
	if reasoning != "" {
		message["reasoning_content"] = reasoning
		if content == "" {
			message["content"] = reasoning
			message["reasoning_used_as_content"] = true
		}
	}
	result := map[string]any{
		"id": id, "object": "chat.completion", "created": time.Now().Unix(), "model": model,
		"choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finishReason, "logprobs": nil}},
		"usage":   map[string]int64{"prompt_tokens": usage.PromptTokens, "completion_tokens": usage.CompletionTokens, "total_tokens": usage.TotalTokens},
	}
	if result["id"] == "" {
		result["id"] = "chatcmpl_" + strings.ReplaceAll(newUUID(), "-", "")
	}
	encoded, err := json.Marshal(result)
	return encoded, usage, err
}

type freeBuffToolCall struct {
	ID          string
	CallID      string
	Name        string
	Arguments   string
	OutputIndex int
}

func aggregateFreeBuffResponses(src io.Reader, model string) ([]byte, tokenUsage, error) {
	reader := bufio.NewScanner(src)
	reader.Buffer(make([]byte, 64<<10), maxBufferedResponseBytes)
	var content, reasoning, upstreamModel string
	var usage tokenUsage
	tools := make(map[int]*freeBuffToolCall)
	for reader.Scan() {
		line := reader.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := bytes.TrimSpace([]byte(strings.TrimPrefix(line, "data:")))
		if len(data) == 0 || string(data) == "[DONE]" {
			continue
		}
		_, value, ok := unwrapFreeBuffEvent(data)
		if !ok {
			continue
		}
		if current, ok := value["model"].(string); ok && current != "" {
			upstreamModel = current
		}
		encoded, _ := json.Marshal(value)
		if parsed := parseTokenUsage(encoded); parsed.TotalTokens > 0 {
			usage = parsed
		}
		choices, _ := value["choices"].([]any)
		if len(choices) == 0 {
			continue
		}
		choice, _ := choices[0].(map[string]any)
		delta, _ := choice["delta"].(map[string]any)
		if text, ok := delta["content"].(string); ok {
			content += text
		}
		if text, ok := delta["reasoning_content"].(string); ok {
			reasoning += text
		}
		collectFreeBuffToolCalls(tools, delta["tool_calls"])
	}
	if err := reader.Err(); err != nil {
		return nil, usage, err
	}
	if content == "" && reasoning != "" {
		content = reasoning
	}
	output := make([]any, 0, 1+len(tools))
	if content != "" {
		output = append(output, map[string]any{
			"id": "msg_" + strings.ReplaceAll(newUUID(), "-", ""), "type": "message", "status": "completed", "role": "assistant",
			"content": []any{map[string]any{"type": "output_text", "text": content, "annotations": []any{}}},
		})
	}
	indexes := sortedFreeBuffToolIndexes(tools)
	for _, index := range indexes {
		tool := tools[index]
		output = append(output, map[string]any{"id": tool.ID, "type": "function_call", "status": "completed", "call_id": tool.CallID, "name": tool.Name, "arguments": tool.Arguments})
	}
	if len(output) == 0 {
		return nil, usage, errUpstreamStreamEmpty
	}
	if upstreamModel == "" {
		upstreamModel = model
	}
	result := map[string]any{
		"id": "resp_" + strings.ReplaceAll(newUUID(), "-", ""), "object": "response", "created_at": time.Now().Unix(), "status": "completed", "model": upstreamModel,
		"output": output, "error": nil, "incomplete_details": nil, "instructions": nil, "max_output_tokens": nil, "parallel_tool_calls": true,
		"previous_response_id": nil, "reasoning": map[string]any{}, "store": true, "temperature": 1, "tool_choice": "auto", "tools": []any{}, "top_p": 1,
		"truncation": "disabled", "user": nil, "metadata": map[string]any{},
		"usage": map[string]any{"input_tokens": usage.PromptTokens, "output_tokens": usage.CompletionTokens, "total_tokens": usage.TotalTokens, "input_tokens_details": map[string]any{"cached_tokens": usage.CachedTokens}},
	}
	encoded, err := json.Marshal(result)
	return encoded, usage, err
}

func collectFreeBuffToolCalls(target map[int]*freeBuffToolCall, raw any) {
	items, _ := raw.([]any)
	for _, value := range items {
		item, _ := value.(map[string]any)
		index := 0
		switch number := item["index"].(type) {
		case float64:
			index = int(number)
		case int:
			index = number
		}
		tool := target[index]
		function, _ := item["function"].(map[string]any)
		if tool == nil {
			id, _ := item["id"].(string)
			if id == "" {
				id = "call_" + strings.ReplaceAll(newUUID(), "-", "")
			}
			tool = &freeBuffToolCall{ID: "fc_" + strings.ReplaceAll(newUUID(), "-", ""), CallID: id, OutputIndex: index}
			target[index] = tool
		}
		if name, ok := function["name"].(string); ok && name != "" {
			tool.Name = name
		}
		if arguments, ok := function["arguments"].(string); ok {
			tool.Arguments += arguments
		}
	}
}

func sortedFreeBuffToolIndexes(tools map[int]*freeBuffToolCall) []int {
	indexes := make([]int, 0, len(tools))
	for index := range tools {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	return indexes
}

func copyFreeBuffResponsesStream(w http.ResponseWriter, src io.Reader, started time.Time, model string) (tokenUsage, int64, error) {
	reader := bufio.NewScanner(src)
	reader.Buffer(make([]byte, 64<<10), maxBufferedResponseBytes)
	flusher, _ := w.(http.Flusher)
	responseID := "resp_" + strings.ReplaceAll(newUUID(), "-", "")
	messageID := "msg_" + strings.ReplaceAll(newUUID(), "-", "")
	createdAt := started.Unix()
	var content, reasoning, upstreamModel string
	var usage tokenUsage
	var firstToken int64
	messageStarted := false
	tools := make(map[int]*freeBuffToolCall)
	nextOutputIndex := 0
	writeEvent := func(payload any) error {
		encoded, _ := json.Marshal(payload)
		if _, err := io.WriteString(w, "data: "+string(encoded)+"\n\n"); err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}
	base := func(status string, output []any, includeUsage bool) map[string]any {
		result := map[string]any{
			"id": responseID, "object": "response", "created_at": createdAt, "status": status, "model": model, "output": output,
			"error": nil, "incomplete_details": nil, "instructions": nil, "max_output_tokens": nil, "parallel_tool_calls": true,
			"previous_response_id": nil, "reasoning": map[string]any{}, "store": true, "temperature": 1, "tool_choice": "auto", "tools": []any{},
			"top_p": 1, "truncation": "disabled", "user": nil, "metadata": map[string]any{}, "usage": nil,
		}
		if includeUsage {
			result["usage"] = map[string]any{"input_tokens": usage.PromptTokens, "output_tokens": usage.CompletionTokens, "total_tokens": usage.TotalTokens, "input_tokens_details": map[string]any{"cached_tokens": usage.CachedTokens}}
		}
		return result
	}
	if err := writeEvent(map[string]any{"type": "response.created", "response": base("in_progress", []any{}, false)}); err != nil {
		return usage, 0, err
	}
	if err := writeEvent(map[string]any{"type": "response.in_progress", "response": base("in_progress", []any{}, false)}); err != nil {
		return usage, 0, err
	}
	for reader.Scan() {
		line := reader.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := bytes.TrimSpace([]byte(strings.TrimPrefix(line, "data:")))
		if len(data) == 0 || string(data) == "[DONE]" {
			continue
		}
		_, value, ok := unwrapFreeBuffEvent(data)
		if !ok {
			continue
		}
		if current, ok := value["model"].(string); ok && current != "" {
			upstreamModel = current
		}
		encoded, _ := json.Marshal(value)
		if parsed := parseTokenUsage(encoded); parsed.TotalTokens > 0 {
			usage = parsed
		}
		choices, _ := value["choices"].([]any)
		if len(choices) == 0 {
			continue
		}
		choice, _ := choices[0].(map[string]any)
		delta, _ := choice["delta"].(map[string]any)
		if text, ok := delta["reasoning_content"].(string); ok {
			reasoning += text
			if firstToken == 0 && text != "" {
				firstToken = time.Since(started).Milliseconds()
			}
		}
		if rawTools, ok := delta["tool_calls"].([]any); ok {
			for _, raw := range rawTools {
				item, _ := raw.(map[string]any)
				index := 0
				if number, ok := item["index"].(float64); ok {
					index = int(number)
				}
				tool := tools[index]
				function, _ := item["function"].(map[string]any)
				if tool == nil {
					callID, _ := item["id"].(string)
					if callID == "" {
						callID = "call_" + strings.ReplaceAll(newUUID(), "-", "")
					}
					name, _ := function["name"].(string)
					tool = &freeBuffToolCall{ID: "fc_" + strings.ReplaceAll(newUUID(), "-", ""), CallID: callID, Name: name, OutputIndex: nextOutputIndex}
					nextOutputIndex++
					tools[index] = tool
					if err := writeEvent(map[string]any{"type": "response.output_item.added", "output_index": tool.OutputIndex, "item": map[string]any{"id": tool.ID, "type": "function_call", "status": "in_progress", "call_id": tool.CallID, "name": tool.Name, "arguments": ""}}); err != nil {
						return usage, firstToken, err
					}
				}
				if name, ok := function["name"].(string); ok && name != "" {
					tool.Name = name
				}
				if arguments, ok := function["arguments"].(string); ok && arguments != "" {
					tool.Arguments += arguments
					if err := writeEvent(map[string]any{"type": "response.function_call_arguments.delta", "item_id": tool.ID, "output_index": tool.OutputIndex, "delta": arguments}); err != nil {
						return usage, firstToken, err
					}
				}
				if firstToken == 0 {
					firstToken = time.Since(started).Milliseconds()
				}
			}
		}
		if text, ok := delta["content"].(string); ok && text != "" {
			if !messageStarted {
				messageStarted = true
				if err := writeEvent(map[string]any{"type": "response.output_item.added", "output_index": nextOutputIndex, "item": map[string]any{"id": messageID, "type": "message", "status": "in_progress", "role": "assistant", "content": []any{}}}); err != nil {
					return usage, firstToken, err
				}
				if err := writeEvent(map[string]any{"type": "response.content_part.added", "item_id": messageID, "output_index": nextOutputIndex, "content_index": 0, "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}}); err != nil {
					return usage, firstToken, err
				}
			}
			content += text
			if firstToken == 0 {
				firstToken = time.Since(started).Milliseconds()
			}
			if err := writeEvent(map[string]any{"type": "response.output_text.delta", "item_id": messageID, "output_index": nextOutputIndex, "content_index": 0, "delta": text}); err != nil {
				return usage, firstToken, err
			}
		}
	}
	if err := reader.Err(); err != nil {
		return usage, firstToken, err
	}
	if !messageStarted && content == "" && reasoning != "" {
		messageStarted = true
		content = reasoning
		if err := writeEvent(map[string]any{"type": "response.output_item.added", "output_index": nextOutputIndex, "item": map[string]any{"id": messageID, "type": "message", "status": "in_progress", "role": "assistant", "content": []any{}}}); err != nil {
			return usage, firstToken, err
		}
		if err := writeEvent(map[string]any{"type": "response.content_part.added", "item_id": messageID, "output_index": nextOutputIndex, "content_index": 0, "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}}); err != nil {
			return usage, firstToken, err
		}
		if err := writeEvent(map[string]any{"type": "response.output_text.delta", "item_id": messageID, "output_index": nextOutputIndex, "content_index": 0, "delta": content}); err != nil {
			return usage, firstToken, err
		}
	}
	output := make([]any, 0, len(tools)+1)
	for _, index := range sortedFreeBuffToolIndexes(tools) {
		tool := tools[index]
		item := map[string]any{"id": tool.ID, "type": "function_call", "status": "completed", "call_id": tool.CallID, "name": tool.Name, "arguments": tool.Arguments}
		if err := writeEvent(map[string]any{"type": "response.function_call_arguments.done", "item_id": tool.ID, "output_index": tool.OutputIndex, "arguments": tool.Arguments}); err != nil {
			return usage, firstToken, err
		}
		if err := writeEvent(map[string]any{"type": "response.output_item.done", "output_index": tool.OutputIndex, "item": item}); err != nil {
			return usage, firstToken, err
		}
		output = append(output, item)
	}
	if messageStarted {
		part := map[string]any{"type": "output_text", "text": content, "annotations": []any{}}
		if err := writeEvent(map[string]any{"type": "response.output_text.done", "item_id": messageID, "output_index": nextOutputIndex, "content_index": 0, "text": content}); err != nil {
			return usage, firstToken, err
		}
		if err := writeEvent(map[string]any{"type": "response.content_part.done", "item_id": messageID, "output_index": nextOutputIndex, "content_index": 0, "part": part}); err != nil {
			return usage, firstToken, err
		}
		item := map[string]any{"id": messageID, "type": "message", "status": "completed", "role": "assistant", "content": []any{part}}
		if err := writeEvent(map[string]any{"type": "response.output_item.done", "output_index": nextOutputIndex, "item": item}); err != nil {
			return usage, firstToken, err
		}
		output = append(output, item)
	}
	if len(output) == 0 {
		return usage, firstToken, errUpstreamStreamEmpty
	}
	if upstreamModel != "" {
		model = upstreamModel
	}
	completed := base("completed", output, true)
	completed["model"] = model
	if err := writeEvent(map[string]any{"type": "response.completed", "response": completed}); err != nil {
		return usage, firstToken, err
	}
	return usage, firstToken, nil
}
