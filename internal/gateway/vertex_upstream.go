package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dualroute-gateway/internal/config"
	"dualroute-gateway/internal/transform"
	"dualroute-gateway/internal/vertexai"
)

// Vertex calls Google's anonymous console endpoint through a TLS-fingerprinted
// client plus reCAPTCHA tokens (logic ported from the vproxy project). Each
// attempt binds to one proxy-slot exit; slot rotation and cooldowns reuse the
// standard gateway machinery, exactly like the OpenCode provider.

const (
	vertexStreamIdleTimeoutSeconds = 300
	vertexInternalMaxRetries       = 1
	vertexExitRestTTL              = 30 * time.Second
)

var (
	vertexOnce        sync.Once
	vertexClient      *vertexai.VertexAIClient
	vertexReqConv     transform.RequestConverter
	vertexRespConv    transform.ResponseConverter
	vertexConfigCache config.Static
)

func vertexStack(cfg Config) (*vertexai.VertexAIClient, transform.RequestConverter, transform.ResponseConverter) {
	vertexOnce.Do(func() {
		timeoutSeconds := int(cfg.RequestTimeout / time.Second)
		if timeoutSeconds <= 0 {
			timeoutSeconds = 300
		}
		retries := cfg.MaxRetries
		if retries > vertexInternalMaxRetries {
			retries = vertexInternalMaxRetries
		}
		vertexConfigCache = config.Static{
			Retries:            retries,
			TimeoutSeconds:     timeoutSeconds,
			IdleTimeoutSeconds: vertexStreamIdleTimeoutSeconds,
		}
		vertexClient = vertexai.NewVertexAIClient(vertexConfigCache)
		vertexReqConv = transform.DefaultRequestConverter()
		vertexRespConv = transform.DefaultResponseConverter()
	})
	return vertexClient, vertexReqConv, vertexRespConv
}

type vertexSSEWriter struct {
	w           http.ResponseWriter
	contentType string
	written     bool
	flush       func()
}

func newVertexSSEWriter(w http.ResponseWriter) *vertexSSEWriter {
	sw := &vertexSSEWriter{w: w, contentType: "text/event-stream"}
	if flusher, ok := w.(http.Flusher); ok {
		sw.flush = flusher.Flush
	}
	return sw
}

func (s *vertexSSEWriter) hasWritten() bool { return s.written }

func (s *vertexSSEWriter) header() {
	if !s.written {
		s.written = true
		s.w.Header().Set("Content-Type", s.contentType)
		s.w.Header().Set("Cache-Control", "no-cache")
		s.w.Header().Set("Connection", "keep-alive")
		s.w.WriteHeader(http.StatusOK)
	}
}

func (s *vertexSSEWriter) write(event string) bool {
	s.header()
	if _, err := s.w.Write([]byte(event)); err != nil {
		return false
	}
	if s.flush != nil {
		s.flush()
	}
	return true
}

func vertexSSEEvent(obj map[string]any) string {
	data, err := vertexMarshal(obj)
	if err != nil {
		return "data: {}\n\n"
	}
	return "data: " + string(data) + "\n\n"
}

func vertexChunkBase(model, requestID string) map[string]any {
	return map[string]any{
		"id":      "chatcmpl-" + requestID,
		"object":  "chat.completion.chunk",
		"created": time.Now().Unix(),
		"model":   model,
	}
}

func vertexErrorToOAI(e *vertexai.VertexError) map[string]any {
	var errType string
	switch e.Kind {
	case "invalid":
		errType = "invalid_request_error"
	case "ratelimit":
		errType = "rate_limit_error"
	case "auth":
		errType = "server_error"
	case "notfound", "permission":
		errType = "invalid_request_error"
	default:
		errType = "server_error"
	}
	return map[string]any{"error": map[string]any{
		"message": vertexWithUpstreamDetail(vertexai.FriendlyErrorMessage(e), e),
		"type":    errType,
		"code":    e.Code,
	}}
}

func vertexWithUpstreamDetail(friendly string, e *vertexai.VertexError) string {
	detail := strings.TrimSpace(e.Message)
	if detail == "" {
		detail = strings.TrimSpace(e.UpstreamResponse)
	}
	if detail == "" || strings.Contains(friendly, detail) {
		return friendly
	}
	if r := []rune(detail); len(r) > 400 {
		detail = string(r[:400]) + "…"
	}
	return friendly + "（上游原因：" + detail + "）"
}

func vertexToError(err error) *vertexai.VertexError {
	var ve *vertexai.VertexError
	if errors.As(err, &ve) {
		return ve
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return vertexai.NewContextError(err)
	}
	return vertexai.NewInternalError(err.Error(), err)
}

func vertexIsSafetyBlock(e *vertexai.VertexError) bool {
	if e == nil {
		return false
	}
	if e.Kind == "safety" {
		return true
	}
	msg := strings.ToLower(e.Message)
	status := strings.ToLower(e.Status)
	for _, k := range []string{"safety", "block_reason", "content_filter", "finish_reason_safety"} {
		if strings.Contains(msg, k) || strings.Contains(status, k) {
			return true
		}
	}
	return false
}

func vertexSafetyResponse(model string) map[string]any {
	return map[string]any{
		"id":      "chatcmpl-" + vertexRequestID(),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": ""},
			"finish_reason": "content_filter",
		}},
		"usage": map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
	}
}

var vertexReqCounter uint64

func vertexRequestID() string {
	var buf [12]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:])
	}
	now := time.Now().UnixNano()
	vertexReqCounter++
	var fallback [12]byte
	fallback[0] = byte(now >> 56)
	fallback[1] = byte(now >> 48)
	fallback[2] = byte(now >> 40)
	fallback[3] = byte(now >> 32)
	fallback[4] = byte(now >> 24)
	fallback[5] = byte(now >> 16)
	fallback[6] = byte(now >> 8)
	fallback[7] = byte(now)
	fallback[8] = byte(vertexReqCounter >> 24)
	fallback[9] = byte(vertexReqCounter >> 16)
	fallback[10] = byte(vertexReqCounter >> 8)
	fallback[11] = byte(vertexReqCounter)
	return hex.EncodeToString(fallback[:])
}

func vertexMarshal(v any) ([]byte, error) { return json.Marshal(v) }

func vertexUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func vertexWriteError(w http.ResponseWriter, status int, msg, errType string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":{"message":` + mustQuote(msg) + `,"type":` + mustQuote(errType) + `,"code":` + strconv.Itoa(status) + `}}`))
}

func mustQuote(s string) string {
	data, err := json.Marshal(s)
	if err != nil {
		return `"` + `"`
	}
	return string(data)
}

func (g *Gateway) vertexStreamTerminalError(sw *vertexSSEWriter, e *vertexai.VertexError, requestID, model string) {
	if vertexIsSafetyBlock(e) {
		base := vertexChunkBase(model, requestID)
		base["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "content_filter"}}
		_ = sw.write(vertexSSEEvent(base))
	} else {
		base := vertexChunkBase(model, requestID)
		base["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "error"}}
		base["error"] = vertexErrorToOAI(e)["error"]
		_ = sw.write(vertexSSEEvent(base))
	}
	_ = sw.write("data: [DONE]\n\n")
}

// forwardVertex serves chat completions for ProviderVertex instances.
func (g *Gateway) forwardVertex(ctx context.Context, w http.ResponseWriter, r *http.Request, path string, body []byte, model string, streaming bool) error {
	switch {
	case strings.HasSuffix(path, ":countTokens"):
		g.vertexCountTokens(w, r, strings.TrimSuffix(strings.TrimPrefix(path, "/v1beta/models/"), ":countTokens"))
		return nil
	case path == "/v1/images/generations":
		g.vertexImagesGenerations(w, r, body)
		return nil
	case path == "/v1/images/edits":
		g.vertexImagesEditVariation(w, r, false)
		return nil
	case path == "/v1/images/variations":
		g.vertexImagesEditVariation(w, r, true)
		return nil
	case path == "/v1/audio/speech":
		g.vertexAudioSpeech(w, r, body)
		return nil
	}
	client, reqConv, respConv := vertexStack(g.cfg)
	if path != "/v1/chat/completions" && path != "/chat/completions" {
		vertexWriteError(w, http.StatusNotFound, "unknown Vertex endpoint "+path, "invalid_request_error")
		return nil
	}

	var payload map[string]any
	if err := vertexUnmarshal(body, &payload); err != nil || payload == nil {
		vertexWriteError(w, http.StatusBadRequest, "invalid JSON body", "invalid_request_error")
		return nil
	}
	n := 1
	if rawN, ok := payload["n"].(float64); ok && rawN >= 1 {
		n = int(rawN)
	}
	delete(payload, "n")
	if streaming && n > 1 {
		vertexWriteError(w, http.StatusBadRequest, "streaming supports only n=1; set stream=false or n=1 for multiple choices", "invalid_request_error")
		return nil
	}
	if model == "" {
		m, _ := payload["model"].(string)
		model = strings.TrimSpace(m)
	}
	geminiModel := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(model, "Vertex/"), "vertex/"))
	if geminiModel == "" {
		vertexWriteError(w, http.StatusBadRequest, "missing required field 'model'", "invalid_request_error")
		return nil
	}
	if strings.HasPrefix(strings.ToLower(geminiModel), "veo") {
		vertexWriteError(w, http.StatusBadRequest, "video models are not supported by the Vertex upstream", "invalid_request_error")
		return nil
	}
	// fake- 前缀（原版假流式语义）：先完整非流式生成，再切片按 SSE 推送。
	fakeStream := false
	if strings.HasPrefix(strings.ToLower(geminiModel), "fake-") {
		fakeStream = true
		geminiModel = geminiModel[len("fake-"):]
	}

	_, geminiPayload, convErr := reqConv.Convert(payload, vertexConfigCache)
	if convErr != nil {
		vertexWriteError(w, http.StatusBadRequest, "invalid argument: "+convErr.Error(), "invalid_request_error")
		return nil
	}

	requestID := vertexRequestID()
	exits, primary := g.vertexExitCandidates(geminiModel)
	g.addLog("info", "vertex race started", map[string]any{"request_id": requestID, "model": geminiModel, "exits": len(exits), "stream": streaming})
	if primary == nil {
		primary = &proxySlot{}
	}
	started := time.Now()
	if streaming {
		if fakeStream {
			g.vertexFakeStream(ctx, w, r, client, respConv, exits, primary, geminiModel, model, geminiPayload, requestID)
			return nil
		}
		g.vertexStreamAttempt(ctx, w, r, client, respConv, exits, primary, geminiModel, geminiPayload, requestID)
		return nil
	}
	var resp map[string]any
	var runErr error
	if n > 1 {
		var results []map[string]any
		results, runErr = client.CompleteChatN(ctx, exits, geminiModel, geminiPayload, n)
		if runErr == nil {
			resp = respConv.AggregateN(results, model)
		}
	} else {
		resp, runErr = client.CompleteChat(ctx, exits, geminiModel, geminiPayload)
	}
	attempts := len(exits)
	if runErr != nil {
		ve := vertexToError(runErr)
		if ve.Kind == "ratelimit" || ve.Code == 429 {
			g.stats.Upstream429.Add(1)
		}
		g.recordAudit(r, model, vertexHTTPStatus(ve), primary, started, "upstream", attempts, "")
		g.writeJSON(w, vertexHTTPStatus(ve), vertexErrorToOAI(ve))
		return nil
	}
	transform.EnsureFunctionCallIDs(resp)
	oaiResp := respConv.ToOAI(resp, model)
	g.recordAudit(r, model, http.StatusOK, primary, started, "upstream", attempts, "")
	g.writeJSON(w, http.StatusOK, oaiResp)
	return nil
}

// vertexExitCandidates collects the ready proxy-slot exits for a race,
// mirroring vproxy's healthy-node selection; empty URL denotes direct.
func (g *Gateway) vertexExitCandidates(model string) ([]string, *proxySlot) {
	now := time.Now()
	exits := make([]string, 0, 8)
	var primary *proxySlot
	skipped := 0
	for _, s := range g.snapshotSlots() {
		disabled, _ := s.readiness(model, now)
		if disabled {
			continue
		}
		// 近期被上游 429 的出口休息片刻（对应原版 RecordRateLimit(30s)）。
		if s.url != "" {
			if restUntil, ok := g.vertexExitRest.Load(s.url); ok {
				if until, ok2 := restUntil.(time.Time); ok2 && now.Before(until) {
					skipped++
					continue
				}
				g.vertexExitRest.Delete(s.url)
			}
		}
		if primary == nil {
			primary = s
		}
		exits = append(exits, s.url)
	}
	if skipped > 0 {
		g.addLog("info", "vertex exits resting after 429", map[string]any{"skipped": skipped, "racing": len(exits)})
	}
	// 健康度学习：成功率高的出口优先参赛（原版节点择优语义）。
	sort.SliceStable(exits, func(i, j int) bool {
		return g.vertexExitScore(exits[i]) > g.vertexExitScore(exits[j])
	})
	if len(exits) == 0 {
		// 全部出口都在休息：宁可全上也不拒绝服务。
		exits = exits[:0]
		g.vertexExitRest.Range(func(key, _ any) bool { g.vertexExitRest.Delete(key); return true })
		for _, s := range g.snapshotSlots() {
			exits = append(exits, s.url)
		}
		if primary == nil && len(g.slots) > 0 {
			primary = g.slots[0]
		}
	}
	return exits, primary
}

// vertexStreamAttempt races the stream across all candidate exits and relays
// the winning SSE flow to the client.
func (g *Gateway) vertexStreamAttempt(ctx context.Context, w http.ResponseWriter, r *http.Request, client *vertexai.VertexAIClient, respConv transform.ResponseConverter, exits []string, primary *proxySlot, geminiModel string, geminiPayload map[string]any, requestID string) {
	sw := newVertexSSEWriter(w)
	isFirst := true
	hasFinish := false
	gotContent := false
	streamFailed := false
	var lastErr *vertexai.VertexError
	toolCallTracker := transform.NewStreamToolCallTracker()
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.StreamChat(ctx, exits, geminiModel, geminiPayload, func(chunk vertexai.StreamChunk) bool {
			if isFirst && chunk.Err == nil {
				sw.header()
			}
			if chunk.Err != nil {
				lastErr = vertexToError(chunk.Err)
				streamFailed = true
				return false
			}
			events := respConv.StreamToSSE(chunk.Data, geminiModel, requestID, isFirst, toolCallTracker)
			isFirst = false
			for _, ev := range events {
				if strings.Contains(ev, `"finish_reason"`) && !strings.Contains(ev, `"finish_reason":null`) {
					hasFinish = true
				}
				if strings.Contains(ev, `"content":`) || strings.Contains(ev, `"tool_calls":`) || strings.Contains(ev, `"reasoning_content":`) {
					gotContent = true
				}
				if !sw.write(ev) {
					return false
				}
			}
			return true
		}, g.vertexStreamOnResult())
	}()
	<-done

	started := time.Now()
	if streamFailed {
		ve := lastErr
		if ve == nil {
			ve = vertexai.NewInternalError("stream failed")
		}
		if ve.Kind == "ratelimit" || ve.Code == 429 {
			g.stats.Upstream429.Add(1)
		}
		g.recordAudit(r, geminiModel, vertexHTTPStatus(ve), primary, started, "upstream", len(exits), "")
		if !sw.hasWritten() {
			g.writeJSON(w, vertexHTTPStatus(ve), vertexErrorToOAI(ve))
			return
		}
		g.vertexStreamTerminalError(sw, ve, requestID, geminiModel)
		return
	}
	g.recordAudit(r, geminiModel, http.StatusOK, primary, started, "upstream", len(exits), "")
	if !gotContent {
		empty := vertexai.NewEmptyResponseError("Upstream returned empty response (no content)")
		if !sw.hasWritten() {
			g.writeJSON(w, empty.Code, vertexErrorToOAI(empty))
			return
		}
		g.vertexStreamTerminalError(sw, empty, requestID, geminiModel)
		return
	}
	if !hasFinish {
		base := vertexChunkBase(geminiModel, requestID)
		base["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "length"}}
		_ = sw.write(vertexSSEEvent(base))
	}
	_ = sw.write("data: [DONE]\n\n")
}

// vertexStreamOnResult 返回流式竞速的出口结局回调。
func (g *Gateway) vertexStreamOnResult() vertexai.RaceOption[<-chan vertexai.StreamChunk] {
	return vertexai.WithOnResult[<-chan vertexai.StreamChunk](g.vertexRaceResult)
}

// vertexRaceResult 记录单个出口的请求结局；429 出口休息 30 秒。
func (g *Gateway) vertexRaceResult(uri string, err error) {
	if uri == "" {
		return
	}
	if err == nil {
		g.vertexExitRest.Delete(uri)
		return
	}
	var ve *vertexai.VertexError
	if errors.As(err, &ve) && (ve.Kind == "ratelimit" || ve.Code == 429) {
		g.vertexExitRest.Store(uri, time.Now().Add(vertexExitRestTTL))
	}
	g.vertexRecordHealth(uri, err == nil)
}

type vertexExitHealthCounters struct {
	ok  atomic.Int64
	bad atomic.Int64
}

// vertexRecordHealth 累计出口成败，支撑健康度排序（原版节点学习语义）。
func (g *Gateway) vertexRecordHealth(uri string, ok bool) {
	entry, _ := g.vertexExitHealth.LoadOrStore(uri, &vertexExitHealthCounters{})
	counters := entry.(*vertexExitHealthCounters)
	if ok {
		counters.ok.Add(1)
	} else {
		counters.bad.Add(1)
	}
}

// vertexExitScore 返回 0~1 的健康分；未知出口按 0.5 中位处理。
func (g *Gateway) vertexExitScore(uri string) float64 {
	entry, ok := g.vertexExitHealth.Load(uri)
	if !ok {
		return 0.5
	}
	counters := entry.(*vertexExitHealthCounters)
	total := counters.ok.Load() + counters.bad.Load()
	if total == 0 {
		return 0.5
	}
	return float64(counters.ok.Load()) / float64(total)
}

func vertexHTTPStatus(e *vertexai.VertexError) int {
	if e == nil {
		return http.StatusInternalServerError
	}
	if e.Code >= 400 && e.Code <= 599 {
		return e.Code
	}
	switch e.Kind {
	case "invalid":
		return http.StatusBadRequest
	case "ratelimit":
		return http.StatusTooManyRequests
	case "notfound":
		return http.StatusNotFound
	case "permission":
		return http.StatusForbidden
	default:
		return http.StatusInternalServerError
	}
}



// ---- 假流式（移植自 vproxy fakestream：先完整生成，再按 rune 切片推 SSE）----

// vertexSplitRuneChunks 把文本切成约 8 份用于假流式；必须按 rune 切避免
// 多字节字符被截断成 U+FFFD。
func vertexSplitRuneChunks(text string) []string {
	runes := []rune(text)
	if len(runes) == 0 {
		return nil
	}
	chunkSize := 1
	if cs := len(runes) / 8; cs > 1 {
		chunkSize = cs
	}
	chunks := make([]string, 0, (len(runes)+chunkSize-1)/chunkSize)
	for i := 0; i < len(runes); i += chunkSize {
		end := i + chunkSize
		if end > len(runes) {
			end = len(runes)
		}
		chunks = append(chunks, string(runes[i:end]))
	}
	return chunks
}

func vertexFirstChoiceContent(oai map[string]any) string {
	choices, _ := oai["choices"].([]any)
	if len(choices) == 0 {
		return ""
	}
	c0, _ := choices[0].(map[string]any)
	msg, _ := c0["message"].(map[string]any)
	if msg == nil {
		return ""
	}
	s, _ := msg["content"].(string)
	return s
}

func vertexFirstChoiceToolCalls(oai map[string]any) []any {
	choices, _ := oai["choices"].([]any)
	if len(choices) == 0 {
		return nil
	}
	c0, _ := choices[0].(map[string]any)
	msg, _ := c0["message"].(map[string]any)
	if msg == nil {
		return nil
	}
	tc, _ := msg["tool_calls"].([]any)
	return tc
}

// vertexFakeStream 以非流式方式完成生成，再把结果切片伪装成 SSE 推送，
// 让不支持流式的模型（如图像类）也能被流式客户端消费。
func (g *Gateway) vertexFakeStream(ctx context.Context, w http.ResponseWriter, r *http.Request, client *vertexai.VertexAIClient, respConv transform.ResponseConverter, exits []string, primary *proxySlot, realModel, echoModel string, geminiPayload map[string]any, requestID string) {
	sw := newVertexSSEWriter(w)
	started := time.Now()
	resp, runErr := client.CompleteChat(ctx, exits, realModel, geminiPayload)
	attempts := len(exits)
	if runErr != nil {
		ve := vertexToError(runErr)
		if ve.Kind == "ratelimit" || ve.Code == 429 {
			g.stats.Upstream429.Add(1)
		}
		g.recordAudit(r, echoModel, vertexHTTPStatus(ve), primary, started, "upstream", attempts, "")
		if !sw.hasWritten() {
			g.writeJSON(w, vertexHTTPStatus(ve), vertexErrorToOAI(ve))
			return
		}
		g.vertexStreamTerminalError(sw, ve, requestID, echoModel)
		return
	}
	g.recordAudit(r, echoModel, http.StatusOK, primary, started, "upstream", attempts, "")

	transform.EnsureFunctionCallIDs(resp)
	oai := respConv.ToOAI(resp, echoModel)
	contentText := vertexFirstChoiceContent(oai)
	toolCalls := vertexFirstChoiceToolCalls(oai)

	createdTS := time.Now().Unix()
	chunks := vertexSplitRuneChunks(contentText)
	if len(chunks) == 0 && len(toolCalls) > 0 {
		chunks = []string{""}
	}
	for i, piece := range chunks {
		base := vertexChunkBase(echoModel, requestID)
		base["created"] = createdTS
		var delta map[string]any
		if i == 0 {
			delta = map[string]any{"role": "assistant"}
			if piece != "" {
				delta["content"] = piece
			}
		} else {
			delta = map[string]any{"content": piece}
		}
		choice := map[string]any{"index": 0, "delta": delta}
		if i == len(chunks)-1 && len(toolCalls) == 0 {
			choice["finish_reason"] = "stop"
		}
		base["choices"] = []any{choice}
		if !sw.write(vertexSSEEvent(base)) {
			return
		}
	}
	finishReason := "stop"
	if len(toolCalls) > 0 {
		finishReason = "tool_calls"
	}
	base := vertexChunkBase(echoModel, requestID)
	base["created"] = createdTS
	base["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": finishReason}}
	_ = sw.write(vertexSSEEvent(base))
	_ = sw.write("data: [DONE]\n\n")
}
