package gateway

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"dualroute-gateway/internal/transform"
	"dualroute-gateway/internal/vertexai"
)

// Media endpoints for the Vertex provider, mirroring vproxy's OpenAI-compatible
// surface: images (generations/edits/variations), TTS speech, and token
// counting. All attempts ride the same proxy-slot rotation as chat.

const (
	vertexMultipartMemoryLimit = 32 << 20
	vertexDefaultImageSize     = "1K"
	vertexDefaultModalities    = "图文"
)

func vertexGetStr(body map[string]any, key, def string) string {
	if v, ok := body[key].(string); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return def
}

func vertexFirstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

func vertexCoerceN(value string) int {
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || n < 1 {
		return 1
	}
	if n > 8 {
		return 8
	}
	return n
}

func vertexFormValue(r *http.Request, key string) string {
	return strings.TrimSpace(r.FormValue(key))
}

func vertexFormUploads(r *http.Request, key string) []*multipart.FileHeader {
	if r.MultipartForm == nil {
		return nil
	}
	return r.MultipartForm.File[key]
}

func vertexUploadToInlineImage(fh *multipart.FileHeader) (transform.InlineImage, error) {
	file, err := fh.Open()
	if err != nil {
		return transform.InlineImage{}, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(file)
	if err != nil {
		return transform.InlineImage{}, err
	}
	mimeType := fh.Header.Get("Content-Type")
	if mimeType == "" {
		switch strings.ToLower(filepath.Ext(fh.Filename)) {
		case ".png":
			mimeType = "image/png"
		case ".jpg", ".jpeg":
			mimeType = "image/jpeg"
		case ".webp":
			mimeType = "image/webp"
		default:
			mimeType = "image/png"
		}
	}
	return transform.InlineImage{MimeType: mimeType, Data: base64.StdEncoding.EncodeToString(data)}, nil
}

func vertexUploadsToInlineImages(fhs []*multipart.FileHeader) ([]transform.InlineImage, error) {
	out := make([]transform.InlineImage, 0, len(fhs))
	for _, fh := range fhs {
		img, err := vertexUploadToInlineImage(fh)
		if err != nil {
			return nil, err
		}
		out = append(out, img)
	}
	return out, nil
}


func (g *Gateway) vertexMediaError(w http.ResponseWriter, runErr error) bool {
	if runErr == nil {
		return false
	}
	ve := vertexToError(runErr)
	g.writeJSON(w, vertexHTTPStatus(ve), vertexErrorToOAI(ve))
	return true
}

// ---- images ----

func (g *Gateway) vertexImagesGenerations(w http.ResponseWriter, r *http.Request, body []byte) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil || payload == nil {
		vertexWriteError(w, http.StatusBadRequest, "invalid JSON body", "invalid_request_error")
		return
	}
	rawModel := transform.ResolveImageModel(vertexGetStr(payload, "model", "gemini-3-pro-image"))
	model := strings.TrimPrefix(strings.TrimPrefix(rawModel, "Vertex/"), "vertex/")
	prompt := vertexGetStr(payload, "prompt", "")
	if prompt == "" {
		vertexWriteError(w, http.StatusBadRequest, "missing prompt", "invalid_request_error")
		return
	}
	respFmt := vertexGetStr(payload, "response_format", "b64_json")
	n := vertexCoerceN(strconv.Itoa(int(vertexFloatOr(payload["n"], 1))))

	geminiPayload := map[string]any{
		"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": prompt}}}},
	}
	transform.ApplyImageConfig(geminiPayload, payload, model)
	transform.ApplyImageDefaults(geminiPayload, model, vertexDefaultImageSize, vertexDefaultModalities)
	g.vertexRunImageRequest(w, r, model, geminiPayload, n, respFmt)
}

func vertexFloatOr(v any, def float64) float64 {
	if f, ok := v.(float64); ok && f >= 1 {
		return f
	}
	return def
}

func (g *Gateway) vertexImagesEditVariation(w http.ResponseWriter, r *http.Request, variation bool) {
	if err := r.ParseMultipartForm(vertexMultipartMemoryLimit); err != nil {
		vertexWriteError(w, http.StatusBadRequest, "failed to parse multipart form", "invalid_request_error")
		return
	}
	uploads := vertexFormUploads(r, "image")
	if len(uploads) == 0 {
		vertexWriteError(w, http.StatusBadRequest, "image is required", "invalid_request_error")
		return
	}
	images, err := vertexUploadsToInlineImages(uploads)
	if err != nil {
		vertexWriteError(w, http.StatusBadRequest, err.Error(), "invalid_request_error")
		return
	}
	var mask *transform.InlineImage
	if maskUploads := vertexFormUploads(r, "mask"); len(maskUploads) > 0 {
		m, mErr := vertexUploadToInlineImage(maskUploads[0])
		if mErr != nil {
			vertexWriteError(w, http.StatusBadRequest, mErr.Error(), "invalid_request_error")
			return
		}
		mask = &m
	}
	rawModel := transform.ResolveImageModel(vertexFormValue(r, "model"))
	model := strings.TrimPrefix(strings.TrimPrefix(rawModel, "Vertex/"), "vertex/")
	prompt := vertexFirstNonEmpty(vertexFormValue(r, "prompt"), map[bool]string{true: "Create a variation of the provided image.", false: "Edit the provided image."}[variation])
	prompt = transform.AppendNegativePrompt(prompt, vertexFormValue(r, "negative_prompt"))
	n := vertexCoerceN(vertexFormValue(r, "n"))
	respFmt := vertexFirstNonEmpty(vertexFormValue(r, "response_format"), "b64_json")

	kind := "edit"
	if variation {
		kind = "variation"
	}
	geminiPayload := transform.BuildImagePayload(model, prompt, images, mask,
		vertexFormValue(r, "size"), vertexFormValue(r, "quality"), vertexFormValue(r, "style"), "", kind)
	transform.ApplyImageDefaults(geminiPayload, model, vertexDefaultImageSize, vertexDefaultModalities)
	g.vertexRunImageRequest(w, r, model, geminiPayload, n, respFmt)
}

func (g *Gateway) vertexRunImageRequest(w http.ResponseWriter, r *http.Request, model string, geminiPayload map[string]any, n int, responseFormat string) {
	client, _, _ := vertexStack(g.cfg)
	wantURL := responseFormat == "url"
	exits, primary := g.vertexExitCandidates(model)
	if primary == nil {
		primary = &proxySlot{}
	}
	items := make([]any, 0, n)
	started := time.Now()
	for i := 0; i < n; i++ {
		images, runErr := client.CompleteChatImage(r.Context(), exits, model, geminiPayload)
		if runErr != nil {
			ve := vertexToError(runErr)
			if ve.Kind == "ratelimit" || ve.Code == 429 {
				g.stats.Upstream429.Add(1)
			}
			g.recordAudit(r, model, vertexHTTPStatus(ve), primary, started, "upstream", i+1, "")
			g.writeJSON(w, vertexHTTPStatus(ve), vertexErrorToOAI(ve))
			return
		}
		g.recordAudit(r, model, http.StatusOK, primary, started, "upstream", i+1, "")
		for _, img := range images {
			if img.B64JSON == "" {
				continue
			}
			if wantURL {
				mimeType := img.MimeType
				if mimeType == "" {
					mimeType = "image/png"
				}
				items = append(items, map[string]any{"url": "data:" + mimeType + ";base64," + img.B64JSON})
			} else {
				items = append(items, map[string]any{"b64_json": img.B64JSON})
			}
		}
		if len(items) >= n {
			break
		}
	}
	if len(items) == 0 {
		g.writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{
			"message": "no image returned", "type": "server_error", "code": 502}})
		return
	}
	g.writeJSON(w, http.StatusOK, map[string]any{"created": time.Now().Unix(), "data": items})
}
// ---- TTS ----

const (
	vertexTTSDefaultModel = "gemini-3.1-flash-tts-preview"
	vertexTTSDefaultVoice = "Kore"
)

var vertexTTSVoiceMap = map[string]string{
	"alloy": "Kore", "echo": "Puck", "fable": "Charon", "onyx": "Fenrir",
	"nova": "Aoede", "shimmer": "Leda", "ash": "Orus", "ballad": "Zephyr",
	"coral": "Aoede", "sage": "Charon", "verse": "Puck",
}

var vertexTTSGeminiVoices = map[string]bool{
	"Kore": true, "Puck": true, "Charon": true, "Aoede": true, "Fenrir": true, "Leda": true,
	"Orus": true, "Zephyr": true, "Autonoe": true, "Enceladus": true, "Iapetus": true,
	"Umbriel": true, "Algieba": true, "Despina": true, "Erinome": true, "Algenib": true,
	"Rasalgethi": true, "Laomedeia": true, "Achernar": true, "Alnilam": true, "Schedar": true,
	"Gacrux": true, "Pulcherrima": true, "Achird": true, "Zubenelgenubi": true,
	"Vindemiatrix": true, "Sadachbia": true, "Sadaltager": true, "Sulafat": true,
}

type vertexTTSFormat struct {
	contentType string
	wrapWAV     bool
}

var vertexTTSFormats = map[string]vertexTTSFormat{
	"mp3": {"audio/wav", true}, "wav": {"audio/wav", true}, "pcm": {"audio/L16", false},
	"opus": {"audio/wav", true}, "aac": {"audio/wav", true}, "flac": {"audio/wav", true},
}

func vertexTTSResolveVoice(voice any) string {
	v, ok := voice.(string)
	if !ok || strings.TrimSpace(v) == "" {
		return vertexTTSDefaultVoice
	}
	v = strings.TrimSpace(v)
	if vertexTTSGeminiVoices[v] {
		return v
	}
	if mapped, ok := vertexTTSVoiceMap[strings.ToLower(v)]; ok {
		return mapped
	}
	return vertexTTSDefaultVoice
}

func vertexTTSPCMRate(mimeType string) int {
	const def = 24000
	for _, token := range strings.Split(mimeType, ";") {
		token = strings.ToLower(strings.TrimSpace(token))
		if strings.HasPrefix(token, "rate=") {
			if n, err := strconv.Atoi(token[5:]); err == nil && n > 0 {
				return n
			}
		}
	}
	return def
}

func vertexAppendU32LE(b []byte, v uint32) []byte {
	return append(b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func vertexTTSWAVHeader(dataLen, sampleRate int) []byte {
	const bits, channels = 16, 1
	byteRate := sampleRate * channels * bits / 8
	blockAlign := channels * bits / 8
	h := make([]byte, 0, 44)
	h = append(h, "RIFF"...)
	h = vertexAppendU32LE(h, uint32(36+dataLen))
	h = append(h, "WAVEfmt "...)
	h = vertexAppendU32LE(h, 16)
	h = append(h, 1, 0)
	h = append(h, byte(channels), 0)
	h = vertexAppendU32LE(h, uint32(sampleRate))
	h = vertexAppendU32LE(h, uint32(byteRate))
	h = append(h, byte(blockAlign), 0)
	h = append(h, byte(bits), 0)
	h = append(h, "data"...)
	return vertexAppendU32LE(h, uint32(dataLen))
}

func vertexTTSSpeed(speed any) float64 {
	switch v := speed.(type) {
	case nil:
		return 1.0
	case float64:
		return v
	case int:
		return float64(v)
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return f
		}
	}
	return 1.0
}

func vertexTTSPayload(text, voice string, speed any) map[string]any {
	spd := vertexTTSSpeed(speed)
	prompt := text
	if spd != 0 && math.Abs(spd-1.0) > 1e-6 {
		pace := "faster"
		if spd < 1.0 {
			pace = "more slowly"
		}
		prompt = "Say the following " + pace + ": " + text
	}
	return map[string]any{
		"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": prompt}}}},
		"generationConfig": map[string]any{
			"responseModalities": []any{"AUDIO"},
			"speechConfig": map[string]any{
				"voiceConfig": map[string]any{
					"prebuiltVoiceConfig": map[string]any{"voiceName": voice},
				},
			},
		},
	}
}

func (g *Gateway) vertexAudioSpeech(w http.ResponseWriter, r *http.Request, body []byte) {
	client, _, _ := vertexStack(g.cfg)
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil || payload == nil {
		vertexWriteError(w, http.StatusBadRequest, "invalid JSON body", "invalid_request_error")
		return
	}
	model := strings.TrimPrefix(strings.TrimPrefix(vertexGetStr(payload, "model", vertexTTSDefaultModel), "Vertex/"), "vertex/")
	text, _ := payload["input"].(string)
	if strings.TrimSpace(text) == "" {
		vertexWriteError(w, http.StatusBadRequest, "missing input text", "invalid_request_error")
		return
	}
	voice := vertexTTSResolveVoice(payload["voice"])
	respFmt := strings.ToLower(vertexFirstNonEmpty(vertexGetStr(payload, "response_format", ""), "mp3"))
	fmtInfo, ok := vertexTTSFormats[respFmt]
	if !ok {
		fmtInfo = vertexTTSFormat{"audio/wav", true}
	}

	exits, primary := g.vertexExitCandidates(model)
	if primary == nil {
		primary = &proxySlot{}
	}
	started := time.Now()
	audio, runErr := client.CompleteChatAudio(r.Context(), exits, model, vertexTTSPayload(text, voice, payload["speed"]))
	if runErr != nil {
		ve := vertexToError(runErr)
		g.recordAudit(r, model, vertexHTTPStatus(ve), primary, started, "upstream", 1, "")
		g.writeJSON(w, vertexHTTPStatus(ve), vertexErrorToOAI(ve))
		return
	}
	g.recordAudit(r, model, http.StatusOK, primary, started, "upstream", 1, "")
	if audio.Data == "" {
		g.writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{
			"message": "no audio returned", "type": "server_error", "code": 502}})
		return
	}
	raw, err := base64.StdEncoding.DecodeString(audio.Data)
	if err != nil {
		g.writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{
			"message": "audio decode failed", "type": "server_error", "code": 502}})
		return
	}
	out := raw
	if fmtInfo.wrapWAV {
		out = append(vertexTTSWAVHeader(len(raw), vertexTTSPCMRate(audio.MimeType)), raw...)
	}
	w.Header().Set("Content-Type", fmtInfo.contentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

// ---- count tokens ----

// vertexCountTokens serves POST /v1beta/models/{model}:countTokens in native
// Gemini shape. Counting is a local estimator ported from vproxy (the upstream
// anonymous CountTokens operation is no longer served).
func (g *Gateway) vertexCountTokens(w http.ResponseWriter, r *http.Request, modelName string) {
	var payload struct {
		Contents []any `json:"contents"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&payload); err != nil {
		vertexWriteError(w, http.StatusBadRequest, "invalid JSON body", "invalid_request_error")
		return
	}
	model := strings.TrimPrefix(strings.TrimPrefix(modelName, "Vertex/"), "vertex/")
	total := vertexai.CountTokens(model, payload.Contents)
	g.writeJSON(w, http.StatusOK, map[string]any{"totalTokens": total})
}
