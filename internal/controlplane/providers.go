package controlplane

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
	"unicode"
)

const (
	freeBuffModelsURL     = "https://github.com/pingmike2/freebuff2api-wokers/releases/download/models-cache/freebuff-models.json"
	freeBuffModelsRefresh = 30 * time.Minute
	freeBuffModelsRetry   = time.Minute
	freeBuffClientPrefix  = "FreeBuff/"

	openCodeModelsURL     = "https://opencode.ai/zen/v1/models"
	openCodeModelsRefresh = 30 * time.Minute
	openCodeModelsRetry   = time.Minute
)

type ProviderKey struct {
	ID        string    `json:"id"`
	Provider  string    `json:"provider"`
	Label     string    `json:"label"`
	Secret    string    `json:"secret,omitempty"`
	Masked    string    `json:"secret_masked,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type ModelSettings struct {
	DisabledProviders map[string]bool            `json:"disabled_providers"`
	DisabledModels    map[string]map[string]bool `json:"disabled_models"`
}

type providerModel struct {
	ID          string `json:"id"`
	ClientModel string `json:"client_model"`
	Enabled     bool   `json:"enabled"`
}

type providerCatalog struct {
	ID      string          `json:"id"`
	Name    string          `json:"name"`
	Enabled bool            `json:"enabled"`
	Models  []providerModel `json:"models"`
}

var freeBuffFallbackModels = []string{
	"mimo/mimo-v2.5",
	"minimax/minimax-m3",
	"openai/gpt-5.6-luna",
	"deepseek/deepseek-v4-pro",
	"deepseek/deepseek-v4-flash",
	"z-ai/glm-5.2",
	"poolside/laguna-s-2.1",
	"openrouter/poolside/laguna-s-2.1",
	"crof/kimi-k3-eco",
	"anthropic/claude-fable-5",
	"meta/muse-spark-1.2-contributor",
}

func defaultModelSettings() ModelSettings {
	return ModelSettings{DisabledProviders: make(map[string]bool), DisabledModels: make(map[string]map[string]bool)}
}

func validProvider(provider string) bool {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case ProviderTokenRouter, ProviderOpenCode, ProviderCline, ProviderFreeBuff:
		return true
	default:
		return false
	}
}

func providerDisplayName(provider string) string {
	switch provider {
	case ProviderTokenRouter:
		return "TokenRouter"
	case ProviderOpenCode:
		return "OpenCode"
	case ProviderCline:
		return "Cline"
	case ProviderFreeBuff:
		return "FreeBuff"
	default:
		return provider
	}
}

func clientModelFor(provider, model string) string {
	switch provider {
	case ProviderTokenRouter:
		return tokenRouterClientModel
	case ProviderOpenCode:
		return openCodeClientModelFor(model)
	case ProviderCline:
		if model == clineUpstreamModel {
			return clineClientModel
		}
		return "cline/" + strings.TrimSpace(model)
	case ProviderFreeBuff:
		model = strings.TrimSpace(model)
		if index := strings.IndexByte(model, '/'); index >= 0 && index+1 < len(model) {
			model = model[index+1:]
		}
		return freeBuffClientPrefix + model
	default:
		return model
	}
}

func openCodeClientModelFor(model string) string {
	return "OpenCode/" + strings.TrimSuffix(strings.TrimSpace(model), "-free")
}

func (s *Server) loadProviderKeys() {
	data, err := os.ReadFile(s.cfg.DataDir + "/provider-keys.json")
	if err != nil || json.Unmarshal(data, &s.providerKeys) != nil {
		return
	}
	clean := s.providerKeys[:0]
	for _, record := range s.providerKeys {
		record.Provider = strings.ToLower(strings.TrimSpace(record.Provider))
		record.Label = strings.TrimSpace(record.Label)
		record.Secret = strings.TrimSpace(record.Secret)
		if record.ID != "" && validProvider(record.Provider) && record.Provider != ProviderOpenCode && record.Secret != "" {
			clean = append(clean, record)
		}
	}
	s.providerKeys = clean
}

func (s *Server) persistProviderKeysLocked() {
	data, _ := json.MarshalIndent(s.providerKeys, "", "  ")
	if err := writePrivateFileAtomic(s.cfg.DataDir+"/provider-keys.json", data); err != nil {
		s.addPersistenceLog("provider keys", err)
	}
}

func (s *Server) loadModelSettings() {
	data, err := os.ReadFile(s.cfg.DataDir + "/model-settings.json")
	if err == nil {
		_ = json.Unmarshal(data, &s.modelSettings)
	}
	if s.modelSettings.DisabledProviders == nil {
		s.modelSettings.DisabledProviders = make(map[string]bool)
	}
	if s.modelSettings.DisabledModels == nil {
		s.modelSettings.DisabledModels = make(map[string]map[string]bool)
	}
}

func (s *Server) persistModelSettingsLocked() {
	data, _ := json.MarshalIndent(s.modelSettings, "", "  ")
	if err := writePrivateFileAtomic(s.cfg.DataDir+"/model-settings.json", data); err != nil {
		s.addPersistenceLog("model settings", err)
	}
}

func (s *Server) providerEnabled(provider string) bool {
	s.mu.RLock()
	disabled := s.modelSettings.DisabledProviders[provider]
	s.mu.RUnlock()
	return !disabled
}

func (s *Server) modelEnabled(provider, model string) bool {
	s.mu.RLock()
	disabledProvider := s.modelSettings.DisabledProviders[provider]
	disabledModel := s.modelSettings.DisabledModels[provider][model]
	s.mu.RUnlock()
	return !disabledProvider && !disabledModel
}

func (s *Server) providerKeySecret(id, provider string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, record := range s.providerKeys {
		if record.ID == id && record.Provider == provider {
			return record.Secret, true
		}
	}
	return "", false
}

func (s *Server) providerKeysAPI(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.mu.RLock()
		keys := make([]ProviderKey, len(s.providerKeys))
		for index, record := range s.providerKeys {
			record.Masked = maskAPIKey(record.Secret)
			record.Secret = ""
			keys[index] = record
		}
		s.mu.RUnlock()
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].Provider != keys[j].Provider {
				return keys[i].Provider < keys[j].Provider
			}
			return keys[i].CreatedAt.Before(keys[j].CreatedAt)
		})
		writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
	case http.MethodPost:
		var request struct {
			Provider string `json:"provider"`
			Label    string `json:"label"`
			Secret   string `json:"secret"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&request) != nil {
			http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
			return
		}
		request.Provider = strings.ToLower(strings.TrimSpace(request.Provider))
		request.Label = strings.TrimSpace(request.Label)
		request.Secret = strings.TrimSpace(request.Secret)
		if !validProvider(request.Provider) || request.Provider == ProviderOpenCode {
			http.Error(w, `{"error":"provider_key_not_supported"}`, http.StatusBadRequest)
			return
		}
		if request.Label == "" || len(request.Label) > 80 {
			http.Error(w, `{"error":"invalid_label"}`, http.StatusBadRequest)
			return
		}
		if request.Secret == "" || len(request.Secret) > 512 || strings.IndexFunc(request.Secret, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
			http.Error(w, `{"error":"invalid_upstream_key"}`, http.StatusBadRequest)
			return
		}
		buf := make([]byte, 12)
		if _, err := rand.Read(buf); err != nil {
			http.Error(w, `{"error":"key_id_generation_failed"}`, http.StatusInternalServerError)
			return
		}
		record := ProviderKey{ID: "pk_" + hex.EncodeToString(buf), Provider: request.Provider, Label: request.Label, Secret: request.Secret, CreatedAt: time.Now().UTC()}
		s.mu.Lock()
		for _, existing := range s.providerKeys {
			if existing.Provider == record.Provider && existing.Label == record.Label {
				s.mu.Unlock()
				http.Error(w, `{"error":"provider_key_label_exists"}`, http.StatusConflict)
				return
			}
		}
		s.providerKeys = append(s.providerKeys, record)
		s.persistProviderKeysLocked()
		s.mu.Unlock()
		record.Masked, record.Secret = maskAPIKey(record.Secret), ""
		writeJSON(w, http.StatusCreated, map[string]any{"key": record})
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, `{"error":"method_not_allowed"}`, http.StatusMethodNotAllowed)
	}
}

func (s *Server) deleteProviderKey(w http.ResponseWriter, id string) {
	s.mu.Lock()
	for _, instance := range s.instances {
		if instance.UpstreamKeyID == id || containsString(instance.UpstreamKeyIDs, id) {
			s.mu.Unlock()
			http.Error(w, `{"error":"provider_key_in_use"}`, http.StatusConflict)
			return
		}
	}
	next := s.providerKeys[:0]
	found := false
	for _, record := range s.providerKeys {
		if record.ID == id {
			found = true
			continue
		}
		next = append(next, record)
	}
	s.providerKeys = next
	if found {
		s.persistProviderKeysLocked()
	}
	s.mu.Unlock()
	if !found {
		http.Error(w, `{"error":"provider_key_not_found"}`, http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (s *Server) modelSettingsAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		s.refreshFreeBuffModels()
		s.refreshOpenCodeModels()
		writeJSON(w, http.StatusOK, map[string]any{"providers": s.modelCatalog()})
		return
	}
	if r.Method != http.MethodPut {
		w.Header().Set("Allow", "GET, PUT")
		http.Error(w, `{"error":"method_not_allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var request struct {
		Provider string `json:"provider"`
		Model    string `json:"model"`
		Enabled  bool   `json:"enabled"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&request) != nil {
		http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
		return
	}
	request.Provider = strings.ToLower(strings.TrimSpace(request.Provider))
	request.Model = strings.TrimSpace(request.Model)
	if !validProvider(request.Provider) {
		http.Error(w, `{"error":"invalid_provider"}`, http.StatusBadRequest)
		return
	}
	if request.Model != "" && !catalogContains(s.modelCatalog(), request.Provider, request.Model) {
		http.Error(w, `{"error":"unknown_model"}`, http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	if request.Model == "" {
		if request.Enabled {
			delete(s.modelSettings.DisabledProviders, request.Provider)
		} else {
			s.modelSettings.DisabledProviders[request.Provider] = true
		}
	} else {
		if s.modelSettings.DisabledModels[request.Provider] == nil {
			s.modelSettings.DisabledModels[request.Provider] = make(map[string]bool)
		}
		if request.Enabled {
			delete(s.modelSettings.DisabledModels[request.Provider], request.Model)
		} else {
			s.modelSettings.DisabledModels[request.Provider][request.Model] = true
		}
	}
	s.persistModelSettingsLocked()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"providers": s.modelCatalog()})
}

func catalogContains(catalog []providerCatalog, provider, model string) bool {
	for _, group := range catalog {
		if group.ID != provider {
			continue
		}
		for _, candidate := range group.Models {
			if candidate.ID == model || candidate.ClientModel == model {
				return true
			}
		}
	}
	return false
}

func (s *Server) modelCatalog() []providerCatalog {
	models := map[string][]string{
		ProviderTokenRouter: {tokenRouterModel},
		ProviderOpenCode:    s.openCodeModelList(),
		ProviderCline:       {clineUpstreamModel},
		ProviderFreeBuff:    nil,
	}
	s.providerModelsMu.RLock()
	models[ProviderFreeBuff] = append([]string(nil), s.freeBuffModels...)
	s.providerModelsMu.RUnlock()
	s.clineModelsMu.RLock()
	for model := range s.clineModels {
		if model != clineClientModel && model != clineUpstreamModel {
			models[ProviderCline] = append(models[ProviderCline], model)
		}
	}
	s.clineModelsMu.RUnlock()
	order := []string{ProviderTokenRouter, ProviderOpenCode, ProviderCline, ProviderFreeBuff}
	catalog := make([]providerCatalog, 0, len(order))
	for _, provider := range order {
		seen := make(map[string]struct{})
		group := providerCatalog{ID: provider, Name: providerDisplayName(provider), Enabled: s.providerEnabled(provider)}
		for _, model := range models[provider] {
			if _, exists := seen[model]; exists {
				continue
			}
			seen[model] = struct{}{}
			group.Models = append(group.Models, providerModel{ID: model, ClientModel: clientModelFor(provider, model), Enabled: s.modelEnabled(provider, model)})
		}
		sort.Slice(group.Models, func(i, j int) bool { return group.Models[i].ID < group.Models[j].ID })
		catalog = append(catalog, group)
	}
	return catalog
}

func (s *Server) refreshFreeBuffModels() {
	s.providerModelsMu.RLock()
	now := time.Now()
	fresh := time.Since(s.freeBuffModelsAt) < freeBuffModelsRefresh || now.Before(s.freeBuffModelsRetryAt) || s.freeBuffModelsRefreshing
	s.providerModelsMu.RUnlock()
	if fresh {
		return
	}
	s.providerModelsMu.Lock()
	if time.Since(s.freeBuffModelsAt) < freeBuffModelsRefresh || time.Now().Before(s.freeBuffModelsRetryAt) || s.freeBuffModelsRefreshing {
		s.providerModelsMu.Unlock()
		return
	}
	s.freeBuffModelsRefreshing = true
	s.providerModelsMu.Unlock()
	defer func() {
		s.providerModelsMu.Lock()
		s.freeBuffModelsRefreshing = false
		s.providerModelsMu.Unlock()
	}()
	baseClient := s.client
	if s.freeBuffModelsClient != nil {
		baseClient = s.freeBuffModelsClient
	}
	client := *baseClient
	client.Timeout = 5 * time.Second
	for _, sourceURL := range s.freeBuffModelsURLs {
		response, err := client.Get(sourceURL)
		if err != nil {
			continue
		}
		var payload struct {
			Models []struct {
				ID string `json:"id"`
			} `json:"models"`
		}
		decodeErr := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&payload)
		response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 || decodeErr != nil {
			continue
		}
		models := make([]string, 0, len(payload.Models))
		for _, item := range payload.Models {
			if model := strings.TrimSpace(item.ID); model != "" {
				models = append(models, model)
			}
		}
		if len(models) == 0 {
			continue
		}
		s.providerModelsMu.Lock()
		s.freeBuffModels = models
		s.freeBuffModelsAt = time.Now()
		s.freeBuffModelsRetryAt = time.Time{}
		s.providerModelsMu.Unlock()
		return
	}
	s.providerModelsMu.Lock()
	s.freeBuffModelsRetryAt = time.Now().Add(freeBuffModelsRetry)
	s.providerModelsMu.Unlock()
}

func (s *Server) providerModelFromClient(provider, model string) (string, bool) {
	model = strings.TrimSpace(model)
	switch provider {
	case ProviderTokenRouter:
		return tokenRouterModel, model == tokenRouterModel || model == tokenRouterClientModel
	case ProviderOpenCode:
		for _, upstream := range s.openCodeModelList() {
			if model == upstream || model == openCodeClientModelFor(upstream) {
				return upstream, true
			}
		}
	case ProviderCline:
		if model == clineClientModel {
			return clineUpstreamModel, true
		}
		if strings.HasPrefix(strings.ToLower(model), "cline/") {
			return strings.TrimSpace(model[len("cline/"):]), true
		}
	case ProviderFreeBuff:
		if strings.HasPrefix(strings.ToLower(model), "freebuff/") {
			return strings.TrimSpace(model[len("FreeBuff/"):]), true
		}
	}
	return model, false
}

func (s *Server) providerFromClientModel(model string) (string, string, bool) {
	for _, provider := range []string{ProviderTokenRouter, ProviderOpenCode, ProviderCline, ProviderFreeBuff} {
		if upstream, ok := s.providerModelFromClient(provider, model); ok {
			return provider, upstream, true
		}
	}
	return "", "", false
}

// openCodeModelList returns the keyless OpenCode models advertised by the
// control plane: the dynamically discovered free tier when available, with the
// curated fallback list used until the first successful refresh.
func (s *Server) openCodeModelList() []string {
	s.openCodeModelsMu.RLock()
	dynamic := s.openCodeDynamicModels
	s.openCodeModelsMu.RUnlock()
	if len(dynamic) == 0 {
		return append([]string(nil), openCodeModels...)
	}
	return append([]string(nil), dynamic...)
}

// refreshOpenCodeModels periodically mirrors the free tier of the upstream
// OpenCode catalog. A fetched model is kept when it ends in "-free" (the
// keyless tier) or is already part of the curated fallback list; everything
// else requires upstream credentials and stays out of the catalog.
func (s *Server) refreshOpenCodeModels() {
	s.openCodeModelsMu.RLock()
	now := time.Now()
	fresh := time.Since(s.openCodeModelsAt) < openCodeModelsRefresh || now.Before(s.openCodeModelsRetryAt) || s.openCodeModelsRefreshing
	s.openCodeModelsMu.RUnlock()
	if fresh {
		return
	}
	s.openCodeModelsMu.Lock()
	if time.Since(s.openCodeModelsAt) < openCodeModelsRefresh || time.Now().Before(s.openCodeModelsRetryAt) || s.openCodeModelsRefreshing {
		s.openCodeModelsMu.Unlock()
		return
	}
	s.openCodeModelsRefreshing = true
	s.openCodeModelsMu.Unlock()
	defer func() {
		s.openCodeModelsMu.Lock()
		s.openCodeModelsRefreshing = false
		s.openCodeModelsMu.Unlock()
	}()
	client := *s.client
	client.Timeout = 5 * time.Second
	response, err := client.Get(openCodeModelsURL)
	if err != nil {
		s.openCodeModelsMu.Lock()
		s.openCodeModelsRetryAt = time.Now().Add(openCodeModelsRetry)
		s.openCodeModelsMu.Unlock()
		return
	}
	var payload struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	decodeErr := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&payload)
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 || decodeErr != nil {
		s.openCodeModelsMu.Lock()
		s.openCodeModelsRetryAt = time.Now().Add(openCodeModelsRetry)
		s.openCodeModelsMu.Unlock()
		return
	}
	fallback := make(map[string]struct{}, len(openCodeModels))
	for _, model := range openCodeModels {
		fallback[model] = struct{}{}
	}
	models := make([]string, 0, len(payload.Data))
	for _, item := range payload.Data {
		model := strings.TrimSpace(item.ID)
		if model == "" || len(model) > maxClineModelLength || !openCodeModelPattern.MatchString(model) {
			continue
		}
		_, curated := fallback[model]
		if !curated && !strings.HasSuffix(model, "-free") {
			continue
		}
		models = append(models, model)
	}
	if len(models) == 0 {
		s.openCodeModelsMu.Lock()
		s.openCodeModelsRetryAt = time.Now().Add(openCodeModelsRetry)
		s.openCodeModelsMu.Unlock()
		return
	}
	sort.Strings(models)
	s.openCodeModelsMu.Lock()
	s.openCodeDynamicModels = models
	s.openCodeModelsAt = time.Now()
	s.openCodeModelsRetryAt = time.Time{}
	s.openCodeModelsMu.Unlock()
}

func providerKeyRequired(provider string) bool {
	return provider != ProviderOpenCode
}

func validateProviderKeyReference(id, provider string) error {
	if !providerKeyRequired(provider) {
		return nil
	}
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("upstream_key_id_required")
	}
	return nil
}
