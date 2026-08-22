// Package config provides the configuration surface required by the ported
// Vertex calling stack (transform/recaptcha/transport/vertexai). It mirrors
// the subset of vproxy's config provider that the ported code consumes, backed
// by static values supplied by the gateway worker.
package config

// ConfigProvider is the read-only configuration view used by the ported
// Vertex packages.
type ConfigProvider interface {
	MaxRetries() int
	DebugMode() bool
	DropMaxTokens() bool
	RequestTimeout() int
	StreamIdleTimeoutSeconds() int
	ModelTurnGuardEnabled() bool
	VertexAPIKey() string
	ProxyURL() string
	SafetySettings() map[string]string
	ParallelPoolEnabled() bool
	ParallelPoolRetryEnabled() bool
	ParallelPoolSize() int
	ParallelNodeTopK() int
	RaceTimeout() int
	ActiveNodeURI() string
	ParallelPoolDelayMs() int
	ParallelPoolDelayDynamic() bool
	StickyNodePriority() bool
	ResolveModelName(model string) string
	LookupModel(model string) (ModelEntry, bool)
}

// ModelEntry mirrors vproxy's model capability descriptor. dualroute keeps a
// fixed registry: every known Gemini model enables fake streaming.
type ModelEntry struct {
	ID                 string `json:"id"`
	Enabled            bool   `json:"enabled"`
	FakeStreamEnabled  bool   `json:"fake_stream_enabled"`
	TrailingFixEnabled bool   `json:"trailing_fix_enabled"`
}

// Static is an immutable ConfigProvider. The gateway worker builds one per
// upstream call chain; parallel-pool fields stay disabled because egress
// rotation is owned by dualroute proxy slots.
type Static struct {
	Retries                  int
	TimeoutSeconds           int
	IdleTimeoutSeconds       int
	APIKey                   string
	Safety                   map[string]string
	DropTokens               bool
	TurnGuard                bool
	Debug                    bool
}

func (s Static) MaxRetries() int                   { return s.Retries }
func (s Static) DebugMode() bool                   { return s.Debug }
func (s Static) DropMaxTokens() bool               { return s.DropTokens }
func (s Static) RequestTimeout() int               { return s.TimeoutSeconds }
func (s Static) StreamIdleTimeoutSeconds() int     { return s.IdleTimeoutSeconds }
func (s Static) ModelTurnGuardEnabled() bool       { return s.TurnGuard }
func (s Static) VertexAPIKey() string              { return s.APIKey }
func (s Static) ProxyURL() string                   { return "" }
func (s Static) SafetySettings() map[string]string { return s.Safety }
func (s Static) ParallelPoolEnabled() bool         { return true }
func (s Static) ParallelPoolRetryEnabled() bool    { return false }
func (s Static) ParallelPoolSize() int             { return 8 }
func (s Static) ParallelNodeTopK() int             { return 0 }
func (s Static) RaceTimeout() int                  { return 45 }
func (s Static) ActiveNodeURI() string             { return "" }
func (s Static) ParallelPoolDelayMs() int          { return 300 }
func (s Static) ParallelPoolDelayDynamic() bool    { return false }
func (s Static) StickyNodePriority() bool          { return false }

// ResolveModelName applies the alias map, which dualroute does not maintain;
// names pass through unchanged.
func (s Static) ResolveModelName(model string) string { return model }

// LookupModel reports capabilities for known Gemini models; unknown names are
// still accepted upstream with default capabilities.
func (s Static) LookupModel(model string) (ModelEntry, bool) {
	entry := ModelEntry{ID: model, Enabled: true, FakeStreamEnabled: true}
	return entry, true
}

// ResolveModelName is the package-level alias hook; dualroute keeps no alias
// map so names pass through unchanged.
func ResolveModelName(model string) string { return model }

// SelectEntryProxySequence returns the entry-proxy hop sequence preceding a
// candidate node. dualroute has no entry proxies: an empty slice means direct.
func SelectEntryProxySequence(count int, cfg ConfigProvider) []string { return nil }

// MarkEntryProxyFailure is a no-op without entry-proxy candidates.
func MarkEntryProxyFailure(rawURI, errText string) error { return nil }

// MarkEntryProxySuccess is a no-op without entry-proxy candidates.
func MarkEntryProxySuccess(rawURI string) error { return nil }
