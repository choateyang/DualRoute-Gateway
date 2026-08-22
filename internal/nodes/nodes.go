// Package nodes is a dormant compatibility shim for the ported Vertex stack.
// The upstream vproxy selects egress proxies from a subscription node pool;
// dualroute owns egress rotation through gateway proxy slots instead, so the
// pool is permanently empty here and health recording is a no-op.
package nodes

// Node mirrors the subset of vproxy's node descriptor used by ported code.
type Node struct {
	Type     string `json:"type"`
	Name     string `json:"name"`
	RawURI   string `json:"raw_uri"`
	Disabled bool   `json:"disabled"`
}

// SelectForParallel always returns nil: dualroute rotates exits via proxy
// slots, so parallel node racing is disabled by construction.
func SelectForParallel(int, int, bool, bool) []Node { return nil }

// GetNodeName returns the proxy URI itself; no node catalog exists to resolve
// friendly names from.
func GetNodeName(uri string) string { return uri }

// RecordTest is a no-op: per-egress health is tracked by gateway slots.
func RecordTest(string, bool, float64, string) {}
