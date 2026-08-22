// Package version exposes the release version baked into the binary. The
// Dockerfile overwrites VERSION.txt from the repository root before building;
// local builds fall back to the committed value.
package version

import (
	_ "embed"
	"strings"
)

//go:embed VERSION.txt
var raw string

// Number returns the embedded release version, e.g. "1.3.0".
func Number() string {
	return strings.TrimSpace(raw)
}
