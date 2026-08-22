package transport

import "testing"

func TestNormalizeProxyScheme(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"socks5h://mihomo:10803", "socks5://mihomo:10803"},
		{"SOCKS5H://mihomo:10803", "socks5://mihomo:10803"},
		{"Socks5H://user:pass@host:1080", "socks5://user:pass@host:1080"},
		{"socks5://mihomo:10815", "socks5://mihomo:10815"},
		{"http://proxy:8080", "http://proxy:8080"},
		{"https://proxy:8443", "https://proxy:8443"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := normalizeProxyScheme(tc.in); got != tc.want {
			t.Errorf("normalizeProxyScheme(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestInjectProxyAcceptsNormalizedSchemes(t *testing.T) {
	for _, uri := range []string{"socks5h://mihomo:10803", "socks5://mihomo:10815", "http://proxy:8080"} {
		if _, err := injectProxy(nil, uri, "", "req", false); err != nil {
			t.Errorf("injectProxy(%q) returned error: %v", uri, err)
		}
	}
	for _, uri := range []string{"ss://abc", "vmess://xyz", "trojan://host:443"} {
		if _, err := injectProxy(nil, uri, "", "req", false); err == nil {
			t.Errorf("injectProxy(%q) expected error for non-standard scheme", uri)
		}
	}
}
