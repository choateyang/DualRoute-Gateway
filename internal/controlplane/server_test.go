package controlplane

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStaticAssetsDisableStaleCaching(t *testing.T) {
	server := &Server{}
	request := httptest.NewRequest(http.MethodGet, "/static/style.css", nil)
	response := httptest.NewRecorder()

	server.static(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("static stylesheet status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-cache, must-revalidate" {
		t.Fatalf("Cache-Control = %q, want no-cache, must-revalidate", got)
	}
	if !strings.Contains(response.Body.String(), "--bg") {
		t.Fatal("static stylesheet body does not contain the embedded application CSS")
	}
}

func TestProxyURLOwnerIsScopedByProviderAndCurrentInstance(t *testing.T) {
	instances := []Instance{
		{Name: "gateway-a", Provider: ProviderTokenRouter, ProxyURLs: []string{"socks5h://mihomo:10801"}},
		{Name: "gateway-b", Provider: ProviderOpenCode, ProxyURLs: []string{"socks5h://mihomo:10801"}},
	}
	if got := proxyURLOwner(instances, "gateway-c", ProviderTokenRouter, []string{"socks5h://mihomo:10801"}); got != "gateway-a" {
		t.Fatalf("TokenRouter owner = %q, want gateway-a", got)
	}
	if got := proxyURLOwner(instances, "gateway-c", ProviderOpenCode, []string{"socks5h://mihomo:10801"}); got != "gateway-b" {
		t.Fatalf("OpenCode owner = %q, want gateway-b", got)
	}
	if got := proxyURLOwner(instances, "gateway-a", ProviderTokenRouter, []string{"socks5h://mihomo:10801"}); got != "" {
		t.Fatalf("current instance owner = %q, want empty", got)
	}
}
