package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type geoIPResult struct {
	Country     string
	CountryCode string
	Status      string
	ExpiresAt   time.Time
}

type geoIPResponse struct {
	Success     bool   `json:"success"`
	Country     string `json:"country"`
	CountryCode string `json:"country_code"`
}

const (
	geoIPCacheTTL        = 30 * time.Minute
	geoIPFailureCacheTTL = 2 * time.Minute
)

func (s *Server) annotateFreeBuffEgress(slots []map[string]any) {
	if len(slots) == 0 {
		return
	}
	seen := make(map[string]geoIPResult)
	for _, slot := range slots {
		ip := strings.TrimSpace(fmt.Sprint(slot["egress"]))
		if ip == "" || net.ParseIP(ip) == nil || !isPublicEgressIP(ip) {
			continue
		}
		if result, ok := seen[ip]; ok {
			applyGeoIPResult(slot, result)
			continue
		}
		result := s.lookupGeoIP(ip)
		seen[ip] = result
		applyGeoIPResult(slot, result)
	}
}

func isPublicEgressIP(raw string) bool {
	ip := net.ParseIP(raw)
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
		return false
	}
	return true
}

func applyGeoIPResult(slot map[string]any, result geoIPResult) {
	if result.Country != "" {
		slot["egress_country"] = result.Country
	}
	if result.CountryCode != "" {
		slot["egress_country_code"] = result.CountryCode
	}
	if result.Status != "" {
		slot["egress_country_status"] = result.Status
	}
}

func (s *Server) lookupGeoIP(ip string) geoIPResult {
	now := time.Now()
	s.geoIPMu.Lock()
	if cached, ok := s.geoIPCache[ip]; ok && now.Before(cached.ExpiresAt) {
		s.geoIPMu.Unlock()
		return cached
	}
	s.geoIPMu.Unlock()

	result := geoIPResult{Status: "unknown", ExpiresAt: now.Add(geoIPFailureCacheTTL)}
	base := strings.TrimRight(strings.TrimSpace(s.geoIPBaseURL), "/")
	if base != "" {
		endpoint := base + "/" + url.PathEscape(ip)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err == nil {
			resp, requestErr := s.geoIPClient.Do(req)
			if requestErr == nil {
				var payload geoIPResponse
				decodeErr := json.NewDecoder(resp.Body).Decode(&payload)
				resp.Body.Close()
				if decodeErr == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && payload.Success && payload.CountryCode != "" {
					result.Country = strings.TrimSpace(payload.Country)
					result.CountryCode = strings.ToUpper(strings.TrimSpace(payload.CountryCode))
					if result.CountryCode == "US" {
						result.Status = "us"
					} else {
						result.Status = "non_us"
					}
					result.ExpiresAt = now.Add(geoIPCacheTTL)
				}
			}
		}
		cancel()
	}
	s.geoIPMu.Lock()
	s.geoIPCache[ip] = result
	s.geoIPMu.Unlock()
	return result
}
