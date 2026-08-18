package controlplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxMihomoRotationAttempts = 8

type egressRotationResult struct {
	Instance       string `json:"instance"`
	ProxyURL       string `json:"proxy_url"`
	PreviousNode   string `json:"previous_node"`
	Node           string `json:"node"`
	PreviousEgress string `json:"previous_egress"`
	Egress         string `json:"egress"`
	Attempts       int    `json:"attempts"`
	Reason         string `json:"reason"`
}

type gatewayProbeResponse struct {
	Slots []struct {
		URL    string `json:"url"`
		Egress string `json:"egress"`
	} `json:"slots"`
}

type gatewaySlotRotationResponse struct {
	PreviousURL    string `json:"previous_url"`
	PreviousEgress string `json:"previous_egress"`
	URL            string `json:"url"`
	Egress         string `json:"egress"`
}

func mihomoGroupForProxyURL(raw string) (string, bool) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (parsed.Scheme != "socks5" && parsed.Scheme != "socks5h") {
		return "", false
	}
	host := strings.ToLower(parsed.Hostname())
	if host != "mihomo" && host != "dualroute-gateway-mihomo" {
		return "", false
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil || port < mihomoPortBase {
		return "", false
	}
	index := port - mihomoPortBase + 1
	if index < 1 || index > 128 {
		return "", false
	}
	return fmt.Sprintf("GATEWAY-SLOT-%d", index), true
}

func (s *Server) probeInstanceProxy(instance Instance, proxyURL string) (string, error) {
	payload, _ := json.Marshal(map[string]string{"url": proxyURL})
	var response gatewayProbeResponse
	if err := s.call(instance, http.MethodPost, "/admin/probe", strings.NewReader(string(payload)), &response); err != nil {
		return "", err
	}
	for _, slot := range response.Slots {
		if slot.URL == proxyURL && net.ParseIP(slot.Egress) != nil {
			return slot.Egress, nil
		}
	}
	return "", errors.New("gateway probe did not return a valid egress IP")
}

func (s *Server) rotateInstanceMihomoSlot(instance Instance, proxyURL, previousEgress, reason string, forbidden map[string]struct{}) (egressRotationResult, error) {
	result := egressRotationResult{Instance: instance.Name, ProxyURL: proxyURL, PreviousEgress: previousEgress, Reason: reason}
	groupName, ok := mihomoGroupForProxyURL(proxyURL)
	if !ok {
		return result, fmt.Errorf("proxy %q is not a managed Mihomo slot", proxyURL)
	}
	group, err := s.getMihomoProxyGroup(groupName)
	if err != nil {
		return result, err
	}
	result.PreviousNode = group.Now
	nodes := uniqueMihomoNodes(group.All)
	if len(nodes) < 2 {
		return result, fmt.Errorf("%s has fewer than two selectable nodes", groupName)
	}
	start := -1
	for index, node := range nodes {
		if node == group.Now {
			start = index
			break
		}
	}
	limit := min(len(nodes)-1, maxMihomoRotationAttempts)
	var lastErr error
	for offset := 1; offset <= len(nodes) && result.Attempts < limit; offset++ {
		index := (start + offset + len(nodes)) % len(nodes)
		node := nodes[index]
		if node == group.Now {
			continue
		}
		result.Attempts++
		if err := s.selectMihomoProxyGroup(groupName, node); err != nil {
			lastErr = err
			continue
		}
		time.Sleep(200 * time.Millisecond)
		egress, err := s.probeInstanceProxy(instance, proxyURL)
		if err != nil {
			lastErr = fmt.Errorf("probe node %s: %w", node, err)
			continue
		}
		if _, duplicate := forbidden[egress]; duplicate || egress == previousEgress {
			lastErr = fmt.Errorf("node %s still uses occupied egress %s", node, egress)
			continue
		}
		result.Node = node
		result.Egress = egress
		return result, nil
	}
	if group.Now != "" {
		_ = s.selectMihomoProxyGroup(groupName, group.Now)
		time.Sleep(100 * time.Millisecond)
		_, _ = s.probeInstanceProxy(instance, proxyURL)
	}
	if lastErr == nil {
		lastErr = errors.New("no alternative Mihomo node was available")
	}
	return result, fmt.Errorf("rotate %s after %d attempts: %w", groupName, result.Attempts, lastErr)
}

func (s *Server) rotateInstanceCandidate(instance Instance, previousURL, previousEgress, reason string, forbidden map[string]struct{}) (egressRotationResult, error) {
	blocked := make([]string, 0, len(forbidden))
	for ip := range forbidden {
		blocked = append(blocked, ip)
	}
	payload, _ := json.Marshal(map[string]any{"forbidden": blocked})
	var response gatewaySlotRotationResponse
	if err := s.call(instance, http.MethodPost, "/admin/rotate", strings.NewReader(string(payload)), &response); err != nil {
		return egressRotationResult{Instance: instance.Name, ProxyURL: previousURL, PreviousEgress: previousEgress, Reason: reason}, err
	}
	result := egressRotationResult{Instance: instance.Name, ProxyURL: response.URL, PreviousEgress: response.PreviousEgress, Egress: response.Egress, Attempts: 1, Reason: reason}
	if groupName, ok := mihomoGroupForProxyURL(response.URL); ok {
		if group, err := s.getMihomoProxyGroup(groupName); err == nil {
			result.Node = group.Now
		}
	}
	if groupName, ok := mihomoGroupForProxyURL(response.PreviousURL); ok {
		if group, err := s.getMihomoProxyGroup(groupName); err == nil {
			result.PreviousNode = group.Now
		}
	}
	return result, nil
}

func summarySlotString(slot map[string]any, key string) string {
	value, _ := slot[key].(string)
	return strings.TrimSpace(value)
}

func summarySlotBool(slot map[string]any, key string) bool {
	value, _ := slot[key].(bool)
	return value
}

func activeSummarySlots(summary Summary) []map[string]any {
	active := make([]map[string]any, 0, 1)
	for _, slot := range summary.Slots {
		if summarySlotBool(slot, "active") && summarySlotBoolDefault(slot, "enabled", true) && summarySlotBoolDefault(slot, "healthy", true) {
			active = append(active, slot)
		}
	}
	if len(active) > 0 {
		return active
	}
	for _, slot := range summary.Slots {
		if summarySlotBoolDefault(slot, "enabled", true) && summarySlotBoolDefault(slot, "healthy", true) {
			return []map[string]any{slot}
		}
	}
	// Older gateway images do not expose the active flag. Keep the first slot
	// as a compatibility fallback instead of treating every candidate as active.
	if len(summary.Slots) > 0 {
		return summary.Slots[:1]
	}
	return nil
}

func summarySlotBoolDefault(slot map[string]any, key string, fallback bool) bool {
	value, exists := slot[key]
	if !exists {
		return fallback
	}
	result, ok := value.(bool)
	return ok && result
}

func managedMihomoSlotCount(summary Summary) int {
	count := 0
	for _, slot := range summary.Slots {
		if _, ok := mihomoGroupForProxyURL(summarySlotString(slot, "url")); ok {
			count++
		}
	}
	return count
}

func (s *Server) runningSummaries() []Summary {
	s.mu.RLock()
	instances := append([]Instance(nil), s.instances...)
	s.mu.RUnlock()
	result := make([]Summary, 0, len(instances))
	for _, instance := range instances {
		if instance.Status != "running" {
			continue
		}
		summary := s.summary(instance)
		if summary.Online {
			result = append(result, summary)
		}
	}
	return result
}

func instanceFromSummary(summary Summary) Instance {
	instanceURL := summary.InstanceURL
	if instanceURL == "" && summary.Container != "" {
		instanceURL = "http://" + summary.Container + ":13339"
	}
	return Instance{Name: summary.Instance, Container: summary.Container, ContainerID: summary.ContainerID, Managed: summary.Managed, Status: summary.Status, URL: instanceURL, ProxyURLs: append([]string(nil), summary.ProxyURLs...), MaxConcurrency: summary.MaxConcurrency, QueueSize: summary.QueueSize}
}

func (s *Server) addRotationLog(level, message, instance string, fields map[string]any) {
	s.mu.Lock()
	s.rotationLogs = append(s.rotationLogs, SystemLog{At: time.Now(), Level: level, Message: message, Instance: instance, Fields: fields})
	if len(s.rotationLogs) > 200 {
		s.rotationLogs = s.rotationLogs[len(s.rotationLogs)-200:]
	}
	s.mu.Unlock()
}

func (s *Server) recordRotationFailure(key, instance string, err error) {
	message := err.Error()
	s.mu.Lock()
	if s.rotationFailures[key] == message {
		s.mu.Unlock()
		return
	}
	s.rotationFailures[key] = message
	s.mu.Unlock()
	s.addRotationLog("warn", "egress rotation failed", instance, map[string]any{"error": message})
}

func (s *Server) clearRotationFailure(key string) {
	s.mu.Lock()
	delete(s.rotationFailures, key)
	s.mu.Unlock()
}

func (s *Server) ReconcileEgresses() error {
	s.rotationMu.Lock()
	defer s.rotationMu.Unlock()
	summaries := s.runningSummaries()
	if len(summaries) < 1 {
		return nil
	}
	routeOwners := make(map[string]map[string]struct{})
	for _, summary := range summaries {
		for _, slot := range activeSummarySlots(summary) {
			raw := summarySlotString(slot, "url")
			if _, ok := mihomoGroupForProxyURL(raw); !ok {
				continue
			}
			if routeOwners[raw] == nil {
				routeOwners[raw] = make(map[string]struct{})
			}
			routeOwners[raw][summary.Instance] = struct{}{}
		}
	}
	occupied := make(map[string]map[string]string)
	rotated := make(map[string]struct{})
	var failures []string
	for _, summary := range summaries {
		instance := instanceFromSummary(summary)
		provider := providerOrDefault(summary.Provider)
		providerOccupied := occupied[provider]
		if providerOccupied == nil {
			providerOccupied = make(map[string]string)
			occupied[provider] = providerOccupied
		}
		for _, slot := range activeSummarySlots(summary) {
			if summarySlotBool(slot, "direct") {
				continue
			}
			raw := summarySlotString(slot, "url")
			egress := summarySlotString(slot, "egress")
			if net.ParseIP(egress) == nil {
				continue
			}
			owner, duplicate := providerOccupied[egress]
			if !duplicate {
				providerOccupied[egress] = summary.Instance
				continue
			}
			if owner == summary.Instance {
				continue
			}
			failureKey := summary.Instance + "|" + raw
			if len(routeOwners[raw]) > 1 && len(summary.Slots) < 2 {
				err := fmt.Errorf("Mihomo slot %s is shared by multiple instances; assign a unique local slot before rotation", raw)
				s.recordRotationFailure(failureKey, summary.Instance, err)
				failures = append(failures, err.Error())
				continue
			}
			forbidden := make(map[string]struct{}, len(providerOccupied))
			for ip := range providerOccupied {
				forbidden[ip] = struct{}{}
			}
			var result egressRotationResult
			var err error
			if len(summary.Slots) > 1 {
				result, err = s.rotateInstanceCandidate(instance, raw, egress, "duplicate_egress", forbidden)
			} else {
				result, err = s.rotateInstanceMihomoSlot(instance, raw, egress, "duplicate_egress", forbidden)
			}
			if err != nil {
				s.recordRotationFailure(failureKey, summary.Instance, err)
				failures = append(failures, err.Error())
				continue
			}
			s.clearRotationFailure(failureKey)
			providerOccupied[result.Egress] = summary.Instance
			rotated[summary.Instance] = struct{}{}
			s.addRotationLog("info", "duplicate egress rotated", summary.Instance, map[string]any{"previous_egress": result.PreviousEgress, "egress": result.Egress, "previous_node": result.PreviousNode, "node": result.Node, "attempts": result.Attempts})
		}
	}

	for _, summary := range summaries {
		current := summary.Stats["upstream429"]
		s.mu.Lock()
		previous, known := s.lastUpstream429[summary.Instance]
		s.lastUpstream429[summary.Instance] = current
		s.mu.Unlock()
		if !known || current <= previous {
			continue
		}
		if _, alreadyRotated := rotated[summary.Instance]; alreadyRotated {
			continue
		}
		// With multiple configured candidates the gateway already advanced its
		// sticky active slot while retrying the 429. Do not rotate Mihomo again.
		if managedMihomoSlotCount(summary) > 1 {
			continue
		}
		instance := instanceFromSummary(summary)
		provider := providerOrDefault(summary.Provider)
		providerOccupied := occupied[provider]
		if providerOccupied == nil {
			providerOccupied = make(map[string]string)
			occupied[provider] = providerOccupied
		}
		for _, slot := range activeSummarySlots(summary) {
			raw := summarySlotString(slot, "url")
			egress := summarySlotString(slot, "egress")
			if _, ok := mihomoGroupForProxyURL(raw); !ok || net.ParseIP(egress) == nil || len(routeOwners[raw]) > 1 {
				continue
			}
			forbidden := make(map[string]struct{}, len(providerOccupied)+1)
			for ip, owner := range providerOccupied {
				if owner != summary.Instance {
					forbidden[ip] = struct{}{}
				}
			}
			forbidden[egress] = struct{}{}
			result, err := s.rotateInstanceMihomoSlot(instance, raw, egress, "upstream_429", forbidden)
			if err != nil {
				s.recordRotationFailure(summary.Instance+"|429", summary.Instance, err)
				failures = append(failures, err.Error())
				break
			}
			s.clearRotationFailure(summary.Instance + "|429")
			s.addRotationLog("info", "upstream 429 rotated egress", summary.Instance, map[string]any{"previous_egress": result.PreviousEgress, "egress": result.Egress, "previous_node": result.PreviousNode, "node": result.Node, "attempts": result.Attempts})
			break
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d egress rotation(s) failed", len(failures))
	}
	return nil
}

func (s *Server) RefreshMihomoEgresses() error {
	s.rotationMu.Lock()
	defer s.rotationMu.Unlock()
	s.mu.RLock()
	instances := append([]Instance(nil), s.instances...)
	s.mu.RUnlock()
	var failures []string
	for _, instance := range instances {
		if instance.Status != "running" {
			continue
		}
		for _, raw := range instance.ProxyURLs {
			if _, ok := mihomoGroupForProxyURL(raw); !ok {
				continue
			}
			if _, err := s.probeInstanceProxy(instance, raw); err != nil {
				failures = append(failures, instance.Name+": "+err.Error())
				s.recordRotationFailure(instance.Name+"|refresh|"+raw, instance.Name, err)
			} else {
				s.clearRotationFailure(instance.Name + "|refresh|" + raw)
			}
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d Mihomo egress probe(s) failed", len(failures))
	}
	return nil
}

func (s *Server) RotateInstanceEgress(name string) (egressRotationResult, error) {
	s.rotationMu.Lock()
	defer s.rotationMu.Unlock()
	summaries := s.runningSummaries()
	var target Summary
	for _, summary := range summaries {
		if summary.Instance == name {
			target = summary
			break
		}
	}
	if target.Instance == "" {
		return egressRotationResult{}, fmt.Errorf("instance %q is not running", name)
	}
	forbidden := make(map[string]struct{})
	targetProvider := providerOrDefault(target.Provider)
	routeOwners := make(map[string]map[string]struct{})
	for _, summary := range summaries {
		if providerOrDefault(summary.Provider) != targetProvider {
			continue
		}
		for _, slot := range activeSummarySlots(summary) {
			raw := summarySlotString(slot, "url")
			egress := summarySlotString(slot, "egress")
			if raw != "" {
				if routeOwners[raw] == nil {
					routeOwners[raw] = make(map[string]struct{})
				}
				routeOwners[raw][summary.Instance] = struct{}{}
			}
			if summary.Instance != name && net.ParseIP(egress) != nil {
				forbidden[egress] = struct{}{}
			}
		}
	}
	instance := instanceFromSummary(target)
	for _, slot := range activeSummarySlots(target) {
		raw := summarySlotString(slot, "url")
		egress := summarySlotString(slot, "egress")
		if _, ok := mihomoGroupForProxyURL(raw); !ok {
			continue
		}
		if len(routeOwners[raw]) > 1 && len(target.Slots) < 2 {
			return egressRotationResult{}, fmt.Errorf("Mihomo slot %s is shared by multiple instances", raw)
		}
		forbidden[egress] = struct{}{}
		var result egressRotationResult
		var err error
		if len(target.Slots) > 1 {
			result, err = s.rotateInstanceCandidate(instance, raw, egress, "manual", forbidden)
		} else {
			result, err = s.rotateInstanceMihomoSlot(instance, raw, egress, "manual", forbidden)
		}
		if err != nil {
			return result, err
		}
		s.addRotationLog("info", "egress rotated manually", name, map[string]any{"previous_egress": result.PreviousEgress, "egress": result.Egress, "previous_node": result.PreviousNode, "node": result.Node, "attempts": result.Attempts})
		return result, nil
	}
	return egressRotationResult{}, fmt.Errorf("instance %q has no managed Mihomo proxy slot", name)
}
