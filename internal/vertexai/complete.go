package vertexai

import (
	"context"
	"sort"
)

// CompleteChat races one non-streaming generation across the candidate exits;
// the first candidate finishing with STOP wins, non-STOP results are collected
// and the best is chosen.
func (c *VertexAIClient) CompleteChat(ctx context.Context, exits []string, model string, geminiPayload map[string]any, opts ...RaceOption[map[string]any]) (map[string]any, error) {
	routedCtx, err := c.prepareRequest(ctx)
	if err != nil {
		return nil, NewAuthenticationError("Could not fetch recaptcha token: "+err.Error(), err)
	}
	opts = append(opts,
		WithWinningCheck(func(resp map[string]any) bool {
			return candidateFinish(resp) == "STOP"
		}),
		WithCollectedFinalizer(func(results []raceResult[map[string]any]) (map[string]any, error) {
			cr := make([]candidateResult, len(results))
			for i, r := range results {
				cr[i] = candidateResult{proxyURI: r.uri, resp: r.val, err: r.err}
			}
			return pickBestResult(cr)
		}),
	)
	return RunRaceURIs(routedCtx, c.cfg, exits,
		func(ctx context.Context, proxyURI string) (map[string]any, error) {
			copied := deepCopyAny(geminiPayload).(map[string]any)
			return c.runSingleCandidate(ctx, model, copied, proxyURI)
		},
		opts...,
	)
}

type candidateResult struct {
	proxyURI string
	resp     map[string]any
	err      error
}

func pickBestResult(results []candidateResult) (map[string]any, error) {
	sort.Slice(results, func(i, j int) bool {
		fi := candidateFinish(results[i].resp)
		fj := candidateFinish(results[j].resp)
		if fi == "MAX_TOKENS" && fj != "MAX_TOKENS" {
			return true
		}
		if fj == "MAX_TOKENS" && fi != "MAX_TOKENS" {
			return false
		}
		return responseContentLength(results[i].resp) > responseContentLength(results[j].resp)
	})
	return results[0].resp, nil
}

func responseContentLength(resp map[string]any) int {
	cands, ok := resp["candidates"].([]any)
	if !ok || len(cands) == 0 {
		return 0
	}
	c, ok := cands[0].(map[string]any)
	if !ok {
		return 0
	}
	content, ok := c["content"].(map[string]any)
	if !ok {
		return 0
	}
	parts, ok := content["parts"].([]any)
	if !ok {
		return 0
	}
	total := 0
	for _, pRaw := range parts {
		p, ok := pRaw.(map[string]any)
		if !ok {
			continue
		}
		total += len(toStr(p["text"]))
	}
	return total
}

// CompleteChatN produces n independent generations sequentially over the same
// exit set.
func (c *VertexAIClient) CompleteChatN(ctx context.Context, exits []string, model string, geminiPayload map[string]any, n int, opts ...RaceOption[map[string]any]) ([]map[string]any, error) {
	if n < 1 {
		n = 1
	}
	var ok []map[string]any
	var firstErr error
	for i := 0; i < n; i++ {
		resp, err := c.CompleteChat(ctx, exits, model, geminiPayload, opts...)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		ok = append(ok, resp)
	}
	if len(ok) == 0 {
		if firstErr == nil {
			firstErr = NewInternalError("All candidates failed")
		}
		return nil, firstErr
	}
	return ok, nil
}

func (c *VertexAIClient) runSingleCandidate(ctx context.Context, model string, geminiPayload map[string]any, proxyURI string) (map[string]any, error) {
	var chunks []map[string]any
	var firstErr *VertexError

	c.executeStreamingWithRetries(ctx, model, geminiPayload, proxyURI, func(chunk StreamChunk) bool {
		if chunk.Err != nil {
			if firstErr == nil {
				firstErr = chunk.Err
			}
			return false
		}
		if chunk.Data != nil {
			chunks = append(chunks, chunk.Data)
		}
		return true
	})

	if firstErr != nil {
		return nil, firstErr
	}
	if len(chunks) == 0 {
		return nil, NewEmptyResponseError("Upstream returned no data")
	}

	result := collectChunksToParseResult(chunks)
	resp, err := c.buildCompleteResponse(result)
	if err != nil {
		return nil, err
	}

	if _, hasSafety := geminiPayload["safetySettings"]; candidateFinish(resp) == "SAFETY" && !hasSafety {
		retryPayload := shallowCopy(geminiPayload)
		retryPayload["safetySettings"] = defaultSafetySettings
		return c.runSingleCandidate(ctx, model, retryPayload, proxyURI)
	}

	return resp, nil
}
