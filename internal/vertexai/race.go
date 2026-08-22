package vertexai

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"dualroute-gateway/internal/config"
)

// raceConfig 是竞速层的可配置策略。
type raceConfig[T any] struct {
	noCancelOnSuccess bool
	isWinningResult   func(val T) bool
	collectedResults  []raceResult[T]
	finalizeCollected func([]raceResult[T]) (T, error)
	// onResult 上报每个候选的最终结局（成功或失败），供调用方维护
	// 跨请求的出口限流记忆（对应原版 RecordRateLimit/RecordTest 语义）。
	onResult func(uri string, err error)
}

type RaceOption[T any] func(*raceConfig[T])

// WithOnResult 注入候选结果上报回调；err 为 nil 表示该出口成功。
func WithOnResult[T any](fn func(uri string, err error)) RaceOption[T] {
	return func(cfg *raceConfig[T]) { cfg.onResult = fn }
}

type raceRoundKey struct{}

func WithNoCancelOnSuccess[T any]() RaceOption[T] {
	return func(cfg *raceConfig[T]) { cfg.noCancelOnSuccess = true }
}

func WithWinningCheck[T any](fn func(T) bool) RaceOption[T] {
	return func(cfg *raceConfig[T]) { cfg.isWinningResult = fn }
}

func WithCollectedFinalizer[T any](fn func([]raceResult[T]) (T, error)) RaceOption[T] {
	return func(cfg *raceConfig[T]) { cfg.finalizeCollected = fn }
}

type raceResult[T any] struct {
	uri string
	val T
	err error
}

func errorPriority(err error) int {
	var ve *VertexError
	if errors.As(err, &ve) {
		if ve.IsRetryable() {
			if ve.Kind == "ratelimit" || ve.Code == 429 {
				return 1
			}
			return 2
		}
		if ve.IsGlobalHardError() {
			return 3
		}
		return 4
	}
	return 5
}

func pickBestError(errs []error) error {
	if len(errs) == 0 {
		return fmt.Errorf("all exits failed")
	}
	best := errs[0]
	bestPrio := errorPriority(best)
	for _, e := range errs[1:] {
		if p := errorPriority(e); p < bestPrio {
			best = e
			bestPrio = p
		}
	}
	return best
}

// RunRaceURIs 在给定的候选出口列表上执行对冲竞速（移植自 vproxy RunRace）。
//
// 模型：每轮先启动首个候选，其余按固定延迟接力；已启动候选全部提前失败时
// 立即启动下一个候选。所有候选失败后按 MaxRetries 换批重试（复用同一列表，
// 优先未用过的出口）。
func RunRaceURIs[T any](ctx context.Context, cfg config.ConfigProvider, uris []string,
	run func(context.Context, string) (T, error),
	opts ...RaceOption[T],
) (T, error) {
	var rc raceConfig[T]
	for _, o := range opts {
		o(&rc)
	}

	raceTimeout := cfg.RaceTimeout()

	roundBudget := 0
	if cfg.ParallelPoolEnabled() && !cfg.ParallelPoolRetryEnabled() {
		roundBudget = cfg.MaxRetries()
	}

	cands := make([]string, 0, len(uris))
	for _, u := range uris {
		cands = append(cands, u)
	}
	if len(cands) == 0 {
		cands = []string{""}
	}

	usedURIs := make(map[string]bool)
	var zero T

	if route := routeFromContext(ctx); route != nil && route.authCandidateBound && route.token != nil {
		route.token.setRefreshLimit(len(cands) - 1)
	}

	ctxRace, cancel := context.WithCancel(ctx)
	var returnedOnWinPath bool
	defer func() {
		if !returnedOnWinPath || !rc.noCancelOnSuccess {
			cancel()
		}
	}()

	var cancelsMu sync.Mutex
	cancels := make(map[string]context.CancelFunc)
	cancelCandidate := func(uri string) {
		cancelsMu.Lock()
		cancelFn := cancels[uri]
		cancelsMu.Unlock()
		if cancelFn != nil {
			cancelFn()
		}
	}

	report := func(uri string, err error) {
		if rc.onResult != nil {
			rc.onResult(uri, err)
		}
	}
	recordResult := func(res raceResult[T]) {
		if res.err == nil {
			report(res.uri, nil)
			return
		}
		if errors.Is(res.err, context.Canceled) {
			return
		}
		if ve := asVertexError(res.err); ve != nil && (ve.requestTokenTerminal || ve.requestTokenInvalid) {
			return
		}
		report(res.uri, res.err)
	}

	var failedErrors []error

	for round := 0; ; round++ {
		resCh := make(chan raceResult[T], len(cands))
		var active int32

		launchCandidate := func(uri string) {
			usedURIs[uri] = true
			candCtx, candCancel := context.WithCancel(ctxRace)
			candCtx = context.WithValue(candCtx, raceRoundKey{}, round)
			cancelsMu.Lock()
			cancels[uri] = candCancel
			cancelsMu.Unlock()

			atomic.AddInt32(&active, 1)
			go func(u string, candidateCtx context.Context, candidateCancel context.CancelFunc) {
				resultReady := make(chan raceResult[T], 1)
				go func() {
					result := raceResult[T]{uri: u}
					func() {
						defer func() {
							if recovered := recover(); recovered != nil {
								result.err = NewInternalError(fmt.Sprintf("exit %s candidate panic: %v", u, recovered))
							}
						}()
						result.val, result.err = run(candidateCtx, u)
					}()
					resultReady <- result
				}()

				if raceTimeout <= 0 {
					select {
					case result := <-resultReady:
						resCh <- result
					case <-candidateCtx.Done():
						select {
						case result := <-resultReady:
							resCh <- result
						default:
							resCh <- raceResult[T]{uri: u, err: candidateCtx.Err()}
						}
					}
					return
				}

				timer := time.NewTimer(time.Duration(raceTimeout) * time.Second)
				defer timer.Stop()
				select {
				case result := <-resultReady:
					resCh <- result
				case <-timer.C:
					candidateCancel()
					resCh <- raceResult[T]{
						uri: u,
						err: NewUnavailableError(fmt.Sprintf("exit %s raced past %d seconds, eliminated", u, raceTimeout)),
					}
				case <-candidateCtx.Done():
					select {
					case result := <-resultReady:
						resCh <- result
					default:
						resCh <- raceResult[T]{uri: u, err: candidateCtx.Err()}
					}
				}
			}(uri, candCtx, candCancel)
		}

		delay := time.Duration(cfg.ParallelPoolDelayMs()) * time.Millisecond
		if delay < 0 {
			delay = 0
		}

		nextIdx := 0
		launchNext := func() bool {
			if nextIdx >= len(cands) {
				return false
			}
			candidate := cands[nextIdx]
			nextIdx++
			launchCandidate(candidate)
			return true
		}
		launchNext()

		timer := time.NewTimer(delay)
		if nextIdx >= len(cands) {
			if !timer.Stop() {
				<-timer.C
			}
		}
		resetTimer := func() {
			if nextIdx >= len(cands) {
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(delay)
		}

	InnerLoop:
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				cancel()
				return zero, ctx.Err()

			case <-timer.C:
				if nextIdx < len(cands) {
					launchNext()
					resetTimer()
				}

			case res := <-resCh:
				atomic.AddInt32(&active, -1)
				if parentErr := ctx.Err(); parentErr != nil {
					timer.Stop()
					cancel()
					return zero, parentErr
				}

				if res.err == nil {
					if rc.isWinningResult == nil || rc.isWinningResult(res.val) {
						timer.Stop()
						log.Printf("[Racing] exit won: %s", res.uri)
						report(res.uri, nil)
						returnedOnWinPath = true

						cancelsMu.Lock()
						for u, cancelFn := range cancels {
							if u != res.uri {
								cancelFn()
							}
						}
						cancelsMu.Unlock()

						collectTimeout := time.Duration(min(30, 5+cfg.ParallelPoolSize())) * time.Second
						go func() {
							collectCtx, collectCancel := context.WithTimeout(context.Background(), collectTimeout)
							defer collectCancel()
							if atomic.LoadInt32(&active) == 0 {
								if !rc.noCancelOnSuccess {
									cancel()
								}
								return
							}
							for {
								select {
								case bgRes := <-resCh:
									atomic.AddInt32(&active, -1)
									recordResult(bgRes)
									if atomic.LoadInt32(&active) == 0 {
										if !rc.noCancelOnSuccess {
											cancel()
										}
										return
									}
								case <-collectCtx.Done():
									if !rc.noCancelOnSuccess {
										cancel()
									}
									return
								}
							}
						}()

						return res.val, nil
					}

					cancelCandidate(res.uri)
					report(res.uri, nil)
					rc.collectedResults = append(rc.collectedResults, res)
				} else {
					cancelCandidate(res.uri)
					if !errors.Is(res.err, context.Canceled) {
						ve := asVertexError(res.err)
						if ve != nil && ve.requestTokenTerminal {
							cancel()
							return zero, res.err
						}
						if ve != nil && ve.requestTokenInvalid {
							route := routeFromContext(ctx)
							if route == nil || route.token == nil {
								failedErrors = append(failedErrors, res.err)
								continue
							}
							invalidated := route.token.invalidateToken(ve.requestToken)
							if invalidated.exhausted {
								cancel()
								return zero, NewInternalError("recaptcha token remained invalid after request recovery").markRequestTokenTerminal()
							}
							if invalidated.refreshed {
								remaining := make([]string, 0, len(cands)-1)
								for _, candidate := range cands {
									if candidate != res.uri {
										remaining = append(remaining, candidate)
									}
								}
								cands = remaining
								cancelsMu.Lock()
								for u, cancelFn := range cancels {
									if u != res.uri {
										cancelFn()
									}
								}
								cancelsMu.Unlock()
								if len(cands) == 0 {
									cancel()
									return zero, NewInternalError("recaptcha token remained invalid after request recovery").markRequestTokenTerminal()
								}
								timer.Stop()
								break InnerLoop
							}
							continue
						}

						failedErrors = append(failedErrors, res.err)

						if ve != nil && ve.IsGlobalHardError() {
							cancel()
							return zero, res.err
						}
					}
				}

				if atomic.LoadInt32(&active) == 0 && nextIdx < len(cands) {
					launchNext()
					resetTimer()
					continue
				}

				if atomic.LoadInt32(&active) == 0 && nextIdx >= len(cands) {
					timer.Stop()
					if parentErr := ctx.Err(); parentErr != nil {
						cancel()
						return zero, parentErr
					}
					if len(rc.collectedResults) > 0 {
						cancel()
						if rc.finalizeCollected != nil {
							return rc.finalizeCollected(rc.collectedResults)
						}
						return rc.collectedResults[0].val, nil
					}

					if roundBudget > 0 {
						next := make([]string, 0, len(cands))
						for _, candidate := range cands {
							if !usedURIs[candidate] {
								next = append(next, candidate)
							}
						}
						if len(next) == 0 {
							// 所有出口都试过：清空防重过滤，允许跨轮次复用。
							usedURIs = make(map[string]bool)
							next = append(next, cands...)
						}
						if len(next) == 0 {
							cancel()
							if len(failedErrors) > 0 {
								return zero, pickBestError(failedErrors)
							}
							return zero, fmt.Errorf("all exits failed")
						}
						roundBudget--
						cands = next
						break InnerLoop
					}
					cancel()
					if len(failedErrors) > 0 {
						return zero, pickBestError(failedErrors)
					}
					return zero, fmt.Errorf("all exits failed")
				}
			}
		}
	}
}

// streamParallelURIs 在候选出口上并发启动流式请求：首个产出数据块且无错误的
// 候选胜出，其余被取消；胜出流继续传输直到结束。
func streamParallelURIs(ctx context.Context, cfg config.ConfigProvider, exits []string,
	op func(context.Context, string) <-chan StreamChunk,
	yield func(StreamChunk) bool,
	opts ...RaceOption[<-chan StreamChunk],
) {
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	wrappedOp := func(ctx context.Context, uri string) (<-chan StreamChunk, error) {
		ch := op(ctx, uri)
		var first StreamChunk
		var ok bool
		select {
		case first, ok = <-ch:
			if !ok {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return nil, NewEmptyResponseError(fmt.Sprintf("stream: %s closed with no data", uri))
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		if first.Err != nil {
			return nil, first.Err
		}
		rest := make(chan StreamChunk, 64)
		rest <- first
		go func() {
			defer close(rest)
			for chunk := range ch {
				select {
				case rest <- chunk:
				case <-ctx.Done():
					return
				}
			}
		}()
		return rest, nil
	}

		allOpts := append([]RaceOption[<-chan StreamChunk]{WithNoCancelOnSuccess[<-chan StreamChunk]()}, opts...)
	winnerCh, err := RunRaceURIs(streamCtx, cfg, exits, wrappedOp, allOpts...)
	if err != nil {
		var vertexErr *VertexError
		if errors.As(err, &vertexErr) {
			yield(StreamChunk{Err: vertexErr})
		} else {
			yield(StreamChunk{Err: NewInternalError(err.Error())})
		}
		return
	}
	for chunk := range winnerCh {
		if !yield(chunk) {
			return
		}
	}
}