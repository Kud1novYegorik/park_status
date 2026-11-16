package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	providerTimeout = 2 * time.Second
	maxProviderBody = 1 << 20 // 1 MiB

	providerRate  = 10.0
	providerBurst = 10

	breakerFailureThreshold = 5
	breakerOpenDuration     = 10 * time.Second
)

var (
	ErrProviderNotConfigured = errors.New("provider is not configured")
	ErrProviderNotFound      = errors.New("unit not found at provider")
	ErrProviderBodyTooLarge  = errors.New("provider response is too large")
	ErrCircuitOpen           = errors.New("provider circuit breaker is open")
	ErrInvalidUnitID         = errors.New("invalid unit ID")
)

type ProviderStatusError struct {
	StatusCode int
	RetryAfter time.Duration
}

func (e *ProviderStatusError) Error() string {
	return fmt.Sprintf("provider returned HTTP %d", e.StatusCode)
}

type ProviderClient struct {
	baseURL    *url.URL
	httpClient *http.Client

	flights singleflightGroup
	limiter *rateLimiter
	breaker *circuitBreaker

	reads atomic.Int64
}

func NewProviderClient(baseURL string) (*ProviderClient, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, ErrProviderNotConfigured
	}

	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse provider URL: %w", err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("unsupported provider URL scheme: %q", u.Scheme)
	}

	if u.Host == "" {
		return nil, errors.New("provider URL must contain a host")
	}

	if u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("provider URL must not contain query or fragment")
	}

	return &ProviderClient{
		baseURL: u,
		httpClient: &http.Client{
			Timeout: providerTimeout,
		},
		limiter: newRateLimiter(providerRate, providerBurst),
		breaker: newCircuitBreaker(
			breakerFailureThreshold,
			breakerOpenDuration,
		),
	}, nil
}

// ProviderReads возвращает количество фактических HTTP-запросов к провайдеру.
// Запросы, объединённые Singleflight, учитываются один раз.
func (c *ProviderClient) ProviderReads() int64 {
	return c.reads.Load()
}

// GetUnit получает JSON машины от провайдера.
func (c *ProviderClient) GetUnit(
	ctx context.Context,
	id string,
) ([]byte, error) {
	if c == nil || c.baseURL == nil {
		return nil, ErrProviderNotConfigured
	}

	if !validProviderUnitID(id) {
		return nil, ErrInvalidUnitID
	}

	body, err := c.flights.Do(
		ctx,
		id,
		providerTimeout,
		func(sharedCtx context.Context) ([]byte, error) {
			return c.getUnit(sharedCtx, id)
		},
	)
	if err != nil {
		return nil, err
	}

	// Не отдаём вызывающим один и тот же изменяемый срез.
	return append([]byte(nil), body...), nil
}

func validProviderUnitID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}

	for i := 0; i < len(id); i++ {
		ch := id[i]
		if !((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '-') {
			return false
		}
	}

	return true
}

func (c *ProviderClient) getUnit(
	ctx context.Context,
	id string,
) ([]byte, error) {
	permit, err := c.breaker.Allow()
	if err != nil {
		return nil, err
	}

	if err := c.limiter.Wait(ctx); err != nil {
		c.breaker.Abandon(permit)
		return nil, err
	}

	u := *c.baseURL
	u.Path = strings.TrimRight(u.Path, "/") + "/units/" + id
	u.RawPath = ""

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		u.String(),
		nil,
	)
	if err != nil {
		c.breaker.Abandon(permit)
		return nil, fmt.Errorf("create provider request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	c.reads.Add(1)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.breaker.Failure(permit)
		return nil, fmt.Errorf("provider request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(
			io.LimitReader(resp.Body, maxProviderBody+1),
		)
		if err != nil {
			c.breaker.Failure(permit)
			return nil, fmt.Errorf("read provider response: %w", err)
		}

		if len(body) > maxProviderBody {
			c.breaker.Failure(permit)
			return nil, ErrProviderBodyTooLarge
		}

		c.breaker.Success(permit)
		return body, nil

	case http.StatusNotFound:
		// Провайдер доступен, но такой машины у него нет.
		c.breaker.Success(permit)
		return nil, ErrProviderNotFound

	case http.StatusTooManyRequests:
		retryAfter := parseRetryAfter(
			resp.Header.Get("Retry-After"),
		)

		c.limiter.Cooldown(retryAfter)

		// 429 — ограничение частоты, а не отказ доступности.
		c.breaker.Success(permit)

		return nil, &ProviderStatusError{
			StatusCode: resp.StatusCode,
			RetryAfter: retryAfter,
		}

	default:
		if resp.StatusCode >= 500 ||
			resp.StatusCode == http.StatusRequestTimeout {
			c.breaker.Failure(permit)
		} else {
			// Остальные 4xx означают, что провайдер ответил.
			c.breaker.Success(permit)
		}

		return nil, &ProviderStatusError{
			StatusCode: resp.StatusCode,
		}
	}
}

// -----------------------------------------------------------------------------
// Singleflight
// -----------------------------------------------------------------------------

type flightCall struct {
	done   chan struct{}
	body   []byte
	err    error
	cancel context.CancelFunc
}

type singleflightGroup struct {
	mu    sync.Mutex
	calls map[string]*flightCall
}

// Do объединяет запросы с одинаковым ключом.
//
// Общий запрос использует собственный контекст с таймаутом.
// Поэтому отмена контекста одного ожидающего клиента не отменяет
// запрос, который нужен другим клиентам.
func (g *singleflightGroup) Do(
	ctx context.Context,
	key string,
	timeout time.Duration,
	fn func(context.Context) ([]byte, error),
) ([]byte, error) {
	g.mu.Lock()

	if g.calls == nil {
		g.calls = make(map[string]*flightCall)
	}

	if call, ok := g.calls[key]; ok {
		g.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-call.done:
			return call.body, call.err
		}
	}

	sharedCtx, cancel := context.WithTimeout(
		context.Background(),
		timeout,
	)

	call := &flightCall{
		done:   make(chan struct{}),
		cancel: cancel,
	}

	g.calls[key] = call
	g.mu.Unlock()

	// Общий запрос выполняется независимо от контекста первого клиента.
	go func() {
		body, err := fn(sharedCtx)

		g.mu.Lock()

		call.body = body
		call.err = err

		delete(g.calls, key)
		close(call.done)

		g.mu.Unlock()

		call.cancel()
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case <-call.done:
		return call.body, call.err
	}
}

// -----------------------------------------------------------------------------
// Rate limiter: Token Bucket
// -----------------------------------------------------------------------------

type rateLimiter struct {
	mu sync.Mutex

	rate   float64
	burst  float64
	tokens float64

	last       time.Time
	blockedTil time.Time
}

func newRateLimiter(rate float64, burst int) *rateLimiter {
	if rate <= 0 {
		rate = 1
	}
	if burst < 1 {
		burst = 1
	}

	now := time.Now()

	return &rateLimiter{
		rate:   rate,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   now,
	}
}

func (l *rateLimiter) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		l.mu.Lock()

		now := time.Now()

		if now.Before(l.blockedTil) {
			wait := time.Until(l.blockedTil)
			l.mu.Unlock()

			if err := waitContext(ctx, wait); err != nil {
				return err
			}
			continue
		}

		elapsed := now.Sub(l.last).Seconds()
		if elapsed > 0 {
			l.tokens += elapsed * l.rate
			if l.tokens > l.burst {
				l.tokens = l.burst
			}
			l.last = now
		}

		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		}

		waitSeconds := (1 - l.tokens) / l.rate
		wait := time.Duration(waitSeconds * float64(time.Second))

		// Не допускаем нулевого ожидания из-за округления.
		if wait <= 0 {
			wait = time.Nanosecond
		}

		l.mu.Unlock()

		if err := waitContext(ctx, wait); err != nil {
			return err
		}
	}
}

// Cooldown приостанавливает выдачу токенов после 429.
func (l *rateLimiter) Cooldown(delay time.Duration) {
	if delay <= 0 {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	until := time.Now().Add(delay)
	if until.After(l.blockedTil) {
		l.blockedTil = until
	}

	// После 429 не выдаём сразу накопленный burst.
	l.tokens = 0
	l.last = time.Now()
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-timer.C:
		return nil
	}
}

// -----------------------------------------------------------------------------
// Circuit Breaker
// -----------------------------------------------------------------------------

type breakerState uint8

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

type breakerPermit struct {
	generation uint64
}

type circuitBreaker struct {
	mu sync.Mutex

	state breakerState

	failures  int
	threshold int

	openDuration time.Duration
	openedAt     time.Time

	generation    uint64
	probeInFlight bool
}

func newCircuitBreaker(
	threshold int,
	openDuration time.Duration,
) *circuitBreaker {
	if threshold < 1 {
		threshold = 1
	}
	if openDuration <= 0 {
		openDuration = time.Second
	}

	return &circuitBreaker{
		state:        breakerClosed,
		threshold:    threshold,
		openDuration: openDuration,
	}
}

// Allow выдаёт разрешение на один запрос.
func (b *circuitBreaker) Allow() (breakerPermit, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case breakerClosed:
		return breakerPermit{
			generation: b.generation,
		}, nil

	case breakerOpen:
		if time.Since(b.openedAt) < b.openDuration {
			return breakerPermit{}, ErrCircuitOpen
		}

		b.state = breakerHalfOpen
		b.probeInFlight = true

		return breakerPermit{
			generation: b.generation,
		}, nil

	case breakerHalfOpen:
		// В Half-Open разрешён только один проверочный запрос.
		return breakerPermit{}, ErrCircuitOpen

	default:
		return breakerPermit{}, ErrCircuitOpen
	}
}

func (b *circuitBreaker) Success(permit breakerPermit) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Результат устаревшего запроса не меняет новое состояние breaker.
	if permit.generation != b.generation {
		return
	}

	switch b.state {
	case breakerClosed:
		b.failures = 0

	case breakerHalfOpen:
		if !b.probeInFlight {
			return
		}

		b.state = breakerClosed
		b.failures = 0
		b.probeInFlight = false
		b.generation++
	}
}

func (b *circuitBreaker) Failure(permit breakerPermit) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if permit.generation != b.generation {
		return
	}

	switch b.state {
	case breakerClosed:
		b.failures++

		if b.failures >= b.threshold {
			b.openLocked()
		}

	case breakerHalfOpen:
		if !b.probeInFlight {
			return
		}

		b.openLocked()
	}
}

// Abandon вызывается, если запрос не дошёл до HTTP из-за отмены контекста.
// В Half-Open освобождает проверочный слот.
func (b *circuitBreaker) Abandon(permit breakerPermit) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if permit.generation != b.generation {
		return
	}

	if b.state == breakerHalfOpen {
		b.probeInFlight = false

		// Возвращаемся в Open. Время ожидания не перезапускаем:
		// следующий запрос сможет снова попробовать проверку.
		b.state = breakerOpen
	}
}

func (b *circuitBreaker) openLocked() {
	b.state = breakerOpen
	b.openedAt = time.Now()
	b.probeInFlight = false
	b.failures = 0
	b.generation++
}

// -----------------------------------------------------------------------------
// Retry-After
// -----------------------------------------------------------------------------

func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}

	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}

		maxSeconds := int64(math.MaxInt64 / int64(time.Second))
		if seconds > maxSeconds {
			return time.Duration(math.MaxInt64)
		}

		return time.Duration(seconds) * time.Second
	}

	t, err := http.ParseTime(value)
	if err != nil {
		return 0
	}

	delay := time.Until(t)
	if delay < 0 {
		return 0
	}

	return delay
}
