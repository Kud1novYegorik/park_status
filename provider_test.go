package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProviderClientHTTPStatuses(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		retryAfter string
		wantErr    error
	}{
		{
			name:   "success",
			status: http.StatusOK,
			body:   `{"unit":"truck-1"}`,
		},
		{
			name:    "not found",
			status:  http.StatusNotFound,
			wantErr: ErrProviderNotFound,
		},
		{
			name:       "rate limit",
			status:     http.StatusTooManyRequests,
			retryAfter: "5",
		},
		{
			name:   "server error",
			status: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodGet {
						t.Errorf("unexpected method: %s", r.Method)
					}

					if r.URL.Path != "/units/truck-1" {
						t.Errorf("unexpected path: %s", r.URL.Path)
					}

					if tt.retryAfter != "" {
						w.Header().Set("Retry-After", tt.retryAfter)
					}

					w.WriteHeader(tt.status)
					_, _ = w.Write([]byte(tt.body))
				},
			))
			defer server.Close()

			client, err := NewProviderClient(server.URL)
			if err != nil {
				t.Fatal(err)
			}

			body, err := client.GetUnit(
				context.Background(),
				"truck-1",
			)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				return
			}

			if tt.status == http.StatusTooManyRequests {
				var statusErr *ProviderStatusError
				if !errors.As(err, &statusErr) {
					t.Fatalf("expected ProviderStatusError, got %v", err)
				}

				if statusErr.RetryAfter != 5*time.Second {
					t.Fatalf("unexpected Retry-After: %v", statusErr.RetryAfter)
				}
				return
			}

			if tt.status >= 500 {
				var statusErr *ProviderStatusError
				if !errors.As(err, &statusErr) {
					t.Fatalf("expected ProviderStatusError, got %v", err)
				}
				if statusErr.StatusCode != tt.status {
					t.Fatalf("unexpected status code: %d", statusErr.StatusCode)
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if string(body) != tt.body {
				t.Fatalf("expected %q, got %q", tt.body, body)
			}

			if client.ProviderReads() != 1 {
				t.Fatalf("expected one provider read, got %d", client.ProviderReads())
			}
		})
	}
}

func TestSingleflightDeduplicatesRequests(t *testing.T) {
	var requests atomic.Int64
	release := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			<-release
			_, _ = w.Write([]byte(`{"unit":"truck-1"}`))
		},
	))
	defer server.Close()

	client, err := NewProviderClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 20

	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			body, err := client.GetUnit(
				context.Background(),
				"truck-1",
			)
			if err != nil {
				errs <- err
				return
			}

			if string(body) != `{"unit":"truck-1"}` {
				errs <- errors.New("unexpected response body")
			}
		}()
	}

	// Ждём, пока первый запрос дойдёт до провайдера.
	deadline := time.After(time.Second)
	for requests.Load() == 0 {
		select {
		case <-deadline:
			close(release)
			t.Fatal("provider request was not started")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Даём остальным горутинам присоединиться к тому же запросу.
	time.Sleep(30 * time.Millisecond)
	close(release)

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	if got := requests.Load(); got != 1 {
		t.Fatalf("expected one HTTP request, got %d", got)
	}
}

func TestSingleflightLeaderCancellationDoesNotCancelFollowers(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-release
			_, _ = w.Write([]byte(`{"unit":"truck-1"}`))
		},
	))
	defer server.Close()

	client, err := NewProviderClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderResult := make(chan error, 1)

	go func() {
		_, err := client.GetUnit(leaderCtx, "truck-1")
		leaderResult <- err
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		cancelLeader()
		close(release)
		t.Fatal("provider request was not started")
	}

	cancelLeader()

	if err := <-leaderResult; !errors.Is(err, context.Canceled) {
		close(release)
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	followerResult := make(chan error, 1)

	go func() {
		body, err := client.GetUnit(context.Background(), "truck-1")
		if err == nil && string(body) != `{"unit":"truck-1"}` {
			err = errors.New("unexpected response body")
		}
		followerResult <- err
	}()

	time.Sleep(20 * time.Millisecond)
	close(release)

	select {
	case err := <-followerResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("follower did not receive the shared response")
	}

	if got := client.ProviderReads(); got != 1 {
		t.Fatalf("expected one provider read, got %d", got)
	}
}

func TestRateLimiterCancellation(t *testing.T) {
	limiter := newRateLimiter(1, 1)

	// Первый токен доступен сразу.
	if err := limiter.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()

	start := time.Now()
	err := limiter.Wait(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}

	if time.Since(start) > time.Second {
		t.Fatal("limiter did not respect context deadline")
	}
}

func TestRateLimiterCooldown(t *testing.T) {
	limiter := newRateLimiter(100, 10)
	limiter.Cooldown(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()

	err := limiter.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded during cooldown, got %v", err)
	}
}

func TestCircuitBreakerTransitions(t *testing.T) {
	breaker := newCircuitBreaker(2, 50*time.Millisecond)

	first, err := breaker.Allow()
	if err != nil {
		t.Fatal(err)
	}
	breaker.Failure(first)

	second, err := breaker.Allow()
	if err != nil {
		t.Fatal(err)
	}
	breaker.Failure(second)

	if _, err := breaker.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected open breaker, got %v", err)
	}

	time.Sleep(60 * time.Millisecond)

	probe, err := breaker.Allow()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := breaker.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected only one half-open probe, got %v", err)
	}

	breaker.Success(probe)

	if _, err := breaker.Allow(); err != nil {
		t.Fatalf("expected closed breaker, got %v", err)
	}
}

func TestCircuitBreakerIgnoresStaleResults(t *testing.T) {
	breaker := newCircuitBreaker(1, 30*time.Millisecond)

	oldPermit, err := breaker.Allow()
	if err != nil {
		t.Fatal(err)
	}

	breaker.Failure(oldPermit)

	time.Sleep(40 * time.Millisecond)

	probe, err := breaker.Allow()
	if err != nil {
		t.Fatal(err)
	}

	// Запоздавший успех старого запроса не должен закрыть breaker.
	breaker.Success(oldPermit)

	if _, err := breaker.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("stale result changed breaker state: %v", err)
	}

	breaker.Success(probe)

	if _, err := breaker.Allow(); err != nil {
		t.Fatalf("expected closed breaker after successful probe, got %v", err)
	}
}

func TestCircuitBreakerAbandonProbe(t *testing.T) {
	breaker := newCircuitBreaker(1, 30*time.Millisecond)

	permit, err := breaker.Allow()
	if err != nil {
		t.Fatal(err)
	}
	breaker.Failure(permit)

	time.Sleep(40 * time.Millisecond)

	probe, err := breaker.Allow()
	if err != nil {
		t.Fatal(err)
	}

	breaker.Abandon(probe)

	// После освобождения слота следующий запрос может повторить probe.
	nextProbe, err := breaker.Allow()
	if err != nil {
		t.Fatalf("expected another half-open probe, got %v", err)
	}

	breaker.Success(nextProbe)
}

func TestParseRetryAfter(t *testing.T) {
	if got := parseRetryAfter("10"); got != 10*time.Second {
		t.Fatalf("expected 10s, got %v", got)
	}

	if got := parseRetryAfter("-1"); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}

	if got := parseRetryAfter("invalid"); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}

	future := time.Now().Add(5 * time.Second).UTC().Format(http.TimeFormat)
	got := parseRetryAfter(future)

	if got <= 0 || got > 5*time.Second {
		t.Fatalf("unexpected HTTP-date Retry-After: %v", got)
	}
}

func TestProviderClientRejectsInvalidUnitID(t *testing.T) {
	client, err := NewProviderClient("http://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}

	tests := []string{
		"",
		"truck/1",
		"truck 1",
		"../admin",
		strings.Repeat("a", 65),
	}

	for _, id := range tests {
		_, err := client.GetUnit(context.Background(), id)
		if !errors.Is(err, ErrInvalidUnitID) {
			t.Errorf("ID %q: expected ErrInvalidUnitID, got %v", id, err)
		}
	}
}
