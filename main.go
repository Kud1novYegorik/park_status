package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	serverAddr = ":8000"
)

func main() {
	// Адрес телематического провайдера задаётся через переменную окружения.
	providerURL := os.Getenv("PROVIDER_URL")

	var providerClient *ProviderClient

	if providerURL != "" {
		client, err := NewProviderClient(providerURL)
		if err != nil {
			log.Fatalf("create provider client: %v", err)
		}
		providerClient = client
		log.Printf("provider configured: %s", providerURL)
	} else {
		log.Println("provider is not configured; local-only mode")
	}

	mux := http.NewServeMux()

	// Импорт полного snapshot от телематического провайдера.
	mux.HandleFunc("/import", importHandler)

	// Получение и удаление информации о машине.
	mux.Handle("/units/", NewUnitHandler(providerClient))
	mux.HandleFunc("/summary", summaryHandler)

	// Статистика сервиса.
	mux.HandleFunc("/stats", statsHandler(providerClient))

	server := &http.Server{
		Addr:              serverAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	// Контекст для корректного завершения сервера.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("server shutdown: %v", err)
		}
	}()

	log.Printf("server listening on %s", serverAddr)

	if err := server.ListenAndServe(); err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server failed: %v", err)
	}

	log.Println("server stopped")
}

func statsHandler(provider *ProviderClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		stats := UnitStatsSnapshot(provider)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			log.Printf("encode stats: %v", err)
		}
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
