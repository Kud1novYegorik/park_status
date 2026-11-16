package main

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// Состояние процесса. Хранилище машин появится на шаге 2.
var (
	providerURL    atomic.Pointer[string]
	servedLocal    atomic.Int64
	servedProvider atomic.Int64
	providerReads  atomic.Int64
)

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func notImplemented(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusNotImplemented, map[string]string{"detail": "not implemented"})
}

func health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "healthy"})
}

func setSource(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"detail": "bad json"})
		return
	}
	u, err := url.Parse(req.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		writeJSON(w, 400, map[string]string{"detail": "bad url"})
		return
	}
	s := strings.TrimRight(req.URL, "/")
	providerURL.Store(&s)
	// ponytail: шаг 4 — здесь же сбросить breaker и лимитер.
	writeJSON(w, 200, map[string]string{"source": req.URL})
}

func stats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]int64{
		"served_local":         servedLocal.Load(),
		"served_from_provider": servedProvider.Load(),
		"provider_reads":       providerReads.Load(),
		"units":                0, // шаг 2
	})
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("POST /provider/source", setSource)
	mux.HandleFunc("GET /stats", stats)
	// Заглушки до шагов 3–5.
	mux.HandleFunc("POST /import", importHandler)
	mux.HandleFunc("GET /units/{id}", notImplemented)
	mux.HandleFunc("DELETE /units/{id}", notImplemented)
	mux.HandleFunc("GET /summary", notImplemented)

	srv := &http.Server{
		Addr:              ":8000",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}
