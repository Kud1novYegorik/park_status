package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var testUnitCounter atomic.Uint64

func testUnitID() string {
	return fmt.Sprintf("test-unit-%d", testUnitCounter.Add(1))
}

func TestGetUnitFetchesAndCachesProviderResponse(t *testing.T) {
	id := testUnitID()

	var requests int

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			requests++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fmt.Sprintf(
				`{"unit":%q,"fleet":"fleet-test","ts":100,"params":{"speed":60}}`,
				id,
			)))
		},
	))
	defer server.Close()

	provider, err := NewProviderClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	handler := NewUnitHandler(provider)

	// Первый запрос получает машину у провайдера.
	req := httptest.NewRequest(http.MethodGet, "/units/"+id, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if requests != 1 {
		t.Fatalf("expected one provider request, got %d", requests)
	}

	if !strings.Contains(rec.Body.String(), `"speed":60`) {
		t.Fatalf("unexpected response: %s", rec.Body.String())
	}

	// Второй запрос должен использовать свежую локальную запись.
	req = httptest.NewRequest(http.MethodGet, "/units/"+id, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from local store, got %d", rec.Code)
	}

	if requests != 1 {
		t.Fatalf("expected provider not to be called again, got %d calls", requests)
	}
}

func TestDeletedUnknownUnitDoesNotReachProvider(t *testing.T) {
	id := testUnitID()

	var requests int

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			requests++
			w.WriteHeader(http.StatusOK)
		},
	))
	defer server.Close()

	provider, err := NewProviderClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	handler := NewUnitHandler(provider)

	// Удаляем машину, которой ещё нет в локальном Store.
	req := httptest.NewRequest(http.MethodDelete, "/units/"+id, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}

	// GET не должен обращаться к провайдеру.
	req = httptest.NewRequest(http.MethodGet, "/units/"+id, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}

	if requests != 0 {
		t.Fatalf("provider must not be called, got %d requests", requests)
	}
}

func TestGetUnitUsesStaleLocalWhenProviderUnavailable(t *testing.T) {
	id := testUnitID()

	code, ok := codes.id([]byte("speed"))
	if !ok {
		t.Fatal("cannot create parameter code")
	}

	fleetID, ok := fleets.id([]byte("fleet-test"))
	if !ok {
		t.Fatal("cannot create fleet code")
	}

	b := &build{}
	b.Int(code, 45)

	old := time.Now().Unix() - 120
	if !Put(id, fleetID, old, old, b) {
		t.Fatal("cannot insert local unit")
	}

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		},
	))
	defer server.Close()

	provider, err := NewProviderClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	handler := NewUnitHandler(provider)

	req := httptest.NewRequest(http.MethodGet, "/units/"+id, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected stale local response 200, got %d", rec.Code)
	}

	if !strings.Contains(rec.Body.String(), `"speed":45`) {
		t.Fatalf("expected local speed value, got %s", rec.Body.String())
	}
}

func TestProviderOlderMeasurementDoesNotReplaceNewerLocal(t *testing.T) {
	id := testUnitID()

	code, ok := codes.id([]byte("speed"))
	if !ok {
		t.Fatal("cannot create parameter code")
	}

	fleetID, ok := fleets.id([]byte("fleet-test"))
	if !ok {
		t.Fatal("cannot create fleet code")
	}

	old := time.Now().Unix() - 120

	b := &build{}
	b.Int(code, 90)

	if !Put(id, fleetID, 200, old, b) {
		t.Fatal("cannot insert local unit")
	}

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(fmt.Sprintf(
				`{"unit":%q,"fleet":"fleet-test","ts":100,"params":{"speed":10}}`,
				id,
			)))
		},
	))
	defer server.Close()

	provider, err := NewProviderClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	handler := NewUnitHandler(provider)

	req := httptest.NewRequest(http.MethodGet, "/units/"+id, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if !strings.Contains(rec.Body.String(), `"speed":90`) {
		t.Fatalf("newer local measurement was overwritten: %s", rec.Body.String())
	}
}

func TestDeleteKnownUnitRemovesItFromStore(t *testing.T) {
	id := testUnitID()

	code, ok := codes.id([]byte("speed"))
	if !ok {
		t.Fatal("cannot create parameter code")
	}

	fleetID, ok := fleets.id([]byte("fleet-test"))
	if !ok {
		t.Fatal("cannot create fleet code")
	}

	b := &build{}
	b.Int(code, 50)

	if !Put(id, fleetID, 100, time.Now().Unix(), b) {
		t.Fatal("cannot insert local unit")
	}

	handler := NewUnitHandler(nil)

	req := httptest.NewRequest(http.MethodDelete, "/units/"+id, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}

	_, _, _, state := Get(id, nil)
	if state != Dead {
		t.Fatalf("expected Dead, got %d", state)
	}
}

func TestInvalidUnitID(t *testing.T) {
	handler := NewUnitHandler(nil)

	req := httptest.NewRequest(http.MethodGet, "/units/../admin", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
