package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSummaryHandler(t *testing.T) {
	param := fmt.Sprintf("test_param_%d", time.Now().UnixNano())
	fleet := fmt.Sprintf("test_fleet_%d", time.Now().UnixNano())

	code, ok := codes.id([]byte(param))
	if !ok {
		t.Fatal("cannot create parameter code")
	}

	fleetID, ok := fleets.id([]byte(fleet))
	if !ok {
		t.Fatal("cannot create fleet code")
	}

	values := []int64{10, 20, 30}

	for i, value := range values {
		b := &build{}
		b.Int(code, value)

		id := fmt.Sprintf("summary-unit-%d-%d", time.Now().UnixNano(), i)
		if !Put(id, fleetID, int64(i+1), time.Now().Unix(), b) {
			t.Fatal("cannot insert unit")
		}
	}

	req := httptest.NewRequest(
		http.MethodGet,
		"/summary?param="+param+"&fleet="+fleet,
		nil,
	)
	rec := httptest.NewRecorder()

	summaryHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var response summaryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}

	if response.Units != 3 {
		t.Fatalf("expected 3 units, got %d", response.Units)
	}

	if response.Min == nil || *response.Min != 10 {
		t.Fatalf("expected min=10, got %v", response.Min)
	}

	if response.Max == nil || *response.Max != 30 {
		t.Fatalf("expected max=30, got %v", response.Max)
	}

	if response.Avg == nil || *response.Avg != 20 {
		t.Fatalf("expected avg=20, got %v", response.Avg)
	}
}

func TestSummaryCacheInvalidation(t *testing.T) {
	param := fmt.Sprintf("cache_param_%d", time.Now().UnixNano())

	code, ok := codes.id([]byte(param))
	if !ok {
		t.Fatal("cannot create parameter code")
	}

	fleetID, ok := fleets.id([]byte("cache-test-fleet"))
	if !ok {
		t.Fatal("cannot create fleet code")
	}

	id1 := fmt.Sprintf("cache-unit-1-%d", time.Now().UnixNano())

	b1 := &build{}
	b1.Int(code, 10)

	if !Put(id1, fleetID, 1, time.Now().Unix(), b1) {
		t.Fatal("cannot insert first unit")
	}

	first := cachedSummary(param, "")
	if first.N != 1 || first.Min != 10 {
		t.Fatalf("unexpected first summary: %+v", first)
	}

	id2 := fmt.Sprintf("cache-unit-2-%d", time.Now().UnixNano())

	b2 := &build{}
	b2.Int(code, 30)

	if !Put(id2, fleetID, 1, time.Now().Unix(), b2) {
		t.Fatal("cannot insert second unit")
	}

	second := cachedSummary(param, "")

	if second.N != 2 {
		t.Fatalf("expected cache to be invalidated, got %d units", second.N)
	}

	if second.Min != 10 || second.Max != 30 {
		t.Fatalf("unexpected updated summary: %+v", second)
	}
}

func TestSummaryEmptyResult(t *testing.T) {
	param := fmt.Sprintf("unknown_param_%d", time.Now().UnixNano())

	req := httptest.NewRequest(
		http.MethodGet,
		"/summary?param="+param,
		nil,
	)
	rec := httptest.NewRecorder()

	summaryHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var response summaryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}

	if response.Units != 0 {
		t.Fatalf("expected 0 units, got %d", response.Units)
	}

	if response.Min != nil || response.Max != nil || response.Avg != nil {
		t.Fatal("expected min, max and avg to be null")
	}
}

func TestSummaryRequiresParam(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/summary", nil)
	rec := httptest.NewRecorder()

	summaryHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
