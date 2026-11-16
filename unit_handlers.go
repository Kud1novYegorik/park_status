package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	unitFreshFor      = 30 * time.Second
	maxProviderParams = 150
	maxProviderString = 64
)

type providerUnitResponse struct {
	Unit   string                     `json:"unit"`
	Fleet  string                     `json:"fleet"`
	TS     *int64                     `json:"ts"`
	Params map[string]json.RawMessage `json:"params"`
}

type unitCounters struct {
	servedLocal        atomic.Int64
	servedFromProvider atomic.Int64
}

var unitStats unitCounters

type UnitStats struct {
	ServedLocal        int64 `json:"served_local"`
	ServedFromProvider int64 `json:"served_from_provider"`
	ProviderReads      int64 `json:"provider_reads"`
	Units              int64 `json:"units"`
}

func UnitStatsSnapshot(provider *ProviderClient) UnitStats {
	var reads int64
	if provider != nil {
		reads = provider.ProviderReads()
	}

	return UnitStats{
		ServedLocal:        unitStats.servedLocal.Load(),
		ServedFromProvider: unitStats.servedFromProvider.Load(),
		ProviderReads:      reads,
		Units:              nUnits.Load(),
	}
}

// NewUnitHandler обслуживает GET и DELETE /units/{id}.
func NewUnitHandler(provider *ProviderClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/units/")

		if !validProviderUnitID(id) || strings.Contains(id, "/") {
			writeUnitError(w, http.StatusBadRequest, "invalid unit ID")
			return
		}

		switch r.Method {
		case http.MethodGet:
			getUnitHandler(w, r, provider, id)

		case http.MethodDelete:
			deleteUnitHandler(w, id)

		default:
			w.Header().Set("Allow", "GET, DELETE")
			writeUnitError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func getUnitHandler(
	w http.ResponseWriter,
	r *http.Request,
	provider *ProviderClient,
	id string,
) {
	localBody, _, conf, state := Get(id, make([]byte, 0, 4096))

	switch state {
	case Dead:
		http.NotFound(w, r)
		return

	case Found:
		if isUnitFresh(conf, time.Now()) {
			serveLocalUnit(w, localBody)
			return
		}

	case Missing:
		// Запрашиваем провайдера.

	default:
		writeUnitError(w, http.StatusInternalServerError, "invalid local state")
		return
	}

	if provider == nil {
		if serveCurrentLocal(w, r, id) {
			return
		}
		writeUnitError(w, http.StatusServiceUnavailable, "provider is not configured")
		return
	}

	// Момент начала чтения. Он защищает от воскресения машины,
	// если DELETE произошёл во время запроса к провайдеру.
	asof := time.Now().Unix()

	body, err := provider.GetUnit(r.Context(), id)
	if err != nil {
		handleProviderError(w, r, id, asof, err)
		return
	}

	fleetID, measurementTS, b, err := parseProviderUnit(body, id)
	if err != nil {
		if serveCurrentLocal(w, r, id) {
			return
		}
		writeUnitError(w, http.StatusBadGateway, "invalid provider response")
		return
	}

	accepted := Put(id, fleetID, measurementTS, asof, b)

	// Обязательно перечитываем состояние после сетевого запроса.
	// Между начальным Get и этим местом могли выполниться DELETE или Sweep.
	currentBody, currentTS, _, currentState := Get(
		id,
		make([]byte, 0, 4096),
	)

	switch currentState {
	case Dead:
		http.NotFound(w, r)
		return

	case Missing:
		http.NotFound(w, r)
		return

	case Found:
		if accepted && currentTS <= measurementTS {
			unitStats.servedFromProvider.Add(1)
		} else {
			unitStats.servedLocal.Add(1)
		}
		writeUnitJSON(w, currentBody)

	default:
		writeUnitError(w, http.StatusInternalServerError, "invalid local state")
	}
}

// При ошибке провайдера отдаём актуальную локальную запись,
// если она всё ещё существует. Старую копию из начала GET не используем.
func handleProviderError(
	w http.ResponseWriter,
	r *http.Request,
	id string,
	asof int64,
	err error,
) {
	body, _, conf, state := Get(id, make([]byte, 0, 4096))

	switch state {
	case Dead:
		http.NotFound(w, r)
		return

	case Found:
		if errors.Is(err, ErrProviderNotFound) && conf <= asof {
			http.NotFound(w, r)
			return
		}

		serveLocalUnit(w, body)
		return

	case Missing:
		if errors.Is(err, ErrProviderNotFound) {
			http.NotFound(w, r)
			return
		}
	}

	var statusErr *ProviderStatusError
	if errors.As(err, &statusErr) &&
		statusErr.StatusCode == http.StatusTooManyRequests &&
		statusErr.RetryAfter > 0 {
		seconds := int64(math.Ceil(statusErr.RetryAfter.Seconds()))
		w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
	}

	writeUnitError(w, http.StatusServiceUnavailable, "provider unavailable")
}

// serveCurrentLocal повторно читает Store, поэтому не может вернуть
// локальную копию, удалённую параллельным DELETE.
func serveCurrentLocal(w http.ResponseWriter, r *http.Request, id string) bool {
	body, _, _, state := Get(id, make([]byte, 0, 4096))

	switch state {
	case Found:
		serveLocalUnit(w, body)
		return true

	case Dead:
		http.NotFound(w, r)
		return true

	default:
		return false
	}
}

func deleteUnitHandler(w http.ResponseWriter, id string) {
	// Tombstone создаётся даже для неизвестной локально машины.
	// Повторный DELETE также возвращает успешный результат.
	Tombstone(id, time.Now().Unix())
	w.WriteHeader(http.StatusNoContent)
}

func isUnitFresh(conf int64, now time.Time) bool {
	if conf <= 0 {
		return false
	}

	age := now.Unix() - conf
	return age <= int64(unitFreshFor.Seconds())
}

func serveLocalUnit(w http.ResponseWriter, body []byte) {
	unitStats.servedLocal.Add(1)
	writeUnitJSON(w, body)
}

func writeUnitJSON(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func writeUnitError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}

// parseProviderUnit проверяет ответ провайдера и преобразует параметры
// в компактное представление, используемое Store.
func parseProviderUnit(
	body []byte,
	expectedID string,
) (uint32, int64, *build, error) {
	var unit providerUnitResponse

	if err := json.Unmarshal(body, &unit); err != nil {
		return 0, 0, nil, fmt.Errorf("decode provider response: %w", err)
	}

	if unit.Unit != expectedID || !validProviderUnitID(unit.Unit) {
		return 0, 0, nil, errors.New("invalid unit ID in provider response")
	}

	if unit.Fleet == "" {
		return 0, 0, nil, errors.New("empty fleet")
	}

	if unit.TS == nil || *unit.TS < 0 {
		return 0, 0, nil, errors.New("invalid measurement timestamp")
	}

	if unit.Params == nil {
		return 0, 0, nil, errors.New("missing params")
	}

	if len(unit.Params) > maxProviderParams {
		return 0, 0, nil, errors.New("too many params")
	}

	fleetID, ok := fleets.id([]byte(unit.Fleet))
	if !ok {
		return 0, 0, nil, errors.New("fleet dictionary is full")
	}

	b := &build{
		ids:  make([]uint16, 0, len(unit.Params)),
		vals: make([]uint64, 0, len(unit.Params)),
		tail: make([]byte, 0, 1024),
	}

	for name, raw := range unit.Params {
		if name == "" {
			return 0, 0, nil, errors.New("empty parameter name")
		}

		code, ok := codes.id([]byte(name))
		if !ok {
			return 0, 0, nil, errors.New("parameter dictionary is full")
		}

		if err := appendProviderValue(b, code, raw); err != nil {
			return 0, 0, nil, fmt.Errorf("parameter %q: %w", name, err)
		}
	}

	b.finish()
	return fleetID, *unit.TS, b, nil
}

func appendProviderValue(
	b *build,
	code uint32,
	raw json.RawMessage,
) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var value any
	if err := dec.Decode(&value); err != nil {
		return err
	}

	switch v := value.(type) {
	case string:
		if len(v) > maxProviderString {
			return errors.New("string value is too long")
		}
		b.Str(code, []byte(v))

	case bool:
		b.Bool(code, v)

	case json.Number:
		if i, err := v.Int64(); err == nil {
			b.Int(code, i)
			return nil
		}

		f, err := v.Float64()
		if err != nil {
			return err
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return errors.New("non-finite number")
		}
		b.Float(code, f)

	default:
		return errors.New("unsupported parameter type")
	}

	return nil
}
