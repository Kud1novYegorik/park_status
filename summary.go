package main

import (
	"encoding/json"
	"net/http"
	"sync"
)

const maxSummaryCacheEntries = 4096

type summaryKey struct {
	param string
	fleet string
}

type summaryCacheEntry struct {
	epoch uint64
	value Agg
}

type summaryCache struct {
	mu      sync.RWMutex
	entries map[summaryKey]summaryCacheEntry
}

var summaries = summaryCache{
	entries: make(map[summaryKey]summaryCacheEntry),
}

// Get возвращает запись только если она рассчитана для текущей версии Store.
func (c *summaryCache) Get(key summaryKey, currentEpoch uint64) (Agg, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok || entry.epoch != currentEpoch {
		return Agg{}, false
	}

	return entry.value, true
}

func (c *summaryCache) Put(
	key summaryKey,
	value Agg,
	currentEpoch uint64,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Ограничиваем потребление памяти.
	// При достижении лимита очищаем кэш целиком.
	if len(c.entries) >= maxSummaryCacheEntries {
		clear(c.entries)
	}

	c.entries[key] = summaryCacheEntry{
		epoch: currentEpoch,
		value: value,
	}
}

// cachedSummary использует epoch для проверки актуальности кэша.
//
// Если Store изменился во время расчёта, повторяем расчёт.
// Результат, полученный при изменении Store, в кэш не помещаем.
func cachedSummary(param, fleet string) Agg {
	key := summaryKey{
		param: param,
		fleet: fleet,
	}

	for attempt := 0; attempt < 2; attempt++ {
		currentEpoch := epoch.Load()

		if value, ok := summaries.Get(key, currentEpoch); ok {
			return value
		}

		value := Summary(param, fleet)

		if epoch.Load() == currentEpoch {
			summaries.Put(key, value, currentEpoch)
			return value
		}
	}

	// При постоянных изменениях Store не блокируем запрос бесконечными
	// повторными вычислениями. Последний результат не кэшируем.
	return Summary(param, fleet)
}

type summaryResponse struct {
	Param string   `json:"param"`
	Fleet string   `json:"fleet,omitempty"`
	Units int64    `json:"units"`
	Min   *float64 `json:"min"`
	Max   *float64 `json:"max"`
	Avg   *float64 `json:"avg"`
}

func summaryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()

	param := query.Get("param")
	fleet := query.Get("fleet")

	if param == "" {
		http.Error(w, "query parameter 'param' is required",
			http.StatusBadRequest)
		return
	}

	agg := cachedSummary(param, fleet)

	response := summaryResponse{
		Param: param,
		Fleet: fleet,
		Units: agg.N,
	}

	// При отсутствии числовых значений возвращаем JSON null.
	if agg.N > 0 {
		minValue := agg.Min
		maxValue := agg.Max
		avgValue := agg.Avg()

		response.Min = &minValue
		response.Max = &maxValue
		response.Avg = &avgValue
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		return
	}
}
