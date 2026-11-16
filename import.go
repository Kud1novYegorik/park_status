package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
)

const (
	maxImportSize = 250 << 20 // 250 MiB
	maxLineSize   = 1 << 20   // 1 MiB на одну машину
)

var importMu sync.Mutex

type importResult struct {
	Imported int `json:"imported"`
	Skipped  int `json:"skipped"`
	Dropped  int `json:"dropped"`
}

// importHandler принимает полный снимок провайдера.
func importHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed,
			map[string]string{"detail": "method not allowed"})
		return
	}

	// Одновременно обрабатываем только один снимок.
	importMu.Lock()
	defer importMu.Unlock()

	body := http.MaxBytesReader(w, r.Body, maxImportSize)
	defer body.Close()

	reader := bufio.NewReaderSize(body, 64*1024)

	takenAt, err := readImportHeader(reader)
	if err != nil {
		writeJSON(w, http.StatusBadRequest,
			map[string]string{"detail": "bad import header"})
		return
	}

	var result importResult

	for {
		line, err := readImportLine(reader)
		if err != nil {
			writeJSON(w, http.StatusBadRequest,
				map[string]string{"detail": "incomplete import"})
			return
		}

		line = bytes.TrimSpace(line)

		// Последняя строка снимка.
		if bytes.Equal(line, []byte("]}")) {
			// Sweep разрешён только после успешного окончания снимка.
			result.Dropped = Sweep(takenAt)

			writeJSON(w, http.StatusOK, result)
			return
		}

		// Убираем запятую после объекта машины.
		line = bytes.TrimSuffix(line, []byte(","))

		if len(line) == 0 {
			result.Skipped++
			continue
		}

		var unit importUnit
		if err := json.Unmarshal(line, &unit); err != nil {
			result.Skipped++
			continue
		}

		b := acquireBuild()
		ok := parseImportUnit(line, &unit, b)

		if !ok {
			releaseBuild(b)
			result.Skipped++
			continue
		}

		fleetID, ok := fleets.id([]byte(unit.Fleet))
		if !ok {
			releaseBuild(b)
			result.Skipped++
			continue
		}

		accepted := Put(unit.Unit, fleetID, unit.TS, takenAt, b)

		releaseBuild(b)

		if accepted {
			result.Imported++
		}
	}
}

type importUnit struct {
	Unit   string          `json:"unit"`
	Fleet  string          `json:"fleet"`
	TS     int64           `json:"ts"`
	Params json.RawMessage `json:"params"`
}

// readImportHeader читает первую строку и извлекает taken_at.

func readImportHeader(r *bufio.Reader) (int64, error) {
	line, err := readImportLine(r)
	if err != nil {
		return 0, err
	}

	dec := json.NewDecoder(bytes.NewReader(line))

	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return 0, errors.New("expected object")
	}

	token, err = dec.Token()
	if err != nil || token != "taken_at" {
		return 0, errors.New("expected taken_at")
	}

	var takenAt int64
	if err := dec.Decode(&takenAt); err != nil || takenAt < 0 {
		return 0, errors.New("invalid taken_at")
	}

	token, err = dec.Token()
	if err != nil || token != "units" {
		return 0, errors.New("expected units")
	}

	token, err = dec.Token()
	if err != nil || token != json.Delim('[') {
		return 0, errors.New("expected array")
	}

	return takenAt, nil
}

// readImportLine читает одну строку с ограничением размера.
func readImportLine(r *bufio.Reader) ([]byte, error) {
	var line []byte

	for {
		part, err := r.ReadSlice('\n')

		if len(line)+len(part) > maxLineSize {
			return nil, errors.New("line too large")
		}

		line = append(line, part...)

		switch err {
		case nil:
			return bytes.TrimSuffix(line, []byte("\n")), nil

		case bufio.ErrBufferFull:
			continue

		case io.EOF:
			if len(line) > 0 {
				return bytes.TrimSuffix(line, []byte("\n")), nil
			}
			return nil, io.ErrUnexpectedEOF

		default:
			return nil, err
		}
	}
}

func parseImportUnit(
	line []byte,
	unit *importUnit,
	b *build,
) bool {
	if unit.Unit == "" || unit.Fleet == "" ||
		unit.TS < 0 || len(unit.Params) == 0 {
		return false
	}

	if !validID(unit.Unit) || !validID(unit.Fleet) {
		return false
	}

	// params обязан быть JSON-объектом.
	params := bytes.TrimSpace(unit.Params)
	if len(params) < 2 || params[0] != '{' ||
		params[len(params)-1] != '}' {
		return false
	}

	var values map[string]json.RawMessage
	if err := json.Unmarshal(params, &values); err != nil {
		return false
	}

	if values == nil {
		return false
	}

	b.Reset()

	for name, raw := range values {
		if name == "" {
			return false
		}

		code, ok := codes.id([]byte(name))
		if !ok {
			return false
		}

		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 {
			return false
		}

		switch raw[0] {
		case '"':
			var value string
			if err := json.Unmarshal(raw, &value); err != nil {
				return false
			}
			if len(value) > 64 {
				return false
			}
			b.Str(code, []byte(value))

		case 't', 'f':
			var value bool
			if err := json.Unmarshal(raw, &value); err != nil {
				return false
			}
			b.Bool(code, value)

		default:
			var number json.Number

			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.UseNumber()

			var value any
			if err := dec.Decode(&value); err != nil {
				return false
			}

			number, ok = value.(json.Number)
			if !ok {
				return false
			}

			if i, err := number.Int64(); err == nil {
				b.Int(code, i)
			} else {
				f, err := number.Float64()
				if err != nil {
					return false
				}
				b.Float(code, f)
			}
		}
	}

	b.finish()
	return true
}

func validID(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}

	for i := 0; i < len(s); i++ {
		c := s[i]

		if (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '-' {
			continue
		}

		return false
	}

	return true
}

var buildPool = sync.Pool{
	New: func() any {
		return &build{
			ids:  make([]uint16, 0, 150),
			vals: make([]uint64, 0, 150),
			tail: make([]byte, 0, 1024),
		}
	},
}

func acquireBuild() *build {
	return buildPool.Get().(*build)
}

func releaseBuild(b *build) {
	b.Reset()
	buildPool.Put(b)
}
