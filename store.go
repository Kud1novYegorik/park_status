package main

import (
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

// Тип значения — два старших бита id параметра, код параметра — младшие 14.
const (
	tInt   = 0
	tFloat = 1
	tBool  = 2
	tStr   = 3

	codeMask = 1<<14 - 1
	maxCodes = 1 << 14
)

// dict: строка → маленький номер. Чтение без блокировок (copy-on-write):
// записей мало (~400 кодов, парки), а читают их миллионы раз за снимок.
type dict struct {
	mu    sync.Mutex
	cur   atomic.Pointer[dsnap]
	max   int
	label func(string) string // во что превратить имя в names; nil — как есть
}

type dsnap struct {
	m     map[string]uint32
	names []string
}

func newDict(max int, label func(string) string) *dict {
	d := &dict{max: max, label: label}
	d.cur.Store(&dsnap{m: map[string]uint32{}})
	return d
}

// find не создаёт записей (m[string(b)] не аллоцирует).
func (d *dict) find(b []byte) (uint32, bool) {
	v, ok := d.cur.Load().m[string(b)]
	return v, ok
}

// id создаёт запись при необходимости; false — словарь переполнен.
func (d *dict) id(b []byte) (uint32, bool) {
	if v, ok := d.find(b); ok {
		return v, true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.cur.Load()
	if v, ok := s.m[string(b)]; ok {
		return v, true
	}
	if len(s.names) >= d.max {
		return 0, false
	}
	name := string(b)
	lbl := name
	if d.label != nil {
		lbl = d.label(name)
	}
	n := &dsnap{
		m:     make(map[string]uint32, len(s.m)+1),
		names: append(s.names[:len(s.names):len(s.names)], lbl), // cap=len → копия
	}
	for k, v := range s.m {
		n.m[k] = v
	}
	n.m[name] = uint32(len(s.names))
	d.cur.Store(n)
	return uint32(len(s.names)), true
}

var (
	// names кодов хранят уже готовый ключ JSON: "имя":
	codes  = newDict(maxCodes, func(s string) string { return string(appendJSONString(nil, s)) + ":" })
	fleets = newDict(math.MaxInt32, nil)
)

// build — набор параметров одной машины; переиспользуется (Reset),
// чтобы разбор снимка не плодил мусор.
type build struct {
	ids  []uint16
	vals []uint64
	tail []byte // тела строковых значений; в vals: смещение<<16 | длина
}

func (b *build) Reset() { b.ids, b.vals, b.tail = b.ids[:0], b.vals[:0], b.tail[:0] }

func (b *build) add(code uint32, t uint16, v uint64) {
	b.ids = append(b.ids, uint16(code)|t<<14)
	b.vals = append(b.vals, v)
}
func (b *build) Int(code uint32, v int64)     { b.add(code, tInt, uint64(v)) }
func (b *build) Float(code uint32, v float64) { b.add(code, tFloat, math.Float64bits(v)) }
func (b *build) Bool(code uint32, v bool) {
	n := uint64(0)
	if v {
		n = 1
	}
	b.add(code, tBool, n)
}
func (b *build) Str(code uint32, s []byte) {
	b.add(code, tStr, uint64(len(b.tail))<<16|uint64(len(s)))
	b.tail = append(b.tail, s...)
}

func (b *build) Len() int           { return len(b.ids) }
func (b *build) Less(i, j int) bool { return b.ids[i]&codeMask < b.ids[j]&codeMask }
func (b *build) Swap(i, j int) {
	b.ids[i], b.ids[j] = b.ids[j], b.ids[i]
	b.vals[i], b.vals[j] = b.vals[j], b.vals[i]
}

// finish сортирует по коду; при дубле побеждает последнее значение.
func (b *build) finish() {
	sorted := true
	for i := 1; i < len(b.ids); i++ {
		if b.ids[i-1]&codeMask >= b.ids[i]&codeMask {
			sorted = false
			break
		}
	}
	if sorted {
		return
	}
	sort.Stable(b)
	n := 0
	for i := range b.ids {
		if i+1 < len(b.ids) && b.ids[i]&codeMask == b.ids[i+1]&codeMask {
			continue
		}
		b.ids[n], b.vals[n] = b.ids[i], b.vals[i]
		n++
	}
	b.ids, b.vals = b.ids[:n], b.vals[:n]
}

// rec — машина. Три слайса без указателей: GC их не сканирует. ~12 байт на параметр.
type rec struct {
	fleet uint32
	ts    int64 // время измерения на борту
	conf  int64 // момент, на который известно, что машина существует (taken_at / начало чтения)
	ids   []uint16
	vals  []uint64
	tail  []byte
}

// set заменяет состояние; при том же числе параметров — на месте, без аллокаций.
func (r *rec) set(fleet uint32, ts int64, b *build) {
	r.fleet, r.ts = fleet, ts
	if len(r.ids) == len(b.ids) {
		copy(r.ids, b.ids)
		copy(r.vals, b.vals)
	} else {
		r.ids = append([]uint16(nil), b.ids...)
		r.vals = append([]uint64(nil), b.vals...)
	}
	if len(b.tail) == 0 {
		r.tail = nil
	} else {
		r.tail = append(r.tail[:0], b.tail...)
	}
}

type shard struct {
	mu   sync.RWMutex
	m    map[string]*rec
	dead map[string]int64 // надгробия: машина → момент снятия
}

var (
	shards [256]shard
	nUnits atomic.Int64
	epoch  atomic.Uint64 // +1 при каждом изменении данных; ключ кэша сводки (шаг 5)
)

func init() {
	for i := range shards {
		shards[i].m = map[string]*rec{}
		shards[i].dead = map[string]int64{}
	}
}

func shardOf(unit string) *shard {
	h := uint32(2166136261) // FNV-1a
	for i := 0; i < len(unit); i++ {
		h = (h ^ uint32(unit[i])) * 16777619
	}
	return &shards[h&255]
}

// Put применяет измерение. asof — момент, на который машина точно существует:
// taken_at снимка или начало поштучного чтения. Правила:
//   - надгробие и asof <= снятия → отбросить (false); asof позже → машина воскресает;
//   - состояние меняется только если ts новее имеющегося; conf только растёт.
func Put(unit string, fleet uint32, ts, asof int64, b *build) bool {
	b.finish()
	s := shardOf(unit)
	s.mu.Lock()
	defer s.mu.Unlock()
	if d, ok := s.dead[unit]; ok {
		if asof <= d {
			return false
		}
		delete(s.dead, unit)
	}
	r := s.m[unit]
	if r == nil {
		r = &rec{conf: asof}
		r.set(fleet, ts, b)
		s.m[unit] = r
		nUnits.Add(1)
		epoch.Add(1)
		return true
	}
	if ts > r.ts {
		r.set(fleet, ts, b)
		epoch.Add(1)
	}
	r.conf = max(r.conf, asof)
	return true
}

const (
	Missing = iota // службе машина не известна → спросить провайдера
	Found
	Dead // снята → 404 без обращения к провайдеру
)

// Get дописывает JSON машины в dst. Рендер под RLock: копировать запись дороже.
func Get(unit string, dst []byte) (out []byte, ts, conf int64, st int) {
	s := shardOf(unit)
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.m[unit]
	if r == nil {
		if _, dead := s.dead[unit]; dead {
			return dst, 0, 0, Dead
		}
		return dst, 0, 0, Missing
	}
	return r.render(dst, unit), r.ts, r.conf, Found
}

// Delete: false, если машина неизвестна (или уже снята).
func Delete(unit string, now int64) bool {
	s := shardOf(unit)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m[unit] == nil {
		return false
	}
	delete(s.m, unit)
	s.dead[unit] = now // ponytail: живёт до снимка с этой машиной; чистить, если снятий станут тысячи
	nUnits.Add(-1)
	epoch.Add(1)
	return true
}

// Sweep вызывается после успешного приёма снимка: кого он не подтвердил
// (conf < taken_at), у провайдера на тот момент не было.
func Sweep(takenAt int64) (dropped int) {
	for i := range shards {
		s := &shards[i]
		s.mu.Lock()
		for k, r := range s.m {
			if r.conf < takenAt {
				delete(s.m, k)
				dropped++
			}
		}
		s.mu.Unlock()
	}
	if dropped > 0 {
		nUnits.Add(-int64(dropped))
		epoch.Add(1)
	}
	return
}

// Agg — итог сводки по одному параметру.
type Agg struct {
	N          int64
	Min, Max   float64
	isum       int64 // целые суммируем точно: 10¹² × 10⁵ уже за пределами float64
	fsum, comp float64
	hasF       bool
}

func (a *Agg) Avg() float64 {
	if !a.hasF {
		return float64(a.isum) / float64(a.N)
	}
	return (float64(a.isum) + a.fsum) / float64(a.N)
}

// Summary: fleet == "" — по всем паркам. Скан всех машин (~5–10 мс на 100 тыс.).
// ponytail: полный скан; инкрементальные агрегаты (параметр, парк), если сводку будут дёргать чаще кэша.
func Summary(param, fleet string) Agg {
	var a Agg
	code, ok := codes.find([]byte(param))
	if !ok {
		return a
	}
	var fid uint32
	if fleet != "" {
		if fid, ok = fleets.find([]byte(fleet)); !ok {
			return a
		}
	}
	for i := range shards {
		s := &shards[i]
		s.mu.RLock()
		for _, r := range s.m {
			if fleet != "" && r.fleet != fid {
				continue
			}
			lo, hi := 0, len(r.ids)
			for lo < hi {
				m := int(uint(lo+hi) >> 1)
				if uint32(r.ids[m]&codeMask) < code {
					lo = m + 1
				} else {
					hi = m
				}
			}
			if lo == len(r.ids) || uint32(r.ids[lo]&codeMask) != code {
				continue
			}
			var f float64
			switch r.ids[lo] >> 14 {
			case tInt:
				v := int64(r.vals[lo])
				a.isum += v
				f = float64(v)
			case tFloat:
				f = math.Float64frombits(r.vals[lo])
				a.hasF = true
				y := f - a.comp // Kahan
				t := a.fsum + y
				a.comp = (t - a.fsum) - y
				a.fsum = t
			default:
				continue // строки и bool в сводке не участвуют
			}
			if a.N == 0 || f < a.Min {
				a.Min = f
			}
			if a.N == 0 || f > a.Max {
				a.Max = f
			}
			a.N++
		}
		s.mu.RUnlock()
	}
	return a
}

// render: unit и fleet проверены при приёме ([A-Za-z0-9-]), экранировать не нужно.
func (r *rec) render(dst []byte, unit string) []byte {
	names := codes.cur.Load().names
	dst = append(dst, `{"unit":"`...)
	dst = append(dst, unit...)
	dst = append(dst, `","fleet":"`...)
	dst = append(dst, fleets.cur.Load().names[r.fleet]...)
	dst = append(dst, `","ts":`...)
	dst = strconv.AppendInt(dst, r.ts, 10)
	dst = append(dst, `,"params":{`...)
	for i, id := range r.ids {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, names[id&codeMask]...)
		v := r.vals[i]
		switch id >> 14 {
		case tInt:
			dst = strconv.AppendInt(dst, int64(v), 10)
		case tFloat:
			dst = AppendFloat(dst, math.Float64frombits(v))
		case tBool:
			if v != 0 {
				dst = append(dst, "true"...)
			} else {
				dst = append(dst, "false"...)
			}
		default:
			off, n := v>>16, v&0xffff
			dst = appendJSONString(dst, r.tail[off:off+n])
		}
	}
	return append(dst, "}}"...)
}

// AppendFloat: 'f' без экспоненты (61.5, 100), экспонента только для крайних величин.
func AppendFloat(dst []byte, f float64) []byte {
	if a := math.Abs(f); a != 0 && (a < 1e-6 || a >= 1e21) {
		return strconv.AppendFloat(dst, f, 'g', -1, 64)
	}
	return strconv.AppendFloat(dst, f, 'f', -1, 64)
}

func appendJSONString[T string | []byte](dst []byte, s T) []byte {
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"' || c == '\\':
			dst = append(dst, '\\', c)
		case c < 0x20:
			dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&15])
		default:
			dst = append(dst, c)
		}
	}
	return append(dst, '"')
}
