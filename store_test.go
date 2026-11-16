package main

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
)

func cid(n string) uint32 { id, _ := codes.id([]byte(n)); return id }
func fid(n string) uint32 { id, _ := fleets.id([]byte(n)); return id }

func TestSemantics(t *testing.T) {
	var b build
	a, fx := cid("t_a"), fid("fx")
	putF := func(u string, ts, asof int64, v float64) bool {
		b.Reset()
		b.Float(a, v)
		return Put(u, fx, ts, asof, &b)
	}
	get := func(u string) (string, int64, int64, int) {
		o, ts, conf, st := Get(u, nil)
		return string(o), ts, conf, st
	}

	// старее по ts состояние не меняет, но подтверждает существование
	putF("t-1", 100, 100, 1)
	putF("t-1", 50, 120, 2)
	out, ts, conf, st := get("t-1")
	if st != Found || ts != 100 || conf != 120 || !strings.Contains(out, `"t_a":1}`) {
		t.Fatal(out, ts, conf, st)
	}
	if want := `{"unit":"t-1","fleet":"fx","ts":100,"params":{"t_a":1}}`; out != want {
		t.Fatal(out)
	}

	// Sweep: t-2 не подтверждён снимком 100, t-1 подтверждён позже
	putF("t-2", 10, 90, 5)
	if Sweep(100) < 1 {
		t.Fatal("sweep")
	}
	if _, _, _, st := get("t-2"); st != Missing {
		t.Fatal("t-2 должна быть забыта")
	}
	if _, _, _, st := get("t-1"); st != Found {
		t.Fatal("t-1 должна остаться")
	}

	// надгробие: старый снимок не воскрешает, новый — воскрешает
	if !Delete("t-1", 500) || Delete("t-1", 501) {
		t.Fatal("delete")
	}
	if _, _, _, st := get("t-1"); st != Dead {
		t.Fatal("dead")
	}
	if putF("t-1", 600, 400, 1) {
		t.Fatal("снимок до снятия должен игнорироваться")
	}
	if !putF("t-1", 600, 501, 1) {
		t.Fatal("снимок после снятия должен воскрешать")
	}
	if _, _, _, st := get("t-1"); st != Found {
		t.Fatal("t-1 воскресла")
	}

	// сводка: int + float считаются, строка и bool — нет; парк фильтрует
	b.Reset()
	b.Int(a, 4)
	Put("t-3", fx, 1, 600, &b)
	b.Reset()
	b.Str(a, []byte(`x"y`))
	Put("t-4", fx, 1, 600, &b)
	s := Summary("t_a", "fx")
	if s.N != 2 || s.Min != 1 || s.Max != 4 || s.Avg() != 2.5 {
		t.Fatalf("%+v", s)
	}
	if Summary("t_a", "nope").N != 0 || Summary("nope_param", "").N != 0 {
		t.Fatal("пустые сводки")
	}
	if o, _, _, _ := Get("t-4", nil); !strings.Contains(string(o), `"t_a":"x\"y"`) {
		t.Fatal(string(o))
	}
}

// Бюджет памяти: 100 тыс. машин × 110 параметров (60 int, 40 float, 5 bool, 5 строк по 20 знаков).
func TestMemory(t *testing.T) {
	const N, P = 100_000, 110
	ids := make([]uint32, P)
	for k := range ids {
		ids[k] = cid(fmt.Sprintf("m_p%03d", k))
	}
	fl := fid("mem-fleet")
	fill := func(b *build, i int) {
		b.Reset()
		for k := 0; k < P; k++ {
			switch {
			case k < 60:
				b.Int(ids[k], int64(i*7+k))
			case k < 100:
				b.Float(ids[k], float64(i)*0.37+float64(k)+0.5)
			case k < 105:
				b.Bool(ids[k], (i+k)%2 == 0)
			default:
				b.Str(ids[k], []byte(fmt.Sprintf("RU%018d", i)))
			}
		}
	}
	var b build
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc
	for i := 0; i < N; i++ {
		fill(&b, i)
		Put(fmt.Sprintf("m-%07d", i), fl, int64(1000+i), 1, &b)
	}
	runtime.GC()
	runtime.ReadMemStats(&ms)
	used := ms.HeapAlloc - base
	t.Logf("куча: %d МБ (%.0f Б/машину), Sys %d МБ", used>>20, float64(used)/N, ms.Sys>>20)
	if used > 160<<20 {
		t.Fatal("не влезаем в бюджет памяти")
	}

	// повторный снимок с тем же набором параметров — без аллокаций
	fill(&b, 7)
	ts := int64(1 << 40)
	if al := testing.AllocsPerRun(1000, func() { ts++; Put("m-0000007", fl, ts, 2, &b) }); al != 0 {
		t.Fatal("обновление на месте аллоцирует:", al)
	}
}
