package bridge

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeCharger stands in for the charger's max_charging_current setting.
type fakeCharger struct {
	mu        sync.Mutex
	current   int
	unknown   bool // reads fail
	failWrite bool // writes are silently lost (the real setter only logs SQL errors)
	writes    []int
}

func (f *fakeCharger) get() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unknown {
		return -1
	}
	return f.current
}

func (f *fakeCharger) set(a int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, a)
	if !f.failWrite {
		f.current = a
	}
}

func (f *fakeCharger) cur() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current
}

func (f *fakeCharger) with(fn func(*fakeCharger)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeCharger) nWrites() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.writes)
}

func newTestLease(t *testing.T, start int, withMarker bool) (*lease, *fakeCharger, string) {
	t.Helper()
	ch := &fakeCharger{current: start}
	marker := ""
	if withMarker {
		marker = filepath.Join(t.TempDir(), "lease.state")
	}
	l := newLease(ch.get, ch.set, 32, marker)
	l.logf = t.Logf
	l.retry = 10 * time.Millisecond
	return l, ch, marker
}

func (l *lease) isActive() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func markerExists(marker string) bool {
	_, err := os.Stat(marker)
	return err == nil
}

func TestParseLease(t *testing.T) {
	good := []struct {
		in   string
		want leaseMsg
	}{
		{"28 20 7", leaseMsg{amps: 28, hold: 20 * time.Second, fallback: 7}},
		{" 16  12.5  6 ", leaseMsg{amps: 16, hold: 12500 * time.Millisecond, fallback: 6}},
		{"0", leaseMsg{release: true}},
		{"6 3600 6", leaseMsg{amps: 6, hold: time.Hour, fallback: 6}},
		{"32 1 0", leaseMsg{amps: 32, hold: time.Second, fallback: 0}},
		{"28 20 7 1795300000123", leaseMsg{amps: 28, hold: 20 * time.Second, fallback: 7, seq: 1795300000123}},
		{"0 1795300000124", leaseMsg{release: true, seq: 1795300000124}},
	}
	for _, g := range good {
		m, ok := parseLease(g.in, 32)
		if !ok || m != g.want {
			t.Errorf("parseLease(%q) = %+v %v, want %+v", g.in, m, ok, g.want)
		}
	}
	bad := []string{"", "28", "28 20", "28 20 7 1 2", "x 20 7", "28 x 7", "28 20 x", "-1 20 7",
		"28 0 7", "28 -5 7", "28 20 -1", "28 3601 7", "1", "28 NaN 7", "28 nan 7", "28 Inf 7",
		"28 1e-9 7", "28 0.5 7", "33 20 7", "99 20 7", "2147483648 20 7", "28 20 33",
		"28 20 7 x", "28 20 7 0", "28 20 7 -5", "0 x", "0 0", "0 -1", "0 1 2"}
	for _, b := range bad {
		if _, ok := parseLease(b, 32); ok {
			t.Errorf("parseLease(%q) accepted", b)
		}
	}
}

func TestLeaseOutOfOrderDropped(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	l.handle("16 5 7 1001")
	l.handle("28 5 7 1000") // older, higher current arriving late: must not win
	if ch.cur() != 16 {
		t.Fatalf("an out-of-order lease set %d A", ch.cur())
	}
	l.handle("16 5 7 1001") // a duplicate is not newer either
	l.handle("20 5 7 1002")
	if ch.cur() != 20 {
		t.Fatalf("the next lease in order was not applied: %d", ch.cur())
	}
	l.handle("18 5 7 5") // far behind: the controller restarted its count, taken
	if ch.cur() != 18 {
		t.Fatalf("a restarted controller's lease was dropped: %d", ch.cur())
	}
	l.handle("19 5 7 6")
	if ch.cur() != 19 {
		t.Fatalf("counting on after a restart: %d", ch.cur())
	}
	l.handle("20 5 7") // a lease without <seq> is always taken
	l.handle("0")
}

func TestLeaseLateLeaseCannotOvertakeRelease(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	l.handle("16 5 7 1001")
	l.handle("0 1003")      // the controller lowered to the fallback and released
	l.handle("16 5 7 1002") // the older lease arrives last: must not raise again
	if ch.cur() != 7 || l.isActive() {
		t.Fatalf("a late lease overtook the release: %d A, active %v", ch.cur(), l.isActive())
	}
	l.handle("0 1002") // an old release is out of order too
	l.handle("18 5 7 1004")
	if ch.cur() != 18 {
		t.Fatalf("the next lease after the release: %d", ch.cur())
	}
	l.handle("0 1005")
}

func TestLeaseSetsCurrentAndExpiresToFallback(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	l.handle("28 1 7")
	if ch.cur() != 28 {
		t.Fatalf("lease did not set the current: %d", ch.cur())
	}
	waitFor(t, "fallback", func() bool { return ch.cur() == 7 })
	waitFor(t, "lease ended", func() bool { return !l.isActive() })
}

func TestLeaseRenewalKeepsCurrent(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	for i := 0; i < 20; i++ { // renewals every 100 ms against a 1 s lease
		l.handle("28 1 7")
		time.Sleep(100 * time.Millisecond)
	}
	if ch.cur() != 28 || ch.nWrites() != 1 {
		t.Fatalf("renewals changed the current: now %d, %d writes", ch.cur(), ch.nWrites())
	}
	waitFor(t, "fallback after the last renewal", func() bool { return ch.cur() == 7 })
}

func TestLeaseNeverRaisesOnExpiry(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	l.handle("10 1 7")
	ch.set(6) // evcc's own current command lowered it below the fallback meanwhile
	waitFor(t, "expiry", func() bool { return !l.isActive() })
	if ch.cur() != 6 {
		t.Fatalf("expiry raised the current to %d", ch.cur())
	}
}

func TestLeaseUnknownCurrentIsLowered(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	l.handle("28 1 7")
	ch.with(func(f *fakeCharger) { f.unknown = true })
	waitFor(t, "a fallback write", func() bool { return ch.cur() == 7 })
	if !l.isActive() {
		t.Fatal("lease ended without a confirmed fallback")
	}
	ch.with(func(f *fakeCharger) { f.unknown = false })
	waitFor(t, "lease ended once the fallback reads back", func() bool { return !l.isActive() })
}

func TestLeaseFailedFallbackWriteIsRetried(t *testing.T) {
	l, ch, marker := newTestLease(t, 6, true)
	l.handle("28 1 7")
	ch.with(func(f *fakeCharger) { f.failWrite = true }) // MySQL busy at expiry
	waitFor(t, "several fallback attempts", func() bool { return ch.nWrites() >= 4 })
	if ch.cur() != 28 || !l.isActive() || !markerExists(marker) {
		t.Fatalf("a failed fallback must keep the lease and its marker: cur %d active %v", ch.cur(), l.isActive())
	}
	ch.with(func(f *fakeCharger) { f.failWrite = false })
	waitFor(t, "fallback once the database answers", func() bool { return ch.cur() == 7 && !l.isActive() })
	waitFor(t, "marker removed", func() bool { return !markerExists(marker) })
}

func TestLeaseReleaseLowersAndStops(t *testing.T) {
	l, ch, marker := newTestLease(t, 6, true)
	l.handle("28 1 7")
	l.handle("0")
	if ch.cur() != 7 {
		t.Fatalf("release left %d A above the fallback", ch.cur())
	}
	if markerExists(marker) || l.isActive() {
		t.Fatal("release left the marker or the lease")
	}
	ch.set(16) // the controller sets its own current after the release: no countdown touches it
	time.Sleep(1500 * time.Millisecond)
	if ch.cur() != 16 {
		t.Fatalf("a released lease still acted: %d", ch.cur())
	}
}

func TestLeaseReleaseNeverRaises(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	l.handle("28 5 7")
	ch.set(6)
	l.handle("0")
	if ch.cur() != 6 {
		t.Fatalf("release raised to %d", ch.cur())
	}
}

func TestLeaseAtOrBelowFallbackNeedsNoCountdown(t *testing.T) {
	l, ch, marker := newTestLease(t, 16, true)
	l.handle("6 1 7")
	if ch.cur() != 6 || l.isActive() || markerExists(marker) {
		t.Fatalf("a lease at or below its fallback: cur %d active %v marker %v", ch.cur(), l.isActive(), markerExists(marker))
	}
}

func TestLeaseMalformedIgnored(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	l.handle("28 1 7")
	for i := 0; i < 100; i++ {
		l.handle("garbage") // must neither renew nor cancel, and logs quietly
	}
	waitFor(t, "fallback", func() bool { return ch.cur() == 7 })
}

func TestLeaseMarkerBeforeRaiseAndOnlyOnChange(t *testing.T) {
	l, ch, marker := newTestLease(t, 6, true)
	l.setCurrent = func(a int) {
		// The marker must already hold the fallback whenever the lease raises the current.
		if a > 7 {
			if b, err := os.ReadFile(marker); err != nil || strings.TrimSpace(string(b)) == "" {
				t.Errorf("current raised to %d before the marker existed (err %v)", a, err)
			}
		}
		ch.set(a)
	}
	l.handle("28 5 7")
	os.WriteFile(marker, []byte("sentinel\n"), 0o644) // a rewrite would replace this
	l.handle("29 5 7")                                // same fallback: no rewrite (flash wear)
	if b, _ := os.ReadFile(marker); strings.TrimSpace(string(b)) != "sentinel" {
		t.Fatal("marker rewritten on a plain renewal")
	}
	l.handle("29 5 6") // new fallback: rewritten
	if b, _ := os.ReadFile(marker); strings.TrimSpace(string(b)) != "6" {
		t.Fatalf("marker not updated to the new fallback: %q", b)
	}
	l.handle("0")
}

func TestLeaseNoRaiseWithoutMarker(t *testing.T) {
	ch := &fakeCharger{current: 6}
	l := newLease(ch.get, ch.set, 32, filepath.Join(t.TempDir(), "missing-dir", "lease.state"))
	l.logf = t.Logf
	l.retry = 10 * time.Millisecond
	l.handle("28 5 7")
	if ch.cur() > 7 {
		t.Fatalf("raised to %d without a stored fallback", ch.cur())
	}
	l.handle("0")
}

func TestLeaseRenewalRacingExpiry(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	for i := 0; i < 50; i++ { // renewals land right around their own expiry
		l.handle("28 1 7")
		time.Sleep(time.Duration(990+i%20) * time.Millisecond / 50)
	}
	l.handle("28 1 7")
	if ch.cur() != 28 {
		t.Fatalf("a renewal was overtaken by an old countdown: %d", ch.cur())
	}
	waitFor(t, "final fallback", func() bool { return ch.cur() == 7 })
}

func TestLeaseConcurrentRenewals(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, true)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); l.handle("20 1 7") }()
	}
	wg.Wait()
	waitFor(t, "single clean fallback", func() bool { return ch.cur() == 7 && !l.isActive() })
}

func TestLeaseShutdownAppliesFallback(t *testing.T) {
	l, ch, marker := newTestLease(t, 6, true)
	l.handle("28 60 7")
	l.shutdown()
	if ch.cur() != 7 || markerExists(marker) {
		t.Fatalf("shutdown: cur %d, marker %v", ch.cur(), markerExists(marker))
	}

	// A failed write at shutdown keeps the marker for the next start.
	l, ch, marker = newTestLease(t, 6, true)
	l.handle("28 60 7")
	ch.with(func(f *fakeCharger) { f.failWrite = true })
	l.shutdown()
	if !markerExists(marker) {
		t.Fatal("marker removed although the fallback did not take")
	}

	// No lease: shutdown changes nothing.
	l, ch, _ = newTestLease(t, 16, true)
	l.shutdown()
	if ch.cur() != 16 || ch.nWrites() != 0 {
		t.Fatalf("shutdown without a lease wrote: %d", ch.cur())
	}
}

func TestLeaseApplyStale(t *testing.T) {
	cases := []struct {
		name    string
		marker  string // "" = no marker file
		start   int
		want    int
		touched bool
	}{
		{"burst current kept across a reboot", "7\n", 28, 7, true},
		{"already below: never raised", "7\n", 6, 6, false},
		{"no marker: nothing happens", "", 28, 28, false},
		{"corrupt marker: charger minimum", "x", 28, leaseMinAmps, true},
		{"empty marker after a power cut: charger minimum", "", 28, leaseMinAmps, true},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			marker := filepath.Join(dir, "lease.state")
			if c.marker != "" || i == 4 {
				if err := os.WriteFile(marker, []byte(c.marker), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			ch := &fakeCharger{current: c.start}
			l := newLease(ch.get, ch.set, 32, marker)
			l.logf = t.Logf
			l.applyStale()
			if ch.cur() != c.want {
				t.Fatalf("current %d, want %d", ch.cur(), c.want)
			}
			if (ch.nWrites() > 0) != c.touched {
				t.Fatalf("writes %d, touched want %v", ch.nWrites(), c.touched)
			}
			if markerExists(marker) {
				t.Fatal("marker left after start")
			}
		})
	}
}

func TestLeaseApplyStaleRetriesUntilItTakes(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "lease.state")
	os.WriteFile(marker, []byte("7\n"), 0o644)
	ch := &fakeCharger{current: 28, failWrite: true}
	l := newLease(ch.get, ch.set, 32, marker)
	l.logf = t.Logf
	l.retry = 10 * time.Millisecond
	go func() {
		time.Sleep(60 * time.Millisecond)
		ch.with(func(f *fakeCharger) { f.failWrite = false }) // MySQL comes up after boot
	}()
	l.applyStale()
	if ch.cur() != 7 || markerExists(marker) {
		t.Fatalf("applyStale gave up: cur %d marker %v", ch.cur(), markerExists(marker))
	}
}
