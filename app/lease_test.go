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
	mu      sync.Mutex
	current int
	unknown bool  // reads fail
	writes  []int // every value set, in order
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
	f.current = a
	f.writes = append(f.writes, a)
}

func (f *fakeCharger) now() (int, []int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current, append([]int(nil), f.writes...)
}

func newTestLease(t *testing.T, start int, withMarker bool) (*lease, *fakeCharger, string) {
	t.Helper()
	ch := &fakeCharger{current: start}
	marker := ""
	if withMarker {
		marker = filepath.Join(t.TempDir(), "lease.state")
	}
	l := newLease(ch.get, ch.set, marker)
	l.logf = t.Logf
	return l, ch, marker
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestParseLease(t *testing.T) {
	good := []struct {
		in      string
		amps    int
		hold    time.Duration
		fb      int
		release bool
	}{
		{"28 20 7", 28, 20 * time.Second, 7, false},
		{" 16  12.5  6 ", 16, 12500 * time.Millisecond, 6, false},
		{"0", 0, 0, 0, true},
		{"6 3600 6", 6, time.Hour, 6, false},
	}
	for _, g := range good {
		a, h, fb, rel, ok := parseLease(g.in)
		if !ok || a != g.amps || h != g.hold || fb != g.fb || rel != g.release {
			t.Errorf("parseLease(%q) = %d %v %d %v %v", g.in, a, h, fb, rel, ok)
		}
	}
	for _, bad := range []string{"", "28", "28 20", "28 20 7 1", "x 20 7", "28 x 7", "28 20 x", "-1 20 7", "28 0 7", "28 -5 7", "28 20 -1", "28 3601 7", "1"} {
		if _, _, _, _, ok := parseLease(bad); ok {
			t.Errorf("parseLease(%q) accepted", bad)
		}
	}
}

func TestLeaseSetsCurrentAndExpiresToFallback(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	l.handle("28 0.05 7")
	if cur, _ := ch.now(); cur != 28 {
		t.Fatalf("lease did not set the current: %d", cur)
	}
	waitFor(t, "fallback", func() bool { cur, _ := ch.now(); return cur == 7 })
	if l.active {
		t.Fatal("lease still active after expiry")
	}
}

func TestLeaseRenewalKeepsCurrent(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	for i := 0; i < 6; i++ { // renewals every 20 ms, lease 60 ms: never expires
		l.handle("28 0.06 7")
		time.Sleep(20 * time.Millisecond)
	}
	if cur, writes := ch.now(); cur != 28 || len(writes) != 1 {
		t.Fatalf("renewals changed the current: now %d, writes %v", cur, writes)
	}
	waitFor(t, "fallback after the last renewal", func() bool { cur, _ := ch.now(); return cur == 7 })
}

func TestLeaseNeverRaisesOnExpiry(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	l.handle("10 0.03 7")
	ch.set(6) // someone (evcc's own current command) lowered it below the fallback meanwhile
	time.Sleep(80 * time.Millisecond)
	if cur, _ := ch.now(); cur != 6 {
		t.Fatalf("expiry raised the current to %d", cur)
	}
}

func TestLeaseUnknownCurrentIsLowered(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	l.handle("28 0.03 7")
	ch.mu.Lock()
	ch.unknown = true
	ch.mu.Unlock()
	waitFor(t, "fallback with unknown current", func() bool { cur, _ := ch.now(); return cur == 7 })
}

func TestLeaseReleaseStopsCountdown(t *testing.T) {
	l, ch, marker := newTestLease(t, 6, true)
	l.handle("28 0.03 7")
	l.handle("0")
	time.Sleep(80 * time.Millisecond)
	if cur, _ := ch.now(); cur != 28 {
		t.Fatalf("released lease still fell back: %d", cur)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("marker left after release")
	}
}

func TestLeaseMalformedIgnored(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, false)
	l.handle("28 0.03 7")
	l.handle("garbage") // must neither renew nor cancel
	waitFor(t, "fallback", func() bool { cur, _ := ch.now(); return cur == 7 })
}

func TestLeaseMarkerBeforeRaiseAndOnlyOnChange(t *testing.T) {
	l, ch, marker := newTestLease(t, 6, true)
	written := 0
	ch2 := ch
	l.setCurrent = func(a int) {
		// The marker must already exist whenever the lease raises the current.
		if b, err := os.ReadFile(marker); err != nil || strings.TrimSpace(string(b)) != "7" {
			t.Errorf("current set to %d before the marker held the fallback (err %v)", a, err)
		}
		ch2.set(a)
	}
	stat := func() time.Time {
		fi, err := os.Stat(marker)
		if err != nil {
			t.Fatalf("marker missing: %v", err)
		}
		return fi.ModTime()
	}
	l.handle("28 1 7")
	first := stat()
	written++
	time.Sleep(20 * time.Millisecond)
	l.handle("29 1 7") // same fallback: no rewrite (flash wear)
	if !stat().Equal(first) {
		t.Fatal("marker rewritten on a plain renewal")
	}
	l.handle("29 1 6") // new fallback: rewritten
	if b, _ := os.ReadFile(marker); strings.TrimSpace(string(b)) != "6" {
		t.Fatalf("marker not updated to the new fallback: %q", b)
	}
	l.handle("0")
}

func TestLeaseRecoverAfterRestart(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "lease.state")
	if err := os.WriteFile(marker, []byte("7\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ch := &fakeCharger{current: 28} // the charger kept the burst current across the reboot
	l := newLease(ch.get, ch.set, marker)
	l.logf = t.Logf
	l.recover()
	if cur, _ := ch.now(); cur != 7 {
		t.Fatalf("recover left %d A", cur)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("marker not removed after recover")
	}

	// Already below the fallback: recover must not raise.
	if err := os.WriteFile(marker, []byte("7\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ch.set(6)
	l.recover()
	if cur, _ := ch.now(); cur != 6 {
		t.Fatalf("recover raised to %d A", cur)
	}

	// No marker: nothing happens.
	ch.set(28)
	l.recover()
	if cur, _ := ch.now(); cur != 28 {
		t.Fatalf("recover without marker changed the current to %d", cur)
	}

	// Corrupt marker: removed, current untouched.
	if err := os.WriteFile(marker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	l.recover()
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("corrupt marker kept")
	}
}

func TestLeaseExpiryRemovesMarker(t *testing.T) {
	l, ch, marker := newTestLease(t, 6, true)
	l.handle("28 0.03 7")
	waitFor(t, "fallback", func() bool { cur, _ := ch.now(); return cur == 7 })
	waitFor(t, "marker removed", func() bool { _, err := os.Stat(marker); return os.IsNotExist(err) })
}

func TestLeaseConcurrentRenewals(t *testing.T) {
	l, ch, _ := newTestLease(t, 6, true)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); l.handle("20 0.05 7") }()
	}
	wg.Wait()
	waitFor(t, "single clean fallback", func() bool { cur, _ := ch.now(); return cur == 7 })
}
