package bridge

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// A lease lets the controller that sets the current (for example evcc's load
// management) say how long its current may stand without hearing from it again:
//
//	<topic prefix>/lease/set   "<amps> <seconds> <fallback amps>"   or "0" to release
//
// Every lease sets the current to <amps> and restarts a countdown of <seconds>.
// If no new lease arrives in time, the bridge lowers the current to <fallback>
// on its own. The countdown runs here, on the charger, so it still fires when
// the controller, the broker or the network is gone: a lost link can never hold
// a high current. Without leases the bridge behaves exactly as before.
//
// While a lease is active a small marker file holds the fallback. The charger
// keeps max_charging_current across reboots, so if the bridge or the charger
// restarts mid-lease, the fallback is applied at start before anything else.
type lease struct {
	getCurrent func() int // fresh read of the setting; -1 when unknown

	setCurrent func(int)
	marker     string // path of the marker file; "" disables it (tests)
	logf       func(format string, args ...any)

	mu       sync.Mutex
	timer    *time.Timer
	fallback int  // fallback of the active lease
	active   bool // a lease is running
	gen      uint64
}

func newLease(get func() int, set func(int), marker string) *lease {
	return &lease{getCurrent: get, setCurrent: set, marker: marker, logf: func(f string, a ...any) { fmt.Printf(f+"\n", a...) }}
}

// parseLease reads "<amps> <seconds> <fallback>" or "0". ok is false for anything else.
func parseLease(payload string) (amps int, hold time.Duration, fallback int, release bool, ok bool) {
	f := strings.Fields(payload)
	if len(f) == 1 && f[0] == "0" {
		return 0, 0, 0, true, true
	}
	if len(f) != 3 {
		return 0, 0, 0, false, false
	}
	a, err1 := strconv.Atoi(f[0])
	s, err2 := strconv.ParseFloat(f[1], 64)
	fb, err3 := strconv.Atoi(f[2])
	if err1 != nil || err2 != nil || err3 != nil || a < 0 || fb < 0 || s <= 0 || s > 3600 {
		return 0, 0, 0, false, false
	}
	return a, time.Duration(s * float64(time.Second)), fb, false, true
}

// handle applies one lease message.
func (l *lease) handle(payload string) {
	amps, hold, fallback, release, ok := parseLease(payload)
	if !ok {
		l.logf("Lease: ignoring malformed payload %q (want \"<amps> <seconds> <fallback>\" or \"0\")", payload)
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if release {
		if l.active {
			l.logf("Lease: released")
		}
		l.stopLocked()
		return
	}

	// The marker goes down before the current goes up: a restart in between
	// must still find the fallback.
	if !l.active || l.fallback != fallback {
		l.writeMarker(fallback)
	}
	if !l.active {
		l.logf("Lease: active, %d A, fallback %d A after %s without renewal", amps, fallback, hold)
	}
	l.active = true
	l.fallback = fallback
	if l.getCurrent() != amps {
		l.setCurrent(amps)
	}

	l.gen++
	gen := l.gen
	if l.timer != nil {
		l.timer.Stop()
	}
	l.timer = time.AfterFunc(hold, func() { l.expire(gen) })
}

// expire runs when a lease was not renewed in time.
func (l *lease) expire(gen uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active || gen != l.gen { // renewed or released meanwhile
		return
	}
	// An unknown current (-1) is lowered too: lowering is the safe direction.
	cur := l.getCurrent()
	if cur > l.fallback || cur < 0 {
		l.setCurrent(l.fallback)
		l.logf("Lease: expired, current lowered from %d A to %d A", cur, l.fallback)
	} else {
		l.logf("Lease: expired, current already %d A (fallback %d A)", cur, l.fallback)
	}
	l.active = false
	l.timer = nil
	l.removeMarker()
}

// recover applies the fallback of a lease that was active when the bridge or
// the charger last stopped. Call once at start, before taking commands.
func (l *lease) recover() {
	if l.marker == "" {
		return
	}
	b, err := os.ReadFile(l.marker)
	if err != nil {
		return // no lease was active
	}
	fallback, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || fallback < 0 {
		l.logf("Lease: unreadable marker %s, removing it", l.marker)
		l.removeMarker()
		return
	}
	if cur := l.getCurrent(); cur > fallback || cur < 0 {
		l.setCurrent(fallback)
		l.logf("Lease: was active at the last stop, current lowered from %d A to %d A", cur, fallback)
	}
	l.removeMarker()
}

func (l *lease) stopLocked() {
	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
	}
	l.gen++
	l.active = false
	l.removeMarker()
}

func (l *lease) writeMarker(fallback int) {
	if l.marker == "" {
		return
	}
	tmp := l.marker + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(fallback)+"\n"), 0o644); err != nil {
		l.logf("Lease: cannot write marker: %v", err)
		return
	}
	if err := os.Rename(tmp, l.marker); err != nil {
		l.logf("Lease: cannot write marker: %v", err)
	}
}

func (l *lease) removeMarker() {
	if l.marker == "" {
		return
	}
	if err := os.Remove(l.marker); err != nil && !os.IsNotExist(err) {
		l.logf("Lease: cannot remove marker: %v", err)
	}
}
