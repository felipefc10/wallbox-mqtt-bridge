package bridge

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// A lease lets the controller that sets the current (for example evcc's load
// management) say how long its current may stand without hearing from it again:
//
//	<topic prefix>/lease/set   "<amps> <seconds> <fallback amps> [<seq>]"   or "0" to release
//
// Every lease sets the current to <amps> and restarts a countdown of <seconds>.
// If no new lease arrives in time, the bridge lowers the current to <fallback>
// on its own. The countdown runs here, on the charger, so it still fires when
// the controller, the broker or the network is gone: a lost link can never hold
// a high current. Without leases the bridge behaves exactly as before.
//
// Rules, all in the safe direction:
//   - The lease machinery never raises the current except to the <amps> of a
//     valid lease, and only once the marker (below) is safely on disk.
//   - A fallback is verified by reading the setting back; until it reads at or
//     below <fallback> it is retried every second.
//   - A release ("0") lowers to the last fallback if the current is above it.
//   - A stop of the bridge (SIGTERM) applies the fallback before exiting.
//
// While a lease is active a small marker file holds the fallback. The charger
// keeps max_charging_current across reboots, so if the bridge or the charger
// restarts mid-lease, the fallback is applied at start. An unreadable marker
// (for example cut by a power loss) lowers to the charger minimum instead.
//
// The controller must publish leases non-retained (a retained lease is ignored
// here) and should use QoS 0, so that a lease queued during its own reconnect
// is dropped rather than delivered late. A lease delayed in flight still counts
// from its arrival, so the worst-case hold is <seconds> plus the network delay.
// <seq>, optional, is a counter the controller increases with every lease. A lease
// just behind the last one seen (up to leaseSeqWindow back) arrived out of order and
// is dropped, so an older, higher current can never overtake a newer, lower one. A
// much lower <seq> means the controller restarted its count: it is taken.
type lease struct {
	getCurrent func() int // fresh read of the setting; -1 when unknown
	setCurrent func(int)
	maxAmps    int    // the largest <amps> accepted
	marker     string // path of the marker file; "" disables it (tests)
	logf       func(format string, args ...any)
	retry      time.Duration // between attempts to apply a fallback

	mu         sync.Mutex
	timer      *time.Timer
	fallback   int  // fallback of the active lease, or of the last one after a release
	active     bool // a lease is running
	gen        uint64
	lastSeq    int64 // highest <seq> seen, 0 before the first
	badPayload int   // malformed and out-of-order payloads seen, for a quiet log
}

// leaseMsg is one parsed lease message
type leaseMsg struct {
	amps, fallback int
	hold           time.Duration
	seq            int64 // 0 when absent
	release        bool
}

// leaseMinAmps is the lowest charging current of a charger (IEC 61851): the safe
// target when the fallback itself is unknown.
const leaseMinAmps = 6

// leaseSeqWindow is how far behind the last <seq> a lease counts as out of order
// rather than as a restarted controller. Reordering spans a few messages at most.
const leaseSeqWindow = 100

func newLease(get func() int, set func(int), maxAmps int, marker string) *lease {
	return &lease{
		getCurrent: get, setCurrent: set, maxAmps: maxAmps, marker: marker,
		logf:  func(f string, a ...any) { fmt.Printf(f+"\n", a...) },
		retry: time.Second,
	}
}

// parseLease reads "<amps> <seconds> <fallback> [<seq>]" or "0". ok is false for anything else.
func parseLease(payload string, maxAmps int) (m leaseMsg, ok bool) {
	f := strings.Fields(payload)
	if len(f) == 1 && f[0] == "0" {
		return leaseMsg{release: true}, true
	}
	if len(f) != 3 && len(f) != 4 {
		return leaseMsg{}, false
	}
	a, err1 := strconv.Atoi(f[0])
	s, err2 := strconv.ParseFloat(f[1], 64)
	fb, err3 := strconv.Atoi(f[2])
	if err1 != nil || err2 != nil || err3 != nil || math.IsNaN(s) ||
		a < 0 || a > maxAmps || fb < 0 || fb > maxAmps || s < 1 || s > 3600 {
		return leaseMsg{}, false
	}
	m = leaseMsg{amps: a, hold: time.Duration(s * float64(time.Second)), fallback: fb}
	if len(f) == 4 {
		seq, err := strconv.ParseInt(f[3], 10, 64)
		if err != nil || seq <= 0 {
			return leaseMsg{}, false
		}
		m.seq = seq
	}
	return m, true
}

// handle applies one lease message.
func (l *lease) handle(payload string) {
	msg, ok := parseLease(payload, l.maxAmps)

	l.mu.Lock()
	defer l.mu.Unlock()

	if !ok {
		// neither renews nor cancels; logged once, then every 60th time
		if l.badPayload%60 == 0 {
			l.logf("Lease: ignoring malformed payload %q (want \"<amps> <seconds> <fallback> [<seq>]\" or \"0\", amps up to %d)", payload, l.maxAmps)
		}
		l.badPayload++
		return
	}
	if msg.seq != 0 {
		if msg.seq <= l.lastSeq && msg.seq > l.lastSeq-leaseSeqWindow {
			if l.badPayload%60 == 0 {
				l.logf("Lease: dropping out-of-order lease %q (last seq %d)", payload, l.lastSeq)
			}
			l.badPayload++
			return
		}
		l.lastSeq = msg.seq
	}
	amps, hold, fallback, release := msg.amps, msg.hold, msg.fallback, msg.release

	if release {
		wasActive := l.active
		l.stopLocked()
		if wasActive {
			l.logf("Lease: released")
			l.applyFallbackLocked("released")
		}
		return
	}

	// Nothing above the fallback to protect: apply the current, no countdown.
	if amps <= fallback {
		l.stopLocked()
		l.fallback = fallback
		l.setIfDifferent(amps)
		return
	}

	// The marker goes down before the current goes up: a restart in between
	// must still find the fallback. Without it, the current is not raised.
	if !l.active || l.fallback != fallback {
		if err := l.writeMarker(fallback); err != nil {
			l.logf("Lease: cannot store the fallback (%v), not raising above %d A", err, fallback)
			amps = fallback
		}
	}
	if !l.active {
		l.logf("Lease: active, %d A, fallback %d A after %s without renewal", amps, fallback, hold)
	}
	l.active = true
	l.fallback = fallback
	l.setIfDifferent(amps)

	l.gen++
	gen := l.gen
	if l.timer != nil {
		l.timer.Stop()
	}
	l.timer = time.AfterFunc(hold, func() { l.expire(gen) })
}

func (l *lease) setIfDifferent(amps int) {
	if l.getCurrent() != amps {
		l.setCurrent(amps)
	}
}

// expire runs when a lease was not renewed in time, and again every retry until
// the fallback reads back.
func (l *lease) expire(gen uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active || gen != l.gen { // renewed or released meanwhile
		return
	}
	if !l.applyFallbackLocked("expired") {
		l.timer = time.AfterFunc(l.retry, func() { l.expire(gen) })
		return
	}
	l.active = false
	l.timer = nil
	l.removeMarker()
}

// applyFallbackLocked lowers the current to the fallback if it is above it (or
// unknown) and reports whether the setting now reads at or below the fallback.
func (l *lease) applyFallbackLocked(why string) bool {
	cur := l.getCurrent()
	if cur >= 0 && cur <= l.fallback {
		if why == "expired" {
			l.logf("Lease: expired, current already %d A (fallback %d A)", cur, l.fallback)
		}
		return true
	}
	l.setCurrent(l.fallback)
	after := l.getCurrent()
	if after >= 0 && after <= l.fallback {
		l.logf("Lease: %s, current lowered from %d A to %d A", why, cur, l.fallback)
		return true
	}
	l.logf("Lease: %s, lowering to %d A did not take (reads %d A), retrying", why, l.fallback, after)
	return false
}

// applyStale applies the fallback of a lease that was active when the bridge or
// the charger last stopped. Call once at start, before taking commands. It
// returns only once the charger reads at or below the target.
func (l *lease) applyStale() {
	if l.marker == "" {
		return
	}
	b, err := os.ReadFile(l.marker)
	if os.IsNotExist(err) {
		return // no lease was active
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	fallback, perr := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || perr != nil || fallback < 0 {
		// The fallback is lost, but a lease was active: the charger minimum is safe.
		l.logf("Lease: unreadable marker %s, lowering to the charger minimum %d A", l.marker, leaseMinAmps)
		fallback = leaseMinAmps
	}
	l.fallback = fallback
	for !l.applyFallbackLocked("was active at the last stop") {
		time.Sleep(l.retry)
	}
	l.removeMarker()
}

// shutdown applies the fallback of an active lease before the bridge exits: with
// no bridge running nothing would ever lower it. The marker stays until the
// fallback reads back, so a failed attempt is finished at the next start.
func (l *lease) shutdown() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active {
		return
	}
	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
	}
	l.gen++
	l.active = false
	if l.applyFallbackLocked("bridge stopping") {
		l.removeMarker()
	}
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

// writeMarker stores the fallback durably: written, synced, renamed, directory synced.
func (l *lease) writeMarker(fallback int) error {
	if l.marker == "" {
		return nil
	}
	tmp := l.marker + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, err = f.WriteString(strconv.Itoa(fallback) + "\n")
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, l.marker)
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	if d, err := os.Open(filepath.Dir(l.marker)); err == nil {
		d.Sync() // best effort: not every OS can sync a directory
		d.Close()
	}
	return nil
}

func (l *lease) removeMarker() {
	if l.marker == "" {
		return
	}
	if err := os.Remove(l.marker); err != nil && !os.IsNotExist(err) {
		l.logf("Lease: cannot remove marker: %v", err)
	}
}
