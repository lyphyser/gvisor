// Copyright 2018 The gVisor Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kernel

import (
	"fmt"
	"time"

	"gvisor.dev/gvisor/pkg/atomicbitops"
	"gvisor.dev/gvisor/pkg/log"
	"gvisor.dev/gvisor/pkg/sentry/ktime"
	sentrytime "gvisor.dev/gvisor/pkg/sentry/time"
	"gvisor.dev/gvisor/pkg/sync"
	"gvisor.dev/gvisor/pkg/tcpip"
)

// Run states of the Timekeeper's updater goroutine, held atomically in
// Timekeeper.updaterState. updaterStopped is a lifecycle state in which the
// updater must not write the VDSO page; once stopUpdater returns, there is no
// updater goroutine or timer. The running states are ordered from least to most
// active (parked < idle < active): the updater goroutine steps down them one at a
// time while idle (active->idle->parked), and addRef jumps a non-stopped updater
// straight back to updaterActive.
const (
	// updaterStopped indicates that updates are stopped or stopping. This is the
	// zero value, and is published by stopUpdater before it waits for the updater
	// goroutine to exit. Normal idle wake paths must not leave this state.
	updaterStopped int32 = iota

	// updaterParked indicates the updater goroutine has stopped refreshing. The
	// goroutine stores it at its second idle tick and then re-checks refs, committing
	// to the park (blocking in its select with the timer unarmed) only if no reference
	// is held, otherwise restoring active. startUpdater also starts the updater here,
	// since with no reference held there is nothing to refresh until the first addRef.
	// A committed park is resumed only by addRef, which under updateMu re-calibrates and
	// re-arms the timer so the goroutine ticks again from the next interval; stopUpdater
	// also leaves the state, tearing the updater down to updaterStopped.
	updaterParked

	// updaterIdle indicates that the updater goroutine has seen one idle tick (no
	// references held) and refreshed once more; if the next tick is also idle it
	// parks. It is the "about to park" state: reaching updaterParked requires two
	// consecutive idle ticks with no reference taken in between, i.e. a whole update
	// interval with nothing needing a fresh clock, so a workload that holds a
	// reference at least once per interval never parks. Set only by the updater
	// goroutine itself, on its first idle tick.
	updaterIdle

	// updaterActive indicates that the updater goroutine refreshes the clock
	// parameters periodically because a reference is (or was just) held. Reached via
	// addRef -- when a task starts running (incRunningTasks) or a clock read begins
	// (GetTime); startUpdater publishes it directly only if a reference is already held
	// at start, otherwise it starts parked. Held by the updater goroutine while the
	// reference count is nonzero.
	updaterActive
)

// Timekeeper manages all of the kernel clocks.
//
// +stateify savable
type Timekeeper struct {
	// clocks are the clock sources.
	//
	// These are not saved directly, as the new machine's clock may behave
	// differently.
	//
	// It is set only once, by SetClocks.
	clocks sentrytime.Clocks `state:"nosave"`

	// realtimeClock is a ktime.Clock based on timekeeper's Realtime.
	realtimeClock *timekeeperClock

	// monotonicClock is a ktime.Clock based on timekeeper's Monotonic.
	monotonicClock *timekeeperClock

	// bootTime is the realtime when the system "booted". i.e., when
	// SetClocks was called in the initial (not restored) run.
	bootTime ktime.Time

	// monotonicOffset is the offset to apply to the monotonic clock output
	// from clocks.
	//
	// It is set only once, by SetClocks.
	monotonicOffset int64 `state:"nosave"`

	// monotonicLowerBound is the lowerBound for monotonic time.
	monotonicLowerBound atomicbitops.Int64 `state:"nosave"`

	// restored, if non-nil, indicates that this Timekeeper was restored
	// from a state file. The clocks are not set until restored is closed.
	restored chan struct{} `state:"nosave"`

	// saveMonotonic is the (offset) value of the monotonic clock at the
	// time of save.
	//
	// It is only valid if restored is non-nil.
	//
	// It is only used in SetClocks after restore to compute the new
	// monotonicOffset.
	saveMonotonic int64

	// saveRealtime is the value of the realtime clock at the time of save.
	//
	// It is only valid if restored is non-nil.
	//
	// It is only used in SetClocks after restore to compute the new
	// monotonicOffset.
	saveRealtime int64

	// mu protects destruction with stop and wg.
	mu sync.Mutex `state:"nosave"`

	// stop is closed to tell the updater goroutine to exit. The goroutine
	// selects on it even while parked, so closing it wakes a parked goroutine.
	stop chan struct{} `state:"nosave"`

	// wg is used to indicate that the update goroutine has exited.
	wg sync.WaitGroup `state:"nosave"`

	// updaterState is the lifecycle/run state of the updater goroutine (one of
	// updaterStopped, updaterParked, updaterIdle, updaterActive). The updater goroutine
	// winds it down one step per idle tick (active->idle then idle->parked), and addRef
	// raises it back to active when a reference is taken -- locklessly with an
	// idle->active CAS, or under updateMu when it must wake and re-calibrate a parked
	// updater. The two are kept race-free by store-then-load on each side: the
	// goroutine stores the lowered state and then re-reads refs (restoring active if a
	// reference is held), while addRef adds its reference and then reads the state, so a
	// referenced updater is never left winding down toward a park. The running states
	// are ordered parked < idle < active. updaterStopped is left only by startUpdater.
	// Transitions out of updaterStopped or updaterParked publish the new state only
	// after a refresh, so readers cannot observe stale parameters from a stopped or
	// parked interval as already-running.
	updaterState atomicbitops.Int32 `state:"nosave"`

	// refs is the number of references pinning the updater goroutine in the active
	// (refreshing) state; see addRef. A reference is held while tasks are running
	// (incRunningTasks..decRunningTasks) and across each internal clock read
	// (GetTime). The updater goroutine reads it under updateMu and only winds down
	// toward a park while it is zero. It is nosave: at save time no task is running
	// and no read is in flight, so refs is zero, and it is rebuilt as tasks resume and
	// reads occur.
	refs atomicbitops.Int64 `state:"nosave"`

	// updateMu guards the VDSO parameter page (which has a single writer) and every
	// refresh of it: the updater goroutine's whole tick, startUpdater's refresh when it
	// starts active (a reference already held), stopUpdater's publish of updaterStopped,
	// and addRef's slow path (waking and re-calibrating a parked updater). It does not
	// guard every updaterState write -- addRef's idle->active CAS is lockless -- but
	// every write that publishes a fresh-parameter transition (out of parked or stopped)
	// holds it. incRunningTasks holds runningTasksMu across its addRef call, fixing the
	// lock order at runningTasksMu -> updateMu; this cannot deadlock, since no updateMu
	// holder ever takes runningTasksMu (the updater goroutine reads refs as a bare
	// atomic, never acquiring runningTasksMu). The re-calibrate (a clock syscall) runs
	// under updateMu, but only a task resume from a fully idle sandbox and the idle
	// CPU-clock ticker take runningTasksMu, so holding it across that re-calibrate is
	// not a hot-path cost.
	updateMu sync.Mutex `state:"nosave"`

	// timer fires when the next periodic refresh is due, and is also how the
	// parked goroutine is woken: while parked it blocks in a select on timer.C
	// with the timer unarmed (so it never fires); addRef arms it for
	// the next tick.
	timer *time.Timer `state:"nosave"`

	// params is the VDSO parameter page that the updater writes sampled clock
	// parameters to. It is set by startUpdater (from SetClocks/ResumeUpdates) so
	// that addRef can refresh without it being plumbed through every
	// caller.
	params *VDSOParamPage `state:"nosave"`
}

// NewTimekeeper returns a Timekeeper that is automatically kept up-to-date.
// NewTimekeeper does not take ownership of paramPage.
//
// SetClocks must be called on the returned Timekeeper before it is usable.
func NewTimekeeper() *Timekeeper {
	t := Timekeeper{}
	t.realtimeClock = &timekeeperClock{tk: &t, c: sentrytime.Realtime}
	t.monotonicClock = &timekeeperClock{tk: &t, c: sentrytime.Monotonic}
	return &t
}

// SetClocks the backing clock source.
//
// SetClocks must be called before the Timekeeper is used, and it may not be
// called more than once, as changing the clock source without extra correction
// could cause time discontinuities.
//
// It must also be called after Load.
func (t *Timekeeper) SetClocks(c sentrytime.Clocks, params *VDSOParamPage) {
	// Update the params, marking them "not ready", as we may need to
	// restart calibration on this new machine.
	if t.restored != nil {
		if err := params.Write(func() vdsoParams {
			return vdsoParams{}
		}); err != nil {
			panic("unable to reset VDSO params: " + err.Error())
		}
	}

	if t.clocks != nil {
		panic("SetClocks called on previously-initialized Timekeeper")
	}

	t.clocks = c

	// Compute the offset of the monotonic clock from the base Clocks.
	//
	// In a fresh (not restored) sentry, monotonic time starts at zero.
	//
	// In a restored sentry, monotonic time jumps forward by approximately
	// the same amount as real time. There are no guarantees here, we are
	// just making a best-effort attempt to make it appear that the app
	// was simply not scheduled for a long period, rather than that the
	// real time clock was changed.
	//
	// If real time went backwards, it remains the same.
	wantMonotonic := int64(0)

	nowMonotonic, err := t.clocks.GetTime(sentrytime.Monotonic)
	if err != nil {
		panic("Unable to get current monotonic time: " + err.Error())
	}

	nowRealtime, err := t.clocks.GetTime(sentrytime.Realtime)
	if err != nil {
		panic("Unable to get current realtime: " + err.Error())
	}

	if t.restored != nil {
		wantMonotonic = t.saveMonotonic
		elapsed := nowRealtime - t.saveRealtime
		if elapsed > 0 {
			wantMonotonic += elapsed
		}
	}

	t.monotonicOffset = wantMonotonic - nowMonotonic

	if t.restored == nil {
		// Hold on to the initial "boot" time.
		t.bootTime = ktime.FromNanoseconds(nowRealtime)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.startUpdater(params)

	if t.restored != nil {
		close(t.restored)
	}
}

// update samples the backing clocks and writes the new parameters to the VDSO
// parameter page. parked indicates that this refresh resumes a parked
// (unobserved) period, so the backing clocks re-calibrate rather than slew; see
// sentrytime.Clocks.Update. Only addRef passes true.
//
// Preconditions: updateMu must be held (the VDSO parameter page has a single
// writer; see updateMu).
func (t *Timekeeper) update(parked bool) {
	// Call Update within a Write block to prevent the VDSO from using the old
	// params between Update and Write.
	if err := t.params.Write(func() vdsoParams {
		monotonicParams, monotonicOk, realtimeParams, realtimeOk := t.clocks.Update(parked)

		var p vdsoParams
		if monotonicOk {
			p.monotonicReady = 1
			p.monotonicBaseCycles = int64(monotonicParams.BaseCycles)
			p.monotonicBaseRef = int64(monotonicParams.BaseRef) + t.monotonicOffset
			p.monotonicFrequency = monotonicParams.Frequency
		}
		if realtimeOk {
			p.realtimeReady = 1
			p.realtimeBaseCycles = int64(realtimeParams.BaseCycles)
			p.realtimeBaseRef = int64(realtimeParams.BaseRef)
			p.realtimeFrequency = realtimeParams.Frequency
		}
		return p
	}); err != nil {
		log.Warningf("Unable to update VDSO parameter page: %v", err)
	}
}

// startUpdater starts an update goroutine that keeps the clocks updated.
//
// mu must be held.
func (t *Timekeeper) startUpdater(params *VDSOParamPage) {
	if t.stop != nil {
		// Timekeeper already started
		return
	}
	t.stop = make(chan struct{})
	t.params = params

	// The Go runtime uses host CLOCK_MONOTONIC to service the timer, so it may run at
	// a *slightly* different rate from the application CLOCK_MONOTONIC. That is fine,
	// as we only need to update at approximately this rate.
	//
	// Create the timer stopped: the updater starts parked (below), so no tick is
	// pending until a reference arms it. addRef may Reset this timer as soon as the
	// parked state is published, so it must already exist. No channel drain is needed
	// after Stop: the timer channel is unbuffered and, since Go 1.23, Stop and Reset are
	// guaranteed never to deliver a value prepared before the call.
	t.timer = time.NewTimer(sentrytime.ApproxUpdateInterval)
	t.timer.Stop()

	// Start the updater parked rather than active: while nothing holds a reference
	// there is nothing to refresh, so the updater stays parked until the first addRef
	// wakes and re-calibrates it (see addRef). Nothing is written to the VDSO page
	// here -- a parked state means any reader re-calibrates before reading, so the page
	// is brought fresh on first use.
	//
	// Store updaterParked and then re-read refs. If a reference is already held -- e.g.
	// an internal clock read racing ResumeUpdates -- bring the updater active: arm the
	// timer (anchoring the next tick to now rather than to the end of the refresh),
	// re-calibrate (the params are stale), and publish updaterActive last, only after
	// the refresh so the invariant "a non-parked, non-stopped state implies fresh VDSO
	// parameters" holds. This is the same store-then-load the updater goroutine uses: a
	// concurrent addRef adds its reference before reading the state, so a held reference
	// is never left parked -- either we observe it here, or addRef observes updaterParked
	// and wakes the updater via its slow path under updateMu.
	t.updateMu.Lock()
	t.updaterState.Store(updaterParked)
	if t.refs.Load() != 0 {
		t.timer.Reset(sentrytime.ApproxUpdateInterval)
		t.update(true)
		t.updaterState.Store(updaterActive)
	}
	t.updateMu.Unlock()

	// Once running, the updater winds itself down to a park while nothing holds a
	// reference (no tasks running and no clock reads in flight): it lowers its run
	// state one step per tick (active->idle on the first idle tick, idle->parked on
	// the second), and once parked blocks in the select below with the timer left
	// unarmed, so no host timer is pending. Two consecutive idle ticks mean a whole
	// update interval with nothing needing a fresh clock, so a workload that holds a
	// reference at least once per interval keeps refreshing every tick. The goroutine
	// stays active while the reference count is nonzero (see addRef); a reference is
	// held for the whole time tasks are running or a clock read is in flight, and
	// addRef re-calibrates and re-activates a parked updater synchronously under
	// updateMu, so a referenced updater never parks and a referencing caller never
	// observes stale data.
	t.wg.Add(1)
	go func() { // S/R-SAFE: stopped during save.
		defer t.wg.Done()
		defer t.timer.Stop()
		for {
			// Wait for the next tick (or, if parked, until a waker arms the
			// timer), draining timer.C so the next park starts unarmed.
			select {
			case <-t.timer.C:
			case <-t.stop:
				return
			}

			// The whole tick runs under updateMu so the refresh has the single-writer
			// VDSO page to itself and serializes with start/stopUpdater and addRef's
			// slow path. updaterState is still also flipped idle->active locklessly by
			// addRef's fast path, so the goroutine does not assume the state is stable:
			// each wind-down below is a store-then-load (store the lowered state, then
			// re-read refs) that stays race-free against a concurrent addRef.
			t.updateMu.Lock()

			switch t.updaterState.Load() {
			case updaterStopped:
				// stopUpdater published updaterStopped; exit without touching the page.
				t.updateMu.Unlock()
				return

			case updaterActive:
				// First idle-candidate tick: wind down to idle, then re-check refs.
				// Store-before-load is essential and pairs with addRef, which adds its
				// reference before reading the state: if addRef's reference is not yet
				// visible to us here, then our idle store is already visible to addRef,
				// which flips it back to active; if the reference is visible, we undo
				// the wind-down below. Either way a referenced updater is never left
				// winding down.
				t.updaterState.Store(updaterIdle)
				if t.refs.Load() != 0 {
					t.updaterState.Store(updaterActive)
				}

			case updaterIdle:
				// Second idle tick: wind down to parked, then re-check refs (same
				// store-then-load as the active case). A blind store, then the refs
				// check, is what lets addRef flip idle->active locklessly: if addRef's
				// CAS wins, our store would clobber it, but we then see its reference
				// and restore active; if addRef's CAS loses to our store, it retries
				// and takes the slow path against the parked state.
				t.updaterState.Store(updaterParked)
				if t.refs.Load() != 0 {
					// A reference was taken while winding down: restore active and
					// refresh below.
					t.updaterState.Store(updaterActive)
				} else {
					// No reference: commit to the park. Leave the timer unarmed (drained
					// by the receive above) so the select blocks until a waker (addRef)
					// re-arms it, and skip the refresh.
					t.updateMu.Unlock()
					continue
				}
			}

			// Active, the first idle tick, or a reference seen while winding down:
			// refresh once more (so a caller resuming before the next tick still finds
			// fresh params) and re-arm, anchoring the next tick to now rather than
			// pushing it out by the refresh.
			t.timer.Reset(sentrytime.ApproxUpdateInterval)
			t.update(false)
			t.updateMu.Unlock()
		}
	}()
}

// addRef pins the updater goroutine in the active (refreshing) state until the
// matching release, waking and re-calibrating it if it had parked. The updater winds
// down toward a park only while the reference count is zero, so a caller holds a
// reference for any window during which the clock must stay fresh: incRunningTasks
// holds one while tasks are running, and GetTime holds one across each read. Holding
// the reference for the whole window keeps the clock fresh throughout, even if the
// caller is descheduled in the middle of it.
//
// addRef adds its reference and then inspects the state, while the goroutine winds
// the state down a step and then re-checks refs (see startUpdater). Storing before
// loading on both sides is what makes them race-free without a shared lock on the
// fast path: whenever the goroutine does not observe this reference, its wound-down
// store is visible here, and vice versa. So:
//
//   - active: nothing to do; the reference alone keeps the goroutine from parking.
//   - idle: flip idle->active with a CAS, ignoring the result (no retry needed). If
//     the CAS loses, the state moved to active (already done), to stopped (left for
//     ResumeUpdates), or to parked by the goroutine winding down -- and that goroutine
//     then observes this reference via its store-then-load and restores active. A
//     plain Store would be wrong: it could clobber a concurrent stopUpdater's
//     updaterStopped, since GetTime (hence addRef) may run while updates are paused.
//   - stopped: nothing to do; updates are paused, so the state is left stopped and the
//     reference is merely counted. ResumeUpdates restarts the updater -- parked, or
//     active (after a refresh) if a reference is still held then -- and any reference
//     held across the restart keeps it active.
//   - parked: take updateMu and re-calibrate synchronously (the params have drifted)
//     before addRef returns, so the caller never reads a stale clock, and re-arm the
//     timer so the goroutine resumes ticking from the next interval. The state is
//     re-loaded under the lock since it may have changed (e.g. a concurrent
//     stopUpdater) while we waited.
func (t *Timekeeper) addRef() {
	t.refs.Add(1)
	switch t.updaterState.Load() {
	case updaterActive:
		// Already active; the reference keeps it so.
	case updaterIdle:
		t.updaterState.CompareAndSwap(updaterIdle, updaterActive)
	case updaterStopped:
		// Updates are paused; leave the state stopped (see the doc comment).
	case updaterParked:
		// updaterParked: handle under updateMu, re-loading the state since it may have
		// changed (e.g. a concurrent stopUpdater) while we waited for the lock.
		t.updateMu.Lock()
		cur := t.updaterState.Load()
		if cur == updaterParked {
			// Drifted while parked: re-calibrate (parked=true re-samples rather than
			// slews, landing where a continuously-updated clock would be) and re-arm
			// the timer to wake the parked goroutine.
			t.timer.Reset(sentrytime.ApproxUpdateInterval)
			t.update(true)
		}
		if cur != updaterStopped {
			t.updaterState.Store(updaterActive)
		}
		t.updateMu.Unlock()
	}
}

// release drops a reference taken by addRef. Once the count reaches zero the updater
// goroutine is free to wind down and park at its next ticks; nothing is done here, as
// the goroutine notices the drop on its own.
func (t *Timekeeper) release() {
	if t.refs.Add(-1) < 0 {
		panic("Timekeeper.release called with no reference held")
	}
}

// stopUpdater stops the update goroutine, blocking until it exits.
//
// mu must be held.
func (t *Timekeeper) stopUpdater() {
	if t.stop == nil {
		// Updater not running.
		return
	}

	// updaterStopped is published under updateMu so a concurrent addRef (from a clock
	// read or a task resume) that arrives after this point cannot restart the updater
	// or rewrite the VDSO page.
	t.updateMu.Lock()
	t.updaterState.Store(updaterStopped)
	t.updateMu.Unlock()

	// Closing stop tells the goroutine to exit and wakes it if it is parked,
	// since the timer select also waits on stop.
	close(t.stop)
	t.wg.Wait()
	t.stop = nil
	t.timer = nil
	t.params = nil
}

// Destroy destroys the Timekeeper, freeing all associated resources.
func (t *Timekeeper) Destroy() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.stopUpdater()
}

// PauseUpdates stops clock parameter updates. This should only be used while
// tasks cannot access the VDSO parameter page from userspace.
func (t *Timekeeper) PauseUpdates() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopUpdater()
}

// ResumeUpdates restarts clock parameter updates stopped by PauseUpdates.
func (t *Timekeeper) ResumeUpdates(params *VDSOParamPage) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.startUpdater(params)
}

// GetTime returns the current time in nanoseconds.
func (t *Timekeeper) GetTime(c sentrytime.ClockID) (int64, error) {
	if t.clocks == nil {
		if t.restored == nil {
			panic("Timekeeper used before initialized with SetClocks")
		}
		<-t.restored
	}

	// Hold a reference across the read so the updater stays active -- and the clock
	// fresh -- for the whole read, waking and re-calibrating it if it had parked, even
	// if this goroutine is descheduled mid-read. A stopped updater (updates paused) is
	// left alone, serving the read from the backing clocks without mutating the VDSO
	// page.
	t.addRef()
	defer t.release()

	now, err := t.clocks.GetTime(c)
	if err == nil && c == sentrytime.Monotonic {
		now += t.monotonicOffset
		for {
			// It's possible that the clock is shaky. This may be due to
			// platform issues, e.g. the KVM platform relies on the guest
			// TSC and host TSC, which may not be perfectly in sync. To
			// work around this issue, ensure that the monotonic time is
			// always bounded by the last time read.
			oldLowerBound := t.monotonicLowerBound.Load()
			if now < oldLowerBound {
				now = oldLowerBound
				break
			}
			if t.monotonicLowerBound.CompareAndSwap(oldLowerBound, now) {
				break
			}
		}
	}
	return now, err
}

// BootTime returns the system boot real time.
func (t *Timekeeper) BootTime() ktime.Time {
	return t.bootTime
}

// timekeeperClock is a ktime.SampledClock that reads time from a
// kernel.Timekeeper-managed clock.
//
// +stateify savable
type timekeeperClock struct {
	tk *Timekeeper
	c  sentrytime.ClockID

	// Implements ktime.SampledClock.WallTimeUntil.
	ktime.WallRateClock `state:"nosave"`

	// Implements waiter.Waitable. (We have no ability to detect
	// discontinuities from external changes to CLOCK_REALTIME).
	ktime.NoClockEvents `state:"nosave"`
}

// Now implements ktime.Clock.Now.
func (tc *timekeeperClock) Now() ktime.Time {
	now, err := tc.tk.GetTime(tc.c)
	if err != nil {
		panic(fmt.Sprintf("timekeeperClock(ClockID=%v)).Now: %v", tc.c, err))
	}
	return ktime.FromNanoseconds(now)
}

// NewTimer implements ktime.Clock.NewTimer.
func (tc *timekeeperClock) NewTimer(l ktime.Listener) ktime.Timer {
	return ktime.NewSampledTimer(tc, l)
}

var _ tcpip.Clock = (*Timekeeper)(nil)

// Now implements tcpip.Clock.
func (t *Timekeeper) Now() time.Time {
	nsec, err := t.GetTime(sentrytime.Realtime)
	if err != nil {
		panic("timekeeper.GetTime(sentrytime.Realtime): " + err.Error())
	}
	return time.Unix(0, nsec)
}

// NowMonotonic implements tcpip.Clock.
func (t *Timekeeper) NowMonotonic() tcpip.MonotonicTime {
	nsec, err := t.GetTime(sentrytime.Monotonic)
	if err != nil {
		panic("timekeeper.GetTime(sentrytime.Monotonic): " + err.Error())
	}
	var mt tcpip.MonotonicTime
	return mt.Add(time.Duration(nsec) * time.Nanosecond)
}

// AfterFunc implements tcpip.Clock.
func (t *Timekeeper) AfterFunc(d time.Duration, f func()) tcpip.Timer {
	timer := &timekeeperTcpipTimer{
		clock: t.monotonicClock,
		fn:    f,
	}
	timer.Reset(d)
	return timer
}

// timekeeperTcpipTimer implements tcpip.Timer by wrapping a ktime.SampledTimer.
// tcpip.Timer does not define a Destroy method, so each timer expiration and
// each call to Timer.Stop() must release all resources by calling
// ktime.SampledTimer.Destroy().
type timekeeperTcpipTimer struct {
	// immutable
	clock *timekeeperClock
	fn    func()

	// mu protects t.
	mu timekeeperTcpipTimerMutex

	// t stores the latest running Timer. This is replaced whenever Reset is
	// called since Timer cannot be restarted once it has been Destroyed by Stop.
	//
	// This field is nil iff Stop has been called.
	t *ktime.SampledTimer

	// resets is the number of times Reset has been called. resets is written
	// with both mu and ktime.SampledTimer locks held, so it may be read with
	// either or both locks held.
	resets int
}

// Stop implements tcpip.Timer.Stop.
func (r *timekeeperTcpipTimer) Stop() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.t == nil {
		return false
	}
	_, lastSetting := r.t.Set(ktime.Setting{}, nil)
	r.t.Destroy()
	r.t = nil
	return lastSetting.Enabled
}

// stopExpired is equivalent to Stop, but is called when the timer expires.
func (r *timekeeperTcpipTimer) stopExpired(reset int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.t == nil || r.resets != reset {
		return
	}
	r.t.Destroy()
	r.t = nil
}

// Reset implements tcpip.Timer.Reset.
func (r *timekeeperTcpipTimer) Reset(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.t == nil {
		r.t = ktime.NewSampledTimer(r.clock, r)
	}
	r.t.Set(ktime.Setting{
		Enabled: true,
		Next:    r.clock.Now().Add(d),
	}, r.incResets)
}

func (r *timekeeperTcpipTimer) incResets() {
	r.resets++
}

// NotifyTimer implements ktime.Listener.NotifyTimer.
func (r *timekeeperTcpipTimer) NotifyTimer(exp uint64) {
	// Implementations of ktime.Listener.NotifyTimer() can't call Timer methods
	// due to lock ordering, so we must call r.t.Destroy() from another
	// goroutine. We also must call r.stopExpired() rather than r.Stop(), since
	// the latter might cancel an unrelated call to r.Reset() that happens
	// between now and when this goroutine runs.
	thisReset := r.resets
	go func() {
		r.stopExpired(thisReset)
		r.fn()
	}()
}
