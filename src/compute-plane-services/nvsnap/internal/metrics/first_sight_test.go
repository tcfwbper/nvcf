// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// fakeSight is a firstSight on a clock the test moves; held observations
// run when the clock passes their time.
type fakeSight struct {
	*firstSight
	clock time.Time
	held  []heldObservation
}

type heldObservation struct {
	at time.Time
	f  func()
}

func newFakeSight(hold time.Duration) *fakeSight {
	s := &fakeSight{firstSight: newFirstSight(), clock: time.Unix(1_000_000, 0)}
	s.hold = hold
	s.now = func() time.Time { return s.clock }
	s.after = func(d time.Duration, f func()) { s.held = append(s.held, heldObservation{s.clock.Add(d), f}) }
	return s
}

func (s *fakeSight) advance(d time.Duration) {
	s.clock = s.clock.Add(d)
	var left []heldObservation
	for _, h := range s.held {
		if !h.at.After(s.clock) {
			h.f()
		} else {
			left = append(left, h)
		}
	}
	s.held = left
}

func TestFirstSightCreatesTheSeriesAtZeroBeforeItsFirstObservation(t *testing.T) {
	s := newFakeSight(2 * time.Minute)
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total"}, []string{"outcome"})
	inc := func() {
		s.observe("c", []string{"failed"}, func() { c.WithLabelValues("failed") }, func() { c.WithLabelValues("failed").Inc() })
	}

	inc()
	if n := len(series(t, c)); n != 1 {
		t.Fatalf("%d series after the first observation, want the one, at zero", n)
	}
	if v := value(t, c, "failed"); v != 0 {
		t.Fatalf("first observation applied at once (%v): a scrape would never see the series at 0, and increase() would not count it", v)
	}
	s.advance(time.Minute)
	inc() // a second one while the first is held is held too
	if v := value(t, c, "failed"); v != 0 {
		t.Fatalf("an observation within the hold applied at once (%v)", v)
	}
	s.advance(time.Minute)
	if v := value(t, c, "failed"); v != 2 {
		t.Fatalf("after the hold the series is %v, want both observations (2)", v)
	}
	inc()
	if v := value(t, c, "failed"); v != 3 {
		t.Fatalf("an observation after the hold is %v, want it applied at once (3)", v)
	}
}

func TestFirstSightHoldsEachSeriesOnItsOwn(t *testing.T) {
	s := newFakeSight(2 * time.Minute)
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "h_seconds"}, []string{"phase"})
	obs := func(phase string) {
		s.observe("h", []string{phase}, func() { h.WithLabelValues(phase) }, func() { h.WithLabelValues(phase).Observe(1) })
	}
	obs("gpu")
	s.advance(3 * time.Minute)
	obs("gpu")  // seen: at once
	obs("criu") // new: held
	if got := samples(t, h, "gpu"); got != 2 {
		t.Fatalf("gpu has %d samples, want 2", got)
	}
	if got := samples(t, h, "criu"); got != 0 {
		t.Fatalf("a new series' first sample applied at once (%d)", got)
	}
	// The same label values under another metric are another series.
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total"}, []string{"phase"})
	s.observe("c", []string{"gpu"}, func() { c.WithLabelValues("gpu") }, func() { c.WithLabelValues("gpu").Inc() })
	if v := value(t, c, "gpu"); v != 0 {
		t.Fatalf("another metric's series with the same labels was not held (%v)", v)
	}
}

// servicemonitorTemplate is the chart's ServiceMonitor, whose guard
// refuses scrape intervals the hold could not cover.
const servicemonitorTemplate = "../../deploy/helm/nvsnap/templates/agent-servicemonitor.yaml"

func TestFirstSightHoldCoversSeveralScrapes(t *testing.T) {
	raw, err := os.ReadFile(servicemonitorTemplate)
	if err != nil {
		t.Fatal(err)
	}
	// The guard's pattern: whole seconds up to its last alternative.
	m := regexp.MustCompile(`regexMatch "\^\(\[1-9\]\|\[1-3\]\[0-9\]\|(\d+)\)s\$"`).FindSubmatch(raw)
	if m == nil {
		t.Fatalf("%s has no scrape-interval guard", servicemonitorTemplate)
	}
	maxInterval, _ := strconv.Atoi(string(m[1]))
	if FirstSightHold < 3*time.Duration(maxInterval)*time.Second {
		t.Fatalf("FirstSightHold %s covers fewer than three of the %ds scrapes the chart allows", FirstSightHold, maxInterval)
	}
}

func TestObservePodStartAndTeardown(t *testing.T) {
	start := func() uint64 { return samples(t, PodStartSeconds, MethodCRIU, "GB300", "2-4") }
	teardowns := func(o string) float64 { return value(t, TeardownOutcomes, o, "GB300", "2-4") }
	timed := func() uint64 { return samples(t, TeardownSeconds, "GB300", "2-4") }
	s0, r0, d0, t0 := start(), teardowns(TeardownReleased), teardowns(TeardownDeadline), timed()

	ObservePodStart(MethodCRIU, "GB300", 4, 95*time.Second)
	ObservePodStart(MethodCRIU, "GB300", 4, -time.Second)
	if got := start() - s0; got != 1 {
		t.Fatalf("pod start observed +%d, want +1 (the negative one dropped)", got)
	}
	ObserveTeardown(TeardownReleased, "GB300", 4, 125*time.Second)
	ObserveTeardown(TeardownDeadline, "GB300", 4, 0)
	if got := teardowns(TeardownReleased) - r0; got != 1 {
		t.Fatalf("released teardowns +%v, want +1", got)
	}
	if got := teardowns(TeardownDeadline) - d0; got != 1 {
		t.Fatalf("deadline teardowns +%v, want +1", got)
	}
	if got := timed() - t0; got != 1 {
		t.Fatalf("teardown times +%d, want only the released one's", got)
	}
}

// A held series is released once every regular scraper has scraped since
// it was created: about one scrape interval, not the whole hold.
func TestFirstSightReleasesOnceEveryScraperReadTheZero(t *testing.T) {
	s := newFakeSight(2 * time.Minute)
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total"}, []string{"outcome"})
	inc := func() {
		s.observe("c", []string{"failed"}, func() { c.WithLabelValues("failed") }, func() { c.WithLabelValues("failed").Inc() })
	}
	scrape := func(from string) { s.scraped(from, s.clock) }
	// Two Prometheus replicas scraping every 30 s, and a person's curl once.
	scrape("10.0.0.1")
	scrape("10.0.0.2")
	s.advance(30 * time.Second)
	scrape("10.0.0.1")
	scrape("10.0.0.2")
	scrape("127.0.0.1")

	s.advance(time.Second)
	inc()
	// A scrape that began before the series existed did not read its zero.
	s.scraped("10.0.0.1", s.clock.Add(-time.Second))
	s.scraped("10.0.0.2", s.clock.Add(-time.Second))
	if v := value(t, c, "failed"); v != 0 {
		t.Fatalf("released by scrapes that began before the series was created (%v)", v)
	}
	s.advance(10 * time.Second)
	scrape("10.0.0.1")
	inc()
	if v := value(t, c, "failed"); v != 0 {
		t.Fatalf("released before the second replica read the zero (%v)", v)
	}
	s.advance(10 * time.Second)
	scrape("10.0.0.2")
	if v := value(t, c, "failed"); v != 2 {
		t.Fatalf("after every replica read the zero the series is %v, want both observations (2)", v)
	}
	inc()
	if v := value(t, c, "failed"); v != 3 {
		t.Fatalf("an observation after the release is %v, want it applied at once (3)", v)
	}
	// The hold's timer, firing later, applies nothing twice.
	s.advance(3 * time.Minute)
	if v := value(t, c, "failed"); v != 3 {
		t.Fatalf("the hold's end re-applied observations (%v)", v)
	}
}

// A one-off read releases nothing; with no regular scraper the hold's end
// releases the series; a scraper that went away stops counting.
func TestFirstSightIgnoresOneOffAndGoneScrapers(t *testing.T) {
	s := newFakeSight(2 * time.Minute)
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total"}, []string{"outcome"})
	inc := func(o string) {
		s.observe("c", []string{o}, func() { c.WithLabelValues(o) }, func() { c.WithLabelValues(o).Inc() })
	}
	inc("a")
	s.advance(time.Second)
	s.scraped("127.0.0.1", s.clock)
	if v := value(t, c, "a"); v != 0 {
		t.Fatalf("a one-off read released the series (%v)", v)
	}
	s.advance(2 * time.Minute)
	if v := value(t, c, "a"); v != 1 {
		t.Fatalf("with no regular scraper the hold's end left the series at %v, want 1", v)
	}

	// A regular scraper that stops: once its last scrape is older than the
	// hold, the remaining one alone releases.
	s.scraped("10.0.0.1", s.clock)
	s.scraped("10.0.0.2", s.clock)
	s.advance(30 * time.Second)
	s.scraped("10.0.0.1", s.clock)
	s.scraped("10.0.0.2", s.clock)
	for range 4 {
		s.advance(30 * time.Second)
		s.scraped("10.0.0.1", s.clock)
	}
	s.advance(time.Second)
	inc("b")
	s.advance(time.Second)
	s.scraped("10.0.0.1", s.clock)
	if v := value(t, c, "b"); v != 1 {
		t.Fatalf("a scraper gone for more than the hold still held the series (%v)", v)
	}
}

// The /metrics handler counts a scrape served whole, by client address.
func TestScrapedCountsServedScrapes(t *testing.T) {
	s := newFakeSight(2 * time.Minute)
	old := sight
	sight = s.firstSight
	t.Cleanup(func() { sight = old })
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total"}, []string{"outcome"})
	status := http.StatusOK
	h := Scraped(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
	get := func(from string) {
		r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		r.RemoteAddr = from
		h.ServeHTTP(httptest.NewRecorder(), r)
	}
	get("10.0.0.1:41000")
	s.advance(30 * time.Second)
	get("10.0.0.1:41002") // another port, the same scraper
	s.advance(time.Second)
	s.observe("c", []string{"x"}, func() { c.WithLabelValues("x") }, func() { c.WithLabelValues("x").Inc() })
	s.advance(time.Second)
	status = http.StatusInternalServerError
	get("10.0.0.1:41004")
	if v := value(t, c, "x"); v != 0 {
		t.Fatalf("a failed scrape released the series (%v)", v)
	}
	status = http.StatusOK
	get("10.0.0.1:41006")
	if v := value(t, c, "x"); v != 1 {
		t.Fatalf("a served scrape after creation left the series at %v, want 1", v)
	}
}

// The model cache's counters are held on first sight like the lifecycle
// counters: a node's first harvest, fetch or seeded init is counted by
// increase().
func TestModelCacheCountersAreHeldOnFirstSight(t *testing.T) {
	fs := newFakeSight(2 * time.Minute)
	prev := sight
	sight = fs.firstSight
	defer func() { sight = prev }()

	CountModelCacheInit("seeded")
	CountModelCacheOp("harvest", "ok")
	AddModelCacheBytes("harvest", 1<<20)
	for _, c := range []struct {
		name string
		vec  prometheus.Collector
		lbl  []string
		want float64
	}{
		{"inits", ModelCacheInits, []string{"seeded"}, 1},
		{"operations", ModelCacheOps, []string{"harvest", "ok"}, 1},
		{"bytes", ModelCacheBytes, []string{"harvest"}, 1 << 20},
	} {
		if n := len(series(t, c.vec)); n != 1 {
			t.Fatalf("%s: %d series after the first observation, want the one, at zero", c.name, n)
		}
		if v := value(t, c.vec, c.lbl...); v != 0 {
			t.Fatalf("%s: first observation applied at once (%v): increase() would not count it", c.name, v)
		}
		defer func(name string, vec prometheus.Collector, lbl []string, want float64) {
			if v := value(t, vec, lbl...); v != want {
				t.Errorf("%s after the hold = %v, want %v", name, v, want)
			}
		}(c.name, c.vec, c.lbl, c.want)
	}
	fs.advance(2 * time.Minute)
}
