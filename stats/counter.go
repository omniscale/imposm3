package stats

import (
	"sync"
	"sync/atomic"
	"time"
)

type ElementCount struct {
	Current int64
	Total   int64
	Rps     float64
	LastRps float64
}

func NewRpsCounter() *RpsCounter {
	return &RpsCounter{
		mu: &sync.Mutex{},
	}
}

type RpsCounter struct {
	counter int64
	lastAdd int64
	start   time.Time
	stop    time.Time
	updated bool
	mu      *sync.Mutex
	total   int64
}

func (r *RpsCounter) Add(n int) {
	atomic.AddInt64(&r.counter, int64(n))
	atomic.AddInt64(&r.lastAdd, int64(n))
	if n > 0 {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.start.IsZero() {
			r.start = time.Now()
		}
		r.updated = true
	}
}

func (r *RpsCounter) Value() int64 {
	return atomic.LoadInt64(&r.counter)
}

func (r *RpsCounter) Rps() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return float64(atomic.LoadInt64(&r.counter)) / float64(r.stop.Sub(r.start).Seconds())
}

func (r *RpsCounter) LastRps() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return float64(atomic.LoadInt64(&r.lastAdd)) / float64(time.Since(r.stop).Seconds())
}

func (r *RpsCounter) Progress() float64 {
	if r.total == 0 {
		return -1.0
	}

	return float64(atomic.LoadInt64(&r.counter)) / float64(r.total)
}

func (r *RpsCounter) Count() ElementCount {
	return ElementCount{
		Current: r.Value(),
		Total:   r.total,
		Rps:     r.Rps(),
		LastRps: r.LastRps(),
	}
}

func (r *RpsCounter) Tick() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.updated {
		r.stop = time.Now()
		r.updated = false
	}
	atomic.StoreInt64(&r.lastAdd, 0)
}
