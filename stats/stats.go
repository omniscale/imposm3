package stats

import (
	"fmt"
	"goposm/logging"
	"time"
)

type Counter struct {
	start     time.Time
	Coords    *RpsCounter
	Nodes     *RpsCounter
	Ways      *RpsCounter
	Relations *RpsCounter
}

func (c *Counter) Tick() {
	c.Coords.Tick()
	c.Nodes.Tick()
	c.Ways.Tick()
	c.Relations.Tick()
}

func NewCounter() *Counter {
	return &Counter{
		start:     time.Now(),
		Coords:    NewRpsCounter(),
		Nodes:     NewRpsCounter(),
		Ways:      NewRpsCounter(),
		Relations: NewRpsCounter(),
	}
}

func NewCounterWithEstimate(counts ElementCounts) *Counter {
	counter := NewCounter()
	counter.Coords.total = counts.Coords.Current
	counter.Nodes.total = counts.Nodes.Current
	counter.Ways.total = counts.Ways.Current
	counter.Relations.total = counts.Relations.Current
	return counter
}

type ElementCounts struct {
	Coords, Nodes, Ways, Relations ElementCount
}

// Duration returns the duration since start with seconds precission.
func (c *Counter) CurrentCount() *ElementCounts {
	return &ElementCounts{
		Coords:    c.Coords.Count(),
		Nodes:     c.Nodes.Count(),
		Ways:      c.Ways.Count(),
		Relations: c.Relations.Count(),
	}
}

// Duration returns the duration since start with seconds precission.
func (c *Counter) Duration() time.Duration {
	return time.Duration(int64(time.Since(c.start).Seconds()) * 1000 * 1000 * 1000)
}

type Statistics struct {
	counter *Counter
	done    chan bool
}

const (
	RESET = iota
	START
	STOP
	QUIT
)

func (s *Statistics) AddCoords(n int)    { s.counter.Coords.Add(n) }
func (s *Statistics) AddNodes(n int)     { s.counter.Nodes.Add(n) }
func (s *Statistics) AddWays(n int)      { s.counter.Ways.Add(n) }
func (s *Statistics) AddRelations(n int) { s.counter.Relations.Add(n) }
func (s *Statistics) Stop() *ElementCounts {
	s.done <- true
	return s.counter.CurrentCount()
}

func NewStatsReporter() *Statistics {
	s := Statistics{}
	s.counter = NewCounter()
	s.done = make(chan bool)

	go s.loop()
	return &s
}

func NewStatsReporterWithEstimate(counts *ElementCounts) *Statistics {
	s := Statistics{}
	if counts != nil {
		s.counter = NewCounterWithEstimate(*counts)
	} else {
		s.counter = NewCounter()
	}
	s.done = make(chan bool)

	go s.loop()
	return &s
}

func (s *Statistics) loop() {
	tick := time.Tick(500 * time.Millisecond)
	tock := time.Tick(time.Minute)
	for {
		select {
		case <-s.done:
			s.counter.PrintStats()
			return
		case <-tock:
			s.counter.PrintStats()
		case <-tick:
			s.counter.PrintTick()
			s.counter.Tick()
		}
	}
}

func fmtPercentOrVal(progress float64, value int64) string {
	if progress == -1.0 {
		return fmt.Sprintf("%d", value)
	}
	return fmt.Sprintf("%4.1f%%", progress*100)
}

func roundInt(val float64, round int) int64 {
	return int64(val/float64(round)) * int64(round)
}

func (c *Counter) PrintTick() {
	logging.Progress(
		fmt.Sprintf("[%6s] C: %7d/s %7d/s (%s) N: %7d/s %7d/s (%s) W: %7d/s %7d/s (%s) R: %6d/s %6d/s (%s)",
			c.Duration(),
			roundInt(c.Coords.Rps(), 1000),
			roundInt(c.Coords.LastRps(), 1000),
			fmtPercentOrVal(c.Coords.Progress(), c.Coords.Value()),
			roundInt(c.Nodes.Rps(), 100),
			roundInt(c.Nodes.LastRps(), 100),
			fmtPercentOrVal(c.Nodes.Progress(), c.Nodes.Value()),
			roundInt(c.Ways.Rps(), 100),
			roundInt(c.Ways.LastRps(), 100),
			fmtPercentOrVal(c.Ways.Progress(), c.Ways.Value()),
			roundInt(c.Relations.Rps(), 10),
			roundInt(c.Relations.LastRps(), 10),
			fmtPercentOrVal(c.Relations.Progress(), c.Relations.Value()),
		))
}

func (c *Counter) PrintStats() {
	logging.Infof("[%6s] C: %7d/s (%s) N: %7d/s (%s) W: %7d/s (%s) R: %6d/s (%s)",
		c.Duration(),
		roundInt(c.Coords.Rps(), 1000),
		fmtPercentOrVal(c.Coords.Progress(), c.Coords.Value()),
		roundInt(c.Nodes.Rps(), 100),
		fmtPercentOrVal(c.Nodes.Progress(), c.Nodes.Value()),
		roundInt(c.Ways.Rps(), 100),
		fmtPercentOrVal(c.Ways.Progress(), c.Ways.Value()),
		roundInt(c.Relations.Rps(), 10),
		fmtPercentOrVal(c.Relations.Progress(), c.Relations.Value()),
	)
}
