package stats

import (
	"fmt"
	"time"
)

type counter struct {
	coords        int64
	nodes         int64
	ways          int64
	relations     int64
	lastReport    time.Time
	lastCoords    int64
	lastNodes     int64
	lastWays      int64
	lastRelations int64
}

type Statistics struct {
	coords    chan int
	nodes     chan int
	ways      chan int
	relations chan int
}

func (s *Statistics) AddCoords(n int)    { s.coords <- n }
func (s *Statistics) AddNodes(n int)     { s.nodes <- n }
func (s *Statistics) AddWays(n int)      { s.ways <- n }
func (s *Statistics) AddRelations(n int) { s.relations <- n }

func StatsReporter() *Statistics {
	c := counter{}
	s := Statistics{}
	s.coords = make(chan int)
	s.nodes = make(chan int)
	s.ways = make(chan int)
	s.relations = make(chan int)

	go func() {
		tick := time.Tick(time.Second)
		for {
			select {
			case n := <-s.coords:
				c.coords += int64(n)
			case n := <-s.nodes:
				c.nodes += int64(n)
			case n := <-s.ways:
				c.ways += int64(n)
			case n := <-s.relations:
				c.relations += int64(n)
			case <-tick:
				c.Print()
			}
		}
	}()
	return &s
}

func (c *counter) Print() {
	dur := time.Since(c.lastReport)
	coordsPS := float64(c.coords-c.lastCoords) / dur.Seconds()
	nodesPS := float64(c.nodes-c.lastNodes) / dur.Seconds()
	waysPS := float64(c.ways-c.lastWays) / dur.Seconds()
	relationsPS := float64(c.relations-c.lastRelations) / dur.Seconds()

	fmt.Printf("Coords: %8.1f (%10d) Nodes: %6.1f (%9d) Ways: %6.1f (%8d) Relations: %6.1f (%7d)\n",
		coordsPS,
		c.coords,
		nodesPS,
		c.nodes,
		waysPS,
		c.ways,
		relationsPS,
		c.relations,
	)
	c.lastCoords = c.coords
	c.lastNodes = c.nodes
	c.lastWays = c.ways
	c.lastRelations = c.relations
	c.lastReport = time.Now()
}
