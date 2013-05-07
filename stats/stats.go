package stats

import (
	"fmt"
	"os"
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
	reset     chan bool
	messages  chan string
}

func (s *Statistics) AddCoords(n int)    { s.coords <- n }
func (s *Statistics) AddNodes(n int)     { s.nodes <- n }
func (s *Statistics) AddWays(n int)      { s.ways <- n }
func (s *Statistics) AddRelations(n int) { s.relations <- n }
func (s *Statistics) Reset()             { s.reset <- true }
func (s *Statistics) Message(msg string) { s.messages <- msg }

func StatsReporter() *Statistics {
	c := counter{}
	s := Statistics{}
	s.coords = make(chan int)
	s.nodes = make(chan int)
	s.ways = make(chan int)
	s.relations = make(chan int)
	s.reset = make(chan bool)
	s.messages = make(chan string)

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
			case <-s.reset:
				c = counter{}
			case msg := <-s.messages:
				c.Print()
				fmt.Println("\n", msg)
			case <-tick:
				c.Print()
			}
		}
	}()
	return &s
}

func (c *counter) Print() {
	dur := time.Since(c.lastReport)
	coordsPS := int32(float64(c.coords-c.lastCoords)/dur.Seconds()/1000) * 1000
	nodesPS := int32(float64(c.nodes-c.lastNodes)/dur.Seconds()/100) * 100
	waysPS := int32(float64(c.ways-c.lastWays)/dur.Seconds()/100) * 100
	relationsPS := int32(float64(c.relations-c.lastRelations)/dur.Seconds()/10) * 10

	fmt.Printf("Coords: %7d/s (%10d) Nodes: %7d/s (%9d) Ways: %7d/s (%8d) Relations: %6d/s (%7d)",
		coordsPS,
		c.coords,
		nodesPS,
		c.nodes,
		waysPS,
		c.ways,
		relationsPS,
		c.relations,
	)
	if val := os.Getenv("GOGCTRACE"); val != "" {
		fmt.Print("\n")
	} else {
		fmt.Print("\r\b")
	}
	c.lastCoords = c.coords
	c.lastNodes = c.nodes
	c.lastWays = c.ways
	c.lastRelations = c.relations
	c.lastReport = time.Now()
}
