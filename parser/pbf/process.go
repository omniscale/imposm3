package pbf

import (
	"runtime"
	"sync"

	"github.com/omniscale/imposm3/element"
)

type parser struct {
	pbf       *Pbf
	coords    chan []element.Node
	nodes     chan []element.Node
	ways      chan []element.Way
	relations chan []element.Relation
	nParser   int
	wg        sync.WaitGroup
	waySync   *barrier
	relSync   *barrier
}

func NewParser(pbf *Pbf, coords chan []element.Node, nodes chan []element.Node, ways chan []element.Way, relations chan []element.Relation) *parser {
	return &parser{
		pbf:       pbf,
		coords:    coords,
		nodes:     nodes,
		ways:      ways,
		relations: relations,
		nParser:   runtime.NumCPU(),
		wg:        sync.WaitGroup{},
	}
}

func (p *parser) Parse() {
	blocks := p.pbf.BlockPositions()
	for i := 0; i < p.nParser; i++ {
		p.wg.Add(1)
		go func() {
			for block := range blocks {
				p.parseBlock(block)
			}
			if p.waySync != nil {
				p.waySync.doneWait()
			}
			if p.relSync != nil {
				p.relSync.doneWait()
			}
			p.wg.Done()
		}()
	}
	p.wg.Wait()
}

func (p *parser) Wait() {
	p.wg.Wait()
}

func (p *parser) Close() {
	p.wg.Wait()
}

// FinishedCoords registers a single function that gets called when all
// nodes and coords are parsed. The callback should block until it is
// safe to continue with parsing of all ways.
// This only works when the PBF file is ordered by type (nodes before ways before relations).
func (p *parser) FinishedCoords(cb func()) {
	p.waySync = newBarrier(cb)
	p.waySync.add(p.nParser)
}

// FinishedWays registers a single function that gets called when all
// nodes and coords are parsed. The callback should block until it is
// safe to continue with parsing of all ways.
// This only works when the PBF file is ordered by type (nodes before ways before relations).
func (p *parser) FinishedWays(cb func()) {
	p.relSync = newBarrier(cb)
	p.relSync.add(p.nParser)
}

func (p *parser) parseBlock(pos block) {
	block := readPrimitiveBlock(pos)
	stringtable := newStringTable(block.GetStringtable())

	for _, group := range block.Primitivegroup {
		dense := group.GetDense()
		if dense != nil {
			parsedCoords, parsedNodes := readDenseNodes(dense, block, stringtable)
			if len(parsedCoords) > 0 && p.coords != nil {
				p.coords <- parsedCoords
			}
			if len(parsedNodes) > 0 && p.nodes != nil {
				p.nodes <- parsedNodes
			}
		}
		parsedCoords, parsedNodes := readNodes(group.Nodes, block, stringtable)
		if len(parsedCoords) > 0 && p.coords != nil {
			p.coords <- parsedCoords
		}
		if len(parsedNodes) > 0 && p.nodes != nil {
			p.nodes <- parsedNodes
		}
		parsedWays := readWays(group.Ways, block, stringtable)
		if len(parsedWays) > 0 && p.ways != nil {
			if p.waySync != nil {
				p.waySync.doneWait()
			}
			p.ways <- parsedWays
		}
		parsedRelations := readRelations(group.Relations, block, stringtable)
		if len(parsedRelations) > 0 && p.relations != nil {
			if p.waySync != nil {
				p.waySync.doneWait()
			}
			if p.relSync != nil {
				p.relSync.doneWait()
			}
			p.relations <- parsedRelations
		}
	}
}

// barrier is a struct to synchronize multiple goroutines.
// Works similar to a WaitGroup. Except:
// Calls callback function once all goroutines called doneWait().
// doneWait() blocks until the callback returns. doneWait() does not
// block after all goroutines were blocked once.
type barrier struct {
	synced     bool
	wg         sync.WaitGroup
	once       sync.Once
	callbackWg sync.WaitGroup
	callback   func()
}

func newBarrier(callback func()) *barrier {
	s := &barrier{callback: callback}
	s.callbackWg.Add(1)
	return s
}

func (s *barrier) add(delta int) {
	s.wg.Add(delta)
}

func (s *barrier) doneWait() {
	if s.synced {
		return
	}
	s.wg.Done()
	s.wg.Wait()
	s.once.Do(s.call)
	s.callbackWg.Wait()
}

func (s *barrier) call() {
	s.callback()
	s.synced = true
	s.callbackWg.Done()
}
