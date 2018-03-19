package pbf

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/omniscale/imposm3/element"
)

type Parser struct {
	pbf       *pbf
	coords    chan []element.Node
	nodes     chan []element.Node
	ways      chan []element.Way
	relations chan []element.Relation
	nParser   int
	wg        sync.WaitGroup
	waySync   *barrier
	relSync   *barrier
}

func NewParser(
	filename string,
) (*Parser, error) {
	pbf, err := open(filename)
	if err != nil {
		return nil, err
	}
	return &Parser{
		pbf:     pbf,
		nParser: runtime.NumCPU(),
		wg:      sync.WaitGroup{},
	}, nil
}

func (p *Parser) Header() Header {
	return *p.pbf.header
}

func (p *Parser) Parse(
	coords chan []element.Node,
	nodes chan []element.Node,
	ways chan []element.Way,
	relations chan []element.Relation,
) {
	p.coords = coords
	p.nodes = nodes
	p.ways = ways
	p.relations = relations
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

// RegisterFirstWayCallback registers a callback that gets called when the
// the first way is parsed. The callback should block until it is
// safe to send ways to the way channel.
// This only works when the PBF file is ordered by type (nodes before ways before relations).
func (p *Parser) RegisterFirstWayCallback(cb func()) {
	p.waySync = newBarrier(cb)
	p.waySync.add(p.nParser)
}

// RegisterFirstRelationCallback registers a callback that gets called when the
// the first relation is parsed. The callback should block until it is
// safe to send relations to the relation channel.
// This only works when the PBF file is ordered by type (nodes before ways before relations).
func (p *Parser) RegisterFirstRelationCallback(cb func()) {
	p.relSync = newBarrier(cb)
	p.relSync.add(p.nParser)
}

func (p *Parser) parseBlock(pos block) {
	block := readPrimitiveBlock(pos)
	stringtable := newStringTable(block.GetStringtable())

	for _, group := range block.Primitivegroup {
		if p.coords != nil || p.nodes != nil {
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
			if len(group.Nodes) > 0 {
				parsedCoords, parsedNodes := readNodes(group.Nodes, block, stringtable)
				if len(parsedCoords) > 0 && p.coords != nil {
					p.coords <- parsedCoords
				}
				if len(parsedNodes) > 0 && p.nodes != nil {
					p.nodes <- parsedNodes
				}
			}
		}
		if len(group.Ways) > 0 && p.ways != nil {
			parsedWays := readWays(group.Ways, block, stringtable)
			if len(parsedWays) > 0 {
				if p.waySync != nil {
					p.waySync.doneWait()
				}
				p.ways <- parsedWays
			}
		}
		if len(group.Relations) > 0 && p.relations != nil {
			parsedRelations := readRelations(group.Relations, block, stringtable)
			if len(parsedRelations) > 0 {
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
}

// barrier is a struct to synchronize multiple goroutines.
// Works similar to a WaitGroup. Except:
// Calls callback function once all goroutines called doneWait().
// doneWait() blocks until the callback returns. doneWait() does not
// block after all goroutines were blocked once.
type barrier struct {
	synced     int32
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
	if atomic.LoadInt32(&s.synced) == 1 {
		return
	}
	s.wg.Done()
	s.wg.Wait()
	s.once.Do(s.call)
	s.callbackWg.Wait()
}

func (s *barrier) call() {
	s.callback()
	atomic.StoreInt32(&s.synced, 1)
	s.callbackWg.Done()
}
