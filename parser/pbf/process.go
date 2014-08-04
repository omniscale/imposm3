package pbf

import (
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/util"
	"runtime"
	"sync"
)

type parser struct {
	pbf       *Pbf
	coords    chan []element.Node
	nodes     chan []element.Node
	ways      chan []element.Way
	relations chan []element.Relation
	nParser   int
	wg        sync.WaitGroup
	waySync   *util.SyncPoint
	relSync   *util.SyncPoint
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

func (p *parser) Start() {
	blocks := p.pbf.BlockPositions()
	for i := 0; i < p.nParser; i++ {
		p.wg.Add(1)
		go func() {
			for block := range blocks {
				p.parseBlock(block)
			}
			if p.waySync != nil {
				p.waySync.Sync()
			}
			if p.relSync != nil {
				p.relSync.Sync()
			}
			p.wg.Done()
		}()
	}
}

func (p *parser) Close() {
	p.wg.Wait()
}

func (p *parser) NotifyWays(cb func()) {
	p.waySync = util.NewSyncPoint(p.nParser, cb)
}

func (p *parser) NotifyRelations(cb func()) {
	p.relSync = util.NewSyncPoint(p.nParser, cb)
}

func (p *parser) parseBlock(pos Block) {
	block := readPrimitiveBlock(pos)
	stringtable := newStringTable(block.GetStringtable())

	for _, group := range block.Primitivegroup {
		dense := group.GetDense()
		if dense != nil {
			parsedCoords, parsedNodes := readDenseNodes(dense, block, stringtable)
			if len(parsedCoords) > 0 {
				p.coords <- parsedCoords
			}
			if len(parsedNodes) > 0 {
				p.nodes <- parsedNodes
			}
		}
		parsedCoords, parsedNodes := readNodes(group.Nodes, block, stringtable)
		if len(parsedCoords) > 0 {
			p.coords <- parsedCoords
		}
		if len(parsedNodes) > 0 {
			p.nodes <- parsedNodes
		}
		parsedWays := readWays(group.Ways, block, stringtable)
		if len(parsedWays) > 0 {
			if p.waySync != nil {
				p.waySync.Sync()
			}
			p.ways <- parsedWays
		}
		parsedRelations := readRelations(group.Relations, block, stringtable)
		if len(parsedRelations) > 0 {
			if p.waySync != nil {
				p.waySync.Sync()
			}
			if p.relSync != nil {
				p.relSync.Sync()
			}
			p.relations <- parsedRelations
		}
	}

}
