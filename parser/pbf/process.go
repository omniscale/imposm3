package pbf

import (
	"imposm3/element"
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
}

func NewParser(pbf *Pbf, coords chan []element.Node, nodes chan []element.Node, ways chan []element.Way, relations chan []element.Relation) *parser {
	return &parser{pbf, coords, nodes, ways, relations, runtime.NumCPU(), sync.WaitGroup{}}
}

func (p *parser) Start() {
	blocks := p.pbf.BlockPositions()
	for i := 0; i < p.nParser; i++ {
		p.wg.Add(1)
		go func() {
			for block := range blocks {
				p.parseBlock(block)
			}
			p.wg.Done()
		}()
	}
}

func (p *parser) Close() {
	p.wg.Wait()
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
			p.ways <- parsedWays
		}
		parsedRelations := readRelations(group.Relations, block, stringtable)
		if len(parsedRelations) > 0 {
			p.relations <- parsedRelations
		}
	}

}
