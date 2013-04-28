package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"goposm/element"
	"goposm/parser"
	"log"
	"runtime"
	"sort"
	"sync"
)

type Entry struct {
	Pos                 parser.BlockPosition
	NodeFirst, NodeLast int64
	WayFirst, WayLast   int64
	RelFirst, RelLast   int64
}

type NotFound struct {
	id int64
}

func (e *NotFound) Error() string {
	return "not found"
}

func (entry *Entry) readNode(id int64) (*element.Node, error) {
	block := parser.ReadPrimitiveBlock(entry.Pos)
	stringtable := parser.NewStringTable(block.GetStringtable())

	for _, group := range block.Primitivegroup {
		dense := group.GetDense()
		if dense != nil {
			parsedNodes := parser.ReadDenseNodes(dense, block, stringtable)
			if len(parsedNodes) > 0 {
				i := sort.Search(len(parsedNodes), func(i int) bool {
					return parsedNodes[i].Id >= id
				})
				if i < len(parsedNodes) && parsedNodes[i].Id == id {
					return &parsedNodes[i], nil
				}
			}
		}
		parsedNodes := parser.ReadNodes(group.Nodes, block, stringtable)
		if len(parsedNodes) > 0 {
			i := sort.Search(len(parsedNodes), func(i int) bool {
				return parsedNodes[i].Id >= id
			})
			if i < len(parsedNodes) && parsedNodes[i].Id == id {
				return &parsedNodes[i], nil
			}
		}
	}
	return nil, &NotFound{id}
}

func (entry *Entry) readWay(id int64) (*element.Way, error) {
	block := parser.ReadPrimitiveBlock(entry.Pos)
	stringtable := parser.NewStringTable(block.GetStringtable())

	for _, group := range block.Primitivegroup {
		parsedWays := parser.ReadWays(group.Ways, block, stringtable)
		if len(parsedWays) > 0 {
			i := sort.Search(len(parsedWays), func(i int) bool {
				return parsedWays[i].Id >= id
			})
			if i < len(parsedWays) && parsedWays[i].Id == id {
				return &parsedWays[i], nil
			}
		}
	}
	return nil, &NotFound{id}
}

func (entry *Entry) readRel(id int64) (*element.Relation, error) {
	block := parser.ReadPrimitiveBlock(entry.Pos)
	stringtable := parser.NewStringTable(block.GetStringtable())

	for _, group := range block.Primitivegroup {
		parsedRels := parser.ReadRelations(group.Relations, block, stringtable)
		if len(parsedRels) > 0 {
			i := sort.Search(len(parsedRels), func(i int) bool {
				return parsedRels[i].Id >= id
			})
			if i < len(parsedRels) && parsedRels[i].Id == id {
				return &parsedRels[i], nil
			}
		}
	}
	return nil, &NotFound{id}
}

func CreateEntry(pos parser.BlockPosition) Entry {
	block := parser.ReadPrimitiveBlock(pos)

	entry := Entry{pos, -1, -1, -1, -1, -1, -1}

	for _, group := range block.Primitivegroup {
		if entry.NodeFirst == -1 {
			dense := group.GetDense()
			if dense != nil && len(dense.Id) > 0 {
				entry.NodeFirst = dense.Id[0]
			}
			if len(group.Nodes) > 0 {
				entry.NodeFirst = *group.Nodes[0].Id
			}
		}
		dense := group.GetDense()
		if dense != nil && len(dense.Id) > 0 {
			var id int64
			for _, idDelta := range dense.Id {
				id += idDelta
			}
			entry.NodeLast = id
		}
		if len(group.Nodes) > 0 {
			entry.NodeLast = *group.Nodes[len(group.Nodes)-1].Id
		}
		if entry.WayFirst == -1 {
			if len(group.Ways) > 0 {
				entry.WayFirst = *group.Ways[0].Id
			}
		}
		if len(group.Ways) > 0 {
			entry.WayLast = *group.Ways[len(group.Ways)-1].Id
		}
		if entry.RelFirst == -1 {
			if len(group.Relations) > 0 {
				entry.RelFirst = *group.Relations[0].Id
			}
		}
		if len(group.Relations) > 0 {
			entry.RelLast = *group.Relations[len(group.Relations)-1].Id
		}
	}
	return entry
}

type IndexCache struct {
	filename   string
	db         *sql.DB
	insertStmt *sql.Stmt
}

func NewIndex(filename string) *IndexCache {
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		log.Fatal(err)
	}
	insertStmt, err := db.Prepare(`
		insert into indices (
			node_first, node_last,
			way_first, way_last,
			rel_first, rel_last,
			offset, size
		)
		values (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Fatal(err)
	}

	return &IndexCache{filename, db, insertStmt}
}

func (index *IndexCache) clear() {
	stmts := []string{
		"drop table if exists indices",
		`create table indices (
			id integer not null primary key,
			node_first integer,
			node_last integer,
			way_first integer,
			way_last integer,
			rel_first integer,
			rel_last integer,
			offset integer,
			size integer
		)`,
		"create index indices_node_idx on indices (node_first)",
		"create index indices_way_idx on indices (way_first)",
		"create index indices_rel_idx on indices (rel_first)",
	}
	for _, stmt := range stmts {
		_, err := index.db.Exec(stmt)
		if err != nil {
			log.Fatalf("%q: %s\n", err, stmt)
		}
	}
}

func (index *IndexCache) queryNode(id int64) (Entry, error) {
	entry := Entry{}
	stmt, err := index.db.Prepare(
		`select node_first, node_last, offset, size 
		from indices 
		where node_first <= ? and node_last >= ?`)
	if err != nil {
		return entry, err
	}
	defer stmt.Close()

	row := stmt.QueryRow(id, id)

	err = row.Scan(&entry.NodeFirst, &entry.NodeLast, &entry.Pos.Offset, &entry.Pos.Size)
	if err != nil {
		return entry, err
	}
	return entry, nil
}

func (index *IndexCache) queryWay(id int64) (Entry, error) {
	entry := Entry{}
	stmt, err := index.db.Prepare(
		`select way_first, way_last, offset, size 
		from indices 
		where way_first <= ? and way_last >= ?`)
	if err != nil {
		return entry, err
	}
	defer stmt.Close()

	row := stmt.QueryRow(id, id)

	err = row.Scan(&entry.WayFirst, &entry.WayLast, &entry.Pos.Offset, &entry.Pos.Size)
	if err != nil {
		return entry, err
	}
	return entry, nil
}
func (index *IndexCache) queryRel(id int64) (Entry, error) {
	entry := Entry{}
	stmt, err := index.db.Prepare(
		`select rel_first, rel_last, offset, size 
		from indices 
		where rel_first <= ? and rel_last >= ?`)
	if err != nil {
		return entry, err
	}
	defer stmt.Close()

	row := stmt.QueryRow(id, id)

	err = row.Scan(&entry.RelFirst, &entry.RelLast, &entry.Pos.Offset, &entry.Pos.Size)
	if err != nil {
		return entry, err
	}
	return entry, nil
}

func (index *IndexCache) addEntry(entry Entry) {
	_, err := index.insertStmt.Exec(
		entry.NodeFirst, entry.NodeLast,
		entry.WayFirst, entry.WayLast,
		entry.RelFirst, entry.RelLast,
		entry.Pos.Offset, entry.Pos.Size)
	if err != nil {
		log.Fatal(err)
	}
}
func (index *IndexCache) close() {
	index.insertStmt.Close()
	index.db.Close()
}

var createIndex bool
var queryNode, queryWay, queryRel int64

func init() {
	flag.BoolVar(&createIndex, "create-index", false, "create a new index")
	flag.Int64Var(&queryNode, "node", -1, "query node")
	flag.Int64Var(&queryWay, "way", -1, "query way")
	flag.Int64Var(&queryRel, "rel", -1, "query relation")
}

func FillIndex(index *IndexCache, pbfFilename string) {
	indices := make(chan Entry)

	positions := parser.PBFBlockPositions(pbfFilename)

	waitParser := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		waitParser.Add(1)
		go func() {
			for pos := range positions {
				indices <- CreateEntry(pos)
			}
			waitParser.Done()
		}()
	}
	go func() {
		for entry := range indices {
			index.addEntry(entry)
			fmt.Printf("%+v\n", entry)
		}
	}()
	waitParser.Wait()
	close(indices)
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	index := NewIndex("/tmp/index.sqlite")
	defer index.close()

	if createIndex {
		FillIndex(index, flag.Arg(0))
	}

	if queryNode != -1 {
		entry, err := index.queryNode(queryNode)
		if err != nil {
			fmt.Println(err)
			return
		}
		entry.Pos.Filename = flag.Arg(0)
		node, err := entry.readNode(queryNode)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println("queryNode:", node)
	} else if queryWay != -1 {
		entry, err := index.queryWay(queryWay)
		if err != nil {
			fmt.Println(err)
			return
		}
		entry.Pos.Filename = flag.Arg(0)
		way, err := entry.readWay(queryWay)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println("queryWay:", way)
	} else if queryRel != -1 {
		entry, err := index.queryRel(queryRel)
		if err != nil {
			fmt.Println(err)
			return
		}
		entry.Pos.Filename = flag.Arg(0)
		rel, err := entry.readRel(queryRel)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println("queryRel:", rel)
	}

}
