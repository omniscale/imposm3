package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"goposm/parser"
	"log"
	"os"
	"runtime"
	"sync"
)

type Entry struct {
	Pos                 parser.BlockPosition
	NodeFirst, NodeLast int64
	WayFirst, WayLast   int64
	RelFirst, RelLast   int64
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

func initIndex(filename string) *IndexCache {
	os.Remove(filename)

	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		log.Fatal(err)
	}
	stmts := []string{
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
		_, err = db.Exec(stmt)
		if err != nil {
			log.Fatalf("%q: %s\n", err, stmt)
		}
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

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	index := initIndex("/tmp/index.sqlite")
	defer index.close()

	indices := make(chan Entry)

	positions := parser.PBFBlockPositions(flag.Arg(0))

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
