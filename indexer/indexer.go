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
	Pos    parser.BlockPosition
	NodeId int64
	WayId  int64
	RelId  int64
}

func CreateEntry(pos parser.BlockPosition) Entry {
	block := parser.ReadPrimitiveBlock(pos)

	entry := Entry{pos, -1, -1, -1}

	for _, group := range block.Primitivegroup {
		if entry.NodeId == -1 {
			dense := group.GetDense()
			if dense != nil && len(dense.Id) > 0 {
				entry.NodeId = dense.Id[0]
			}
			if len(group.Nodes) > 0 {
				entry.NodeId = *group.Nodes[0].Id
			}
		}
		if entry.WayId == -1 {
			if len(group.Ways) > 0 {
				entry.WayId = *group.Ways[0].Id
			}
		}
		if entry.RelId == -1 {
			if len(group.Relations) > 0 {
				entry.RelId = *group.Relations[0].Id
			}
		}
		if entry.NodeId == -1 && entry.WayId == -1 && entry.RelId == -1 {
			break
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
		"create table indices (id integer not null primary key, node integer, way integer, rel integer, offset integer, size integer)",
		"create index indices_node_idx on indices (node)",
		"create index indices_way_idx on indices (way)",
		"create index indices_rel_idx on indices (rel)",
	}
	for _, stmt := range stmts {
		_, err = db.Exec(stmt)
		if err != nil {
			log.Fatal("%q: %s\n", err, stmt)
		}
	}

	insertStmt, err := db.Prepare("insert into indices (node, way, rel, offset, size) values (?, ?, ?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}

	return &IndexCache{filename, db, insertStmt}
}

func (index *IndexCache) addEntry(entry Entry) {
	_, err := index.insertStmt.Exec(entry.NodeId, entry.WayId, entry.RelId, entry.Pos.Offset, entry.Pos.Size)
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
			//index.addEntry(entry)
			fmt.Printf("%+v\n", entry)
		}
	}()
	waitParser.Wait()
	close(indices)
}
