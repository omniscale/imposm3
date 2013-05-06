package main

import (
	"database/sql"
	_ "github.com/bmizerany/pq"
	"goposm/element"
	"goposm/geom"
	"log"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	db, err := sql.Open("postgres", "user=olt host=/var/run/postgresql dbname=osm sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("DROP TABLE IF EXISTS test")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test (val VARCHAR);")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Query("SELECT AddGeometryColumn('test', 'geom', 3857, 'LINESTRING', 2);")
	if err != nil {
		log.Fatal(err)
	}

	size := 16
	nodes := make([]element.Node, size)
	for i := 0; i < size; i++ {
		nodes[i] = element.Node{Lat: 0, Long: float64(i)}
	}
	wkb := geom.LineString(nodes)

	stmt, err := db.Prepare("INSERT INTO test (val, geom) VALUES ($1, ST_SetSRID(ST_GeomFromWKB($2), 3857));")
	if err != nil {
		log.Fatal(err)
	}

	_, err = stmt.Exec("test", wkb)
	if err != nil {
		log.Fatal(err)
	}

}
