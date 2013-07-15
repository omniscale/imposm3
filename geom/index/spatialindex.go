package main

import (
	"flag"
	"fmt"
	"goposm/geom/clipper"
	"goposm/geom/geos"
	"log"
)

func main() {
	flag.Parse()

	clipper, err := clipper.NewFromOgrSource(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}

	g := geos.NewGeos()
	defer g.Finish()

	line := g.FromWkt("LINESTRING(1106543 7082055, 1107105.2 7087540.0)")

	result, err := clipper.Clip(line)
	fmt.Println(result, err)
}
