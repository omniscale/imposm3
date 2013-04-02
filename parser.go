package main

import (
	"fmt"
	"goposm/parser"
	"os"
)

func main() {
	parser.PBFStats(os.Args[1])
	fmt.Println("done")
}
