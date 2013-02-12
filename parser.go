package main

import (
	"fmt"
	"goposm/parser"
	"os"
)

func main() {
	parser.BlockPositions(os.Args[1])
	fmt.Println("done")
}
