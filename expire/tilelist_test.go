package expire

import (
	"testing"

	"github.com/omniscale/imposm3/element"
)

func TestTileList_ExpireNodes(t *testing.T) {
	tests := []struct {
		nodes    []element.Node
		expected int
		polygon  bool
	}{
		// point
		{[]element.Node{{Long: 8.30, Lat: 53.26}}, 1, false},

		// point + paddings
		{[]element.Node{{Long: 0, Lat: 0}}, 4, false},
		{[]element.Node{{Long: 0.01, Lat: 0}}, 2, false},
		{[]element.Node{{Long: 0, Lat: 0.01}}, 2, false},
		{[]element.Node{{Long: 0.01, Lat: 0.01}}, 1, false},

		// line
		{[]element.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.30},
		}, 5, false},
		// same line, but split into multiple segments
		{[]element.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.27},
			{Long: 8.30, Lat: 53.29},
			{Long: 8.30, Lat: 53.30},
		}, 5, false},

		// L-shape
		{[]element.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.30},
			{Long: 8.35, Lat: 53.30},
		}, 8, false},

		//  closed line (triangle)
		{[]element.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.30},
			{Long: 8.35, Lat: 53.30},
			{Long: 8.30, Lat: 53.25},
		}, 11, false},
		// same closed line but polygon (triangle), whole bbox (4x5 tiles) is expired
		{[]element.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.30},
			{Long: 8.35, Lat: 53.30},
			{Long: 8.30, Lat: 53.25},
		}, 20, true},

		// large triangle, only outline expired for polygons and lines
		{[]element.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.90},
			{Long: 8.85, Lat: 53.90},
			{Long: 8.30, Lat: 53.25},
		}, 124, true},
		{[]element.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.90},
			{Long: 8.85, Lat: 53.90},
			{Long: 8.30, Lat: 53.25},
		}, 124, false},
	}
	for _, test := range tests {
		tl := NewTileList(14, "")
		tl.ExpireNodes(test.nodes, test.polygon)
		if len(tl.tiles) != test.expected {
			t.Errorf("expected %d tiles, got %d", test.expected, len(tl.tiles))
			for tk, _ := range tl.tiles {
				t.Errorf("\t%v", tk)
			}
		}
	}
}
