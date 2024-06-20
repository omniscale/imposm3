package expire

import (
	"bufio"
	"bytes"
	"regexp"
	"testing"

	osm "github.com/omniscale/go-osm"
)

func TestTileList_ExpireNodesAdaptive(t *testing.T) {
	for _, test := range []struct {
		nodes         []osm.Node
		expectedNum   int
		expectedLevel int
		closed        bool
	}{
		// point
		{[]osm.Node{{Long: 8.30, Lat: 53.26}}, 1, 14, false},

		// point + paddings
		{[]osm.Node{{Long: 0, Lat: 0}}, 4, 14, false},
		{[]osm.Node{{Long: 0.01, Lat: 0}}, 2, 14, false},
		{[]osm.Node{{Long: 0, Lat: 0.01}}, 2, 14, false},
		{[]osm.Node{{Long: 0.01, Lat: 0.01}}, 1, 14, false},

		// line
		{[]osm.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.30},
		}, 5, 14, false},
		// same line, but split into multiple segments
		{[]osm.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.27},
			{Long: 8.30, Lat: 53.29},
			{Long: 8.30, Lat: 53.30},
		}, 5, 14, false},

		// L-shape
		{[]osm.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.30},
			{Long: 8.35, Lat: 53.30},
		}, 8, 14, false},

		//  line (triangle)
		{[]osm.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.30},
			{Long: 8.35, Lat: 53.30},
			{Long: 8.30, Lat: 53.25},
		}, 11, 14, false},
		// same line but closed/polygon (triangle), whole bbox (4x5 tiles) is expired
		{[]osm.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.30},
			{Long: 8.35, Lat: 53.30},
			{Long: 8.30, Lat: 53.25},
		}, 20, 14, true},

		// large triangle, moved zoom level up
		{[]osm.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.90},
			{Long: 8.85, Lat: 53.90},
			{Long: 8.30, Lat: 53.25},
		}, 28, 11, true},
		// same large triangle but as line, moved just one zoom level up to be
		// able to follow the outline more precise
		{[]osm.Node{
			{Long: 8.30, Lat: 53.25},
			{Long: 8.30, Lat: 53.90},
			{Long: 8.85, Lat: 53.90},
			{Long: 8.30, Lat: 53.25},
		}, 63, 13, false},
		// long line, accross world
		{[]osm.Node{
			{Long: -170, Lat: -80},
			{Long: 170, Lat: 80},
		}, 17, 4, false},
		// large polygon, accross world
		{[]osm.Node{
			{Long: -160, Lat: -70},
			{Long: 160, Lat: -70},
			{Long: 160, Lat: 70},
			{Long: -160, Lat: 70},
		}, 48, 3, true},
	} {
		t.Run("", func(t *testing.T) {
			tl := NewTileList(14, "")
			tl.ExpireNodes(test.nodes, test.closed)
			for z := 0; z <= tl.maxZoom; z++ {
				expected := 0
				if z == test.expectedLevel {
					expected = test.expectedNum
				}
				if len(tl.tiles[z]) != expected {
					t.Errorf("expected %d tiles, got %d in z=%d", expected, len(tl.tiles[z]), z)
					for tk := range tl.tiles[z] {
						t.Errorf("\t%v", tk)
					}
				}
			}
			buf := bytes.Buffer{}
			if err := tl.writeTiles(&buf); err != nil {
				t.Errorf("error writing tiles list: %s", err)
			}

			tileRe := regexp.MustCompile(`^\d+/\d+/\d+$`)
			scanner := bufio.NewScanner(&buf)

			lines := 0

			for scanner.Scan() {
				line := scanner.Text()
				lines++

				if !tileRe.MatchString(line) {
					t.Errorf("line %d does is not a tile coordinate: %s", lines, line)
				}
			}

			if err := scanner.Err(); err != nil {
				t.Fatalf("rrror reading buffer: %v", err)
			}

			if lines != test.expectedNum {
				t.Errorf("expected %d lines, but got %d", test.expectedNum, lines)
			}
		})
	}
}
