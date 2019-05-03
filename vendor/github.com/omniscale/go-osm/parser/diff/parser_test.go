package diff

import (
	"context"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/kr/pretty"
	"github.com/omniscale/go-osm"
)

func TestParse(t *testing.T) {
	conf := Config{
		Diffs:           make(chan osm.Diff),
		IncludeMetadata: true,
	}
	f, err := os.Open("612.osc.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	p, err := NewGZIP(f, conf)
	if err != nil {
		t.Fatal(err)
	}

	diffs := []osm.Diff{}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for d := range conf.Diffs {
			diffs = append(diffs, d)
		}
		wg.Done()
	}()
	err = p.Parse(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	for _, tc := range []struct {
		Idx  int
		Name string
		Want osm.Diff
	}{
		{
			Name: "modified node",
			Idx:  0,
			Want: osm.Diff{
				Create: false,
				Modify: true,
				Delete: false,
				Node: &osm.Node{
					Element: osm.Element{
						ID:       25594547,
						Tags:     osm.Tags{"source": "SRTM"},
						Metadata: &osm.Metadata{UserID: 462835, UserName: "G-eMapper", Version: 3, Timestamp: time.Date(2016, 12, 2, 14, 15, 11, 0, time.UTC), Changeset: 44115151},
					},
					Lat:  16.187913,
					Long: 122.0913159,
				},
			},
		},
		{
			Name: "deleted node",
			Idx:  47,
			Want: osm.Diff{
				Create: false,
				Modify: false,
				Delete: true,
				Node: &osm.Node{
					Element: osm.Element{
						ID:       1884933281,
						Metadata: &osm.Metadata{UserID: 3315483, UserName: "8dirfriend", Version: 2, Timestamp: time.Date(2016, 12, 2, 14, 15, 10, 0, time.UTC), Changeset: 44115150},
					},
					Lat:  35.0233546,
					Long: 132.879755,
				},
			},
		},
		{
			Idx:  1753,
			Name: "added node",
			Want: osm.Diff{
				Create: true,
				Modify: false,
				Delete: false,
				Node: &osm.Node{
					Element: osm.Element{
						ID:       4533952893,
						Tags:     osm.Tags{"amenity": "hospital", "name": "Кожно-венерологический диспансер", "name:ru": "Кожно-венерологический диспансер"},
						Metadata: &osm.Metadata{UserID: 4112953, UserName: "Sergei97", Version: 1, Timestamp: time.Date(2016, 12, 2, 14, 15, 19, 0, time.UTC), Changeset: 44115157},
					},
					Lat:  52.563681,
					Long: 24.4658314,
				},
			},
		},
		{
			Idx:  2267,
			Name: "modified way",
			Want: osm.Diff{
				Create: false,
				Modify: true,
				Delete: false,
				Way: &osm.Way{
					Element: osm.Element{
						ID:       6863685,
						Tags:     osm.Tags{"highway": "unclassified", "maxspeed": "30", "name": "Oranjestraat", "oneway": "yes", "cycleway": "opposite"},
						Metadata: &osm.Metadata{UserID: 619707, UserName: "openMvD", Version: 6, Timestamp: time.Date(2016, 12, 2, 14, 15, 6, 0, time.UTC), Changeset: 44115110},
					},
					Refs:  []int64{44776397, 44776575, 4534010578, 44776865, 4534010576, 44780387},
					Nodes: nil,
				}},
		},
		{
			Idx:  2563,
			Name: "modified relation",
			Want: osm.Diff{
				Create: false,
				Modify: true,
				Delete: false,
				Rel: &osm.Relation{
					Element: osm.Element{
						ID:       2139646,
						Tags:     osm.Tags{"destination": "Balonne River", "name": "Condamine River", "type": "waterway", "waterway": "river", "wikidata": "Q805500", "wikipedia": "en:Condamine River"},
						Metadata: &osm.Metadata{UserID: 1185091, UserName: "nick0252", Version: 13, Timestamp: time.Date(2016, 12, 2, 14, 15, 32, 0, time.UTC), Changeset: 44115162},
					},
					Members: []osm.Member{
						{ID: 142320051, Type: 1, Role: "main_stream"},
						{ID: 162045587, Type: 1, Role: "main_stream"},
						{ID: 162045590, Type: 1, Role: "main_stream"},
						{ID: 162047493, Type: 1, Role: "main_stream"},
						{ID: 199021540, Type: 1, Role: "main_stream"},
						{ID: 199021536, Type: 1, Role: "main_stream"},
						{ID: 162077162, Type: 1, Role: "main_stream"},
						{ID: 165967517, Type: 1, Role: "main_stream"},
						{ID: 165967518, Type: 1, Role: "main_stream"},
						{ID: 165967519, Type: 1, Role: "main_stream"},
						{ID: 165967516, Type: 1, Role: "main_stream"},
						{ID: 165967520, Type: 1, Role: "main_stream"},
						{ID: 41454859, Type: 1, Role: "main_stream"},
						{ID: 166808071, Type: 1, Role: "main_stream"},
						{ID: 166814228, Type: 1, Role: "main_stream"},
						{ID: 149724655, Type: 1, Role: "main_stream"},
						{ID: 165368857, Type: 1, Role: "main_stream"},
						{ID: 162077161, Type: 1, Role: "side_stream"},
						{ID: 162077160, Type: 1, Role: "side_stream"},
						{ID: 457226217, Type: 1, Role: "outer"},
						{ID: 457226545, Type: 1, Role: "inner"},
					},
				},
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			if !reflect.DeepEqual(diffs[tc.Idx], tc.Want) {
				pretty.Println(diffs[tc.Idx])
				t.Errorf("unexpected diff, got:\n%#v\nwant:\n%#v", diffs[tc.Idx], tc.Want)
			}
		})
	}
}
