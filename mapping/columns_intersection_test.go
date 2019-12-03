package mapping

import (
	"math/rand"
	"testing"

	osm "github.com/omniscale/go-osm"
	geomp "github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/mapping/config"
	"github.com/omniscale/imposm3/proj"
)

func TestIntersectsFeatureField(t *testing.T) {
	makeValue, err := MakeIntersectsFeatureField("",
		AvailableColumnTypes["intersection"],
		config.Column{
			Name: "country",
			Key:  "",
			Type: "intersection",
			Args: map[string]interface{}{"geojson": "be_nl_bounds.geojson", "property": "FIPS_CNTRY"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	match := Match{}
	elem := osm.Element{}
	geom := geomp.Geometry{nil, nil}
	g := geos.NewGeos()

	geom.Geom = g.Point(proj.WgsToMerc(6.76976, 52.60763)) // Germany
	if value := makeValue("", &elem, &geom, match); value != nil {
		t.Error("expected nil, got", value)
	}
	geom.Geom = g.Point(proj.WgsToMerc(5.40129, 52.69766)) // IJsselmeer, not in bounds
	if value := makeValue("", &elem, &geom, match); value != nil {
		t.Error("got", value)
	}
	geom.Geom = g.Point(proj.WgsToMerc(4.8542, 52.5726))
	if value := makeValue("", &elem, &geom, match); value != "NL" {
		t.Error("got", value)
	}

	geom.Geom = g.Point(proj.WgsToMerc(5.04529, 51.40216))
	if value := makeValue("", &elem, &geom, match); value != "BE" {
		t.Error("got", value)
	}
}

func TestIntersectsField(t *testing.T) {
	makeValue, err := MakeIntersectsField("",
		AvailableColumnTypes["intersection"],
		config.Column{
			Name: "country",
			Key:  "",
			Type: "intersection",
			Args: map[string]interface{}{"geojson": "be_nl_bounds.geojson"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	match := Match{}
	elem := osm.Element{}
	geom := geomp.Geometry{nil, nil}
	g := geos.NewGeos()

	geom.Geom = g.Point(proj.WgsToMerc(6.76976, 52.60763)) // Germany
	if value := makeValue("", &elem, &geom, match); value != false {
		t.Error("expected false, got", value)
	}
	geom.Geom = g.Point(proj.WgsToMerc(5.40129, 52.69766)) // IJsselmeer, not in bounds
	if value := makeValue("", &elem, &geom, match); value != false {
		t.Error("got", value)
	}
	geom.Geom = g.Point(proj.WgsToMerc(4.8542, 52.5726))
	if value := makeValue("", &elem, &geom, match); value != true {
		t.Error("got", value)
	}

	geom.Geom = g.Point(proj.WgsToMerc(5.04529, 51.40216))
	if value := makeValue("", &elem, &geom, match); value != true {
		t.Error("got", value)
	}
}

func BenchmarkIntersectsFeatureField(b *testing.B) {
	makeValue, err := MakeIntersectsFeatureField("",
		AvailableColumnTypes["intersection"],
		config.Column{
			Name: "country",
			Key:  "",
			Type: "intersection",
			Args: map[string]interface{}{"geojson": "be_nl_bounds.geojson", "property": "FIPS_CNTRY"},
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	match := Match{}
	elem := osm.Element{}
	g := geos.NewGeos()
	hits := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 2,49 : 9,54
		p := g.Point(proj.WgsToMerc(rand.Float64()*7+2, rand.Float64()*5+49))
		geom := geomp.Geometry{p, nil}
		if value := makeValue("", &elem, &geom, match); value == "BE" || value == "NL" {
			hits += 1
		}
	}
	if b.N > 100 && hits < 1 {
		b.Error("expected more hits than", hits)
	}
}

func BenchmarkIntersectsField(b *testing.B) {
	makeValue, err := MakeIntersectsField("",
		AvailableColumnTypes["intersection"],
		config.Column{
			Name: "country",
			Key:  "",
			Type: "intersection",
			Args: map[string]interface{}{"geojson": "be_nl_bounds.geojson"},
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	match := Match{}
	elem := osm.Element{}
	g := geos.NewGeos()
	hits := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 2,49 : 9,54
		p := g.Point(proj.WgsToMerc(rand.Float64()*7+2, rand.Float64()*5+49))
		geom := geomp.Geometry{p, nil}
		if value := makeValue("", &elem, &geom, match); value == true {
			hits += 1
		}
	}
	if b.N > 100 && hits < 1 {
		b.Error("expected more hits than", hits)
	}
}
