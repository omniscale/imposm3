package mapping

import (
	"testing"

	osm "github.com/omniscale/go-osm"
)

func BenchmarkTagMatch(b *testing.B) {
	m, err := FromFile("test_mapping.yml")
	if err != nil {
		b.Fatal(err)
	}
	matcher := m.PolygonMatcher
	for i := 0; i < b.N; i++ {
		e := osm.Relation{}
		e.Tags = osm.Tags{"landuse": "forest", "name": "Forest", "source": "bling", "tourism": "zoo"}
		if m := matcher.MatchRelation(&e); len(m) != 1 {
			b.Fatal(m)
		}
	}
}
