package mapping

import (
	"testing"

	"github.com/omniscale/imposm3/element"
)

func BenchmarkTagMatch(b *testing.B) {
	m, err := FromFile("test_mapping.yml")
	if err != nil {
		b.Fatal(err)
	}
	matcher := m.PolygonMatcher
	for i := 0; i < b.N; i++ {
		e := element.Relation{}
		e.Tags = element.Tags{"landuse": "forest", "name": "Forest", "source": "bling", "tourism": "zoo"}
		if m := matcher.MatchRelation(&e); len(m) != 1 {
			b.Fatal(m)
		}
	}
}
