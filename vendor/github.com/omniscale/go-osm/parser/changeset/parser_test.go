package changeset

import (
	"context"
	"os"
	"testing"

	osm "github.com/omniscale/go-osm"
)

func TestParse(t *testing.T) {
	conf := Config{
		Changesets: make(chan osm.Changeset),
	}
	f, err := os.Open("999.osm.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	p, err := NewGZIP(f, conf)
	if err != nil {
		t.Fatal(err)
	}
	go p.Parse(context.Background())

	changes := []osm.Changeset{}
	for ch := range conf.Changesets {
		changes = append(changes, ch)
	}

	if err := p.Error(); err != nil {
		t.Error(err)
	}

	if n := len(changes); n != 27 {
		t.Error("expected 27 changes, got", n)
	}
	c := changes[0]
	if c.ID != 43406602 || c.NumChanges != 314 {
		t.Error("unexpected changeset", c)
	}
	if n := len(c.Comments); n != 3 {
		t.Error("expected 3 comments in changeset", c)
	}

}
