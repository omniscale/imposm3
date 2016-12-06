package changeset

import "testing"

func TestParse(t *testing.T) {
	changes, err := ParseAllOsmGz("999.osm.gz")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(changes); n != 27 {
		t.Error("expected 27 changes, got", n)
	}
	c := changes[0]
	if c.Id != 43406602 || c.NumChanges != 314 {
		t.Error("unexpected changeset", c)
	}
	if n := len(c.Comments); n != 3 {
		t.Error("expected 3 comments in changeset", c)
	}

}
