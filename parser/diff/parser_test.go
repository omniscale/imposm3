package diff

import (
	"io"
	"testing"
)

func TestParse(t *testing.T) {
	p, err := NewOscGzParser("612.osc.gz")
	if err != nil {
		t.Fatal(err)
	}

	p.SetWithMetadata(true)

	e, err := p.Next()
	if err != nil {
		t.Fatal(err)
	}

	if e.Add || !e.Mod || e.Del {
		t.Error("element not parsed as modify", e)
	}
	if e.Node == nil || e.Node.Id != 25594547 {
		t.Error("node not parsed correctly", e)
	}
	if md := e.Node.Metadata; md == nil || md.Version != 3 {
		t.Error("metadata not parsed", md)
	}
	for {
		_, err := p.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}

}
