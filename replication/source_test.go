package replication

import "testing"

func TestSeqPath(t *testing.T) {
	if path := seqPath(0); path != "000/000/000" {
		t.Fatal(path)
	}
	if path := seqPath(3069); path != "000/003/069" {
		t.Fatal(path)
	}
	if path := seqPath(123456789); path != "123/456/789" {
		t.Fatal(path)
	}
}
