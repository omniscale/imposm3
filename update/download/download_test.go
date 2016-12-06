package download

import "testing"

func TestDiffPath(t *testing.T) {
	if path := diffPath(0); path != "000/000/000" {
		t.Fatal(path)
	}
	if path := diffPath(3069); path != "000/003/069" {
		t.Fatal(path)
	}
	if path := diffPath(123456789); path != "123/456/789" {
		t.Fatal(path)
	}
}
