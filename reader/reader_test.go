package reader

import (
	"testing"
)

func TestReaderCpus(t *testing.T) {
	p, r, w, n, c := readersForCpus(1)
	if p != 1 && r != 1 && w != 1 && n != 1 && c != 1 {
		t.Fatal(p, r, w, n, c)
	}

	p, r, w, n, c = readersForCpus(2)
	if p != 2 && r != 1 && w != 1 && n != 1 && c != 1 {
		t.Fatal(p, r, w, n, c)
	}

	p, r, w, n, c = readersForCpus(4)
	if p != 3 && r != 1 && w != 1 && n != 1 && c != 1 {
		t.Fatal(p, r, w, n, c)
	}

	p, r, w, n, c = readersForCpus(8)
	if p != 6 && r != 2 && w != 2 && n != 2 && c != 2 {
		t.Fatal(p, r, w, n, c)
	}

	p, r, w, n, c = readersForCpus(12)
	if p != 8 && r != 3 && w != 3 && n != 3 && c != 3 {
		t.Fatal(p, r, w, n, c)
	}
}
