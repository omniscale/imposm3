package cache

import (
	"testing"
)

func TestInsertId(t *testing.T) {

	refs := Refs{}
	refs.Ids = make([]int64, 0, 1)

	refs.insertId(1)
	if refs.Ids[0] != 1 {
		t.Fatal(refs)
	}

	refs.insertId(10)
	if refs.Ids[0] != 1 && refs.Ids[1] != 10 {
		t.Fatal(refs)
	}

	// insert twice
	refs.insertId(10)
	if refs.Ids[0] != 1 && refs.Ids[1] != 10 {
		t.Fatal(refs)
	}

	// insert before
	refs.insertId(0)
	if refs.Ids[0] != 0 && refs.Ids[1] != 1 && refs.Ids[2] != 10 {
		t.Fatal(refs)
	}

	// insert after
	refs.insertId(12)
	if refs.Ids[0] != 0 && refs.Ids[1] != 1 && refs.Ids[2] != 10 && refs.Ids[3] != 12 {
		t.Fatal(refs)
	}

	// insert between
	refs.insertId(11)
	if refs.Ids[0] != 0 && refs.Ids[1] != 1 && refs.Ids[2] != 10 && refs.Ids[3] != 11 && refs.Ids[4] != 12 {
		t.Fatal(refs)
	}

}
