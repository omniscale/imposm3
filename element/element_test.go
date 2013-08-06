package element

import (
	"testing"
)

func TestIdRefs(t *testing.T) {

	idRefs := IdRefs{}

	idRefs.Add(1)
	if idRefs.Refs[0] != 1 {
		t.Fatal(idRefs)
	}

	idRefs.Add(10)
	if idRefs.Refs[0] != 1 || idRefs.Refs[1] != 10 {
		t.Fatal(idRefs)
	}

	// insert twice
	idRefs.Add(10)
	if idRefs.Refs[0] != 1 || idRefs.Refs[1] != 10 || len(idRefs.Refs) != 2 {
		t.Fatal(idRefs)
	}

	// insert before
	idRefs.Add(0)
	if idRefs.Refs[0] != 0 || idRefs.Refs[1] != 1 || idRefs.Refs[2] != 10 {
		t.Fatal(idRefs)
	}

	// insert after
	idRefs.Add(12)
	if idRefs.Refs[0] != 0 || idRefs.Refs[1] != 1 || idRefs.Refs[2] != 10 || idRefs.Refs[3] != 12 {
		t.Fatal(idRefs)
	}

	// insert between
	idRefs.Add(11)
	if idRefs.Refs[0] != 0 || idRefs.Refs[1] != 1 || idRefs.Refs[2] != 10 || idRefs.Refs[3] != 11 || idRefs.Refs[4] != 12 {
		t.Fatal(idRefs)
	}

	// delete between
	idRefs.Delete(11)
	if idRefs.Refs[0] != 0 || idRefs.Refs[1] != 1 || idRefs.Refs[2] != 10 || idRefs.Refs[3] != 12 {
		t.Fatal(idRefs)
	}

	// delete end
	idRefs.Delete(12)
	if idRefs.Refs[0] != 0 || idRefs.Refs[1] != 1 || idRefs.Refs[2] != 10 {
		t.Fatal(idRefs)
	}

	// delete begin
	idRefs.Delete(0)
	if idRefs.Refs[0] != 1 || idRefs.Refs[1] != 10 {
		t.Fatal(idRefs)
	}

	// delete missing
	idRefs.Delete(99)
	if idRefs.Refs[0] != 1 || idRefs.Refs[1] != 10 || len(idRefs.Refs) != 2 {
		t.Fatal(idRefs)
	}

}
