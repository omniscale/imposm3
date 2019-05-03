package binary

import (
	"testing"

	"github.com/omniscale/imposm3/element"
)

func TestMarshalBunch(t *testing.T) {
	bunch := []element.IDRefs{
		{ID: 123923123, Refs: []int64{1213123}},
		{ID: 123923133, Refs: []int64{1231237}},
		{ID: 123924123, Refs: []int64{912412210, 912412213}},
		{ID: 123924129, Refs: []int64{812412213}},
		{ID: 123924130, Refs: []int64{91241213}},
		{ID: 123924132, Refs: []int64{912412210, 9124213, 212412210}},
	}

	buf := MarshalIDRefsBunch2(bunch, nil)
	newBunch := UnmarshalIDRefsBunch2(buf, nil)

	t.Log(len(buf), float64(len(buf))/6.0)

	if len(newBunch) != 6 {
		t.Fatal(newBunch)
	}
	if newBunch[0].ID != 123923123 || newBunch[0].Refs[0] != 1213123 {
		t.Fatal(newBunch[0])
	}
	if newBunch[1].ID != 123923133 || newBunch[1].Refs[0] != 1231237 {
		t.Fatal(newBunch[1])
	}
	if newBunch[2].ID != 123924123 || newBunch[2].Refs[0] != 912412210 || newBunch[2].Refs[1] != 912412213 {
		t.Fatal(newBunch[2])
	}
	if newBunch[5].ID != 123924132 || newBunch[5].Refs[2] != 212412210 {
		t.Fatal(newBunch[5])
	}
}

func BenchmarkMarshalBunch(b *testing.B) {
	bunch := []element.IDRefs{
		{ID: 123923123, Refs: []int64{1213123}},
		{ID: 123923133, Refs: []int64{1231237}},
		{ID: 123924123, Refs: []int64{912412210, 912412213}},
		{ID: 123924129, Refs: []int64{812412213}},
		{ID: 123924130, Refs: []int64{91241213}},
		{ID: 123924132, Refs: []int64{912412210, 9124213, 212412210}},
	}
	idRefs := []element.IDRefs{}
	buf := []byte{}
	for i := 0; i < b.N; i++ {
		buf = MarshalIDRefsBunch2(bunch, buf)
		idRefs = UnmarshalIDRefsBunch2(buf, idRefs)
	}
}
