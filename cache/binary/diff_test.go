package binary

import (
	"testing"

	"goposm/element"
)

func TestmarshalBunch(t *testing.T) {
	bunch := []element.IdRefs{
		{123923123, []int64{1213123}},
		{123923133, []int64{1231237}},
		{123924123, []int64{912412210, 912412213}},
		{123924129, []int64{812412213}},
		{123924130, []int64{91241213}},
		{123924132, []int64{912412210, 9124213, 212412210}},
	}

	buf := MarshalIdRefsBunch(bunch)
	newBunch := UnmarshalIdRefsBunch(buf)

	t.Log(len(buf), float64(len(buf))/6.0)

	if len(newBunch) != 6 {
		t.Fatal(newBunch)
	}
	if newBunch[0].Id != 123923123 || newBunch[0].Refs[0] != 1213123 {
		t.Fatal(newBunch[0])
	}
	if newBunch[1].Id != 123923133 || newBunch[1].Refs[0] != 1231237 {
		t.Fatal(newBunch[1])
	}
	if newBunch[2].Id != 123924123 || newBunch[2].Refs[0] != 912412210 || newBunch[2].Refs[1] != 912412213 {
		t.Fatal(newBunch[2])
	}
	if newBunch[5].Id != 123924132 || newBunch[5].Refs[2] != 212412210 {
		t.Fatal(newBunch[5])
	}
}

func BenchmarkMarshalBunch(b *testing.B) {
	bunch := []element.IdRefs{
		{123923123, []int64{1213123}},
		{123923133, []int64{1231237}},
		{123924123, []int64{912412210, 912412213}},
		{123924129, []int64{812412213}},
		{123924130, []int64{91241213}},
		{123924132, []int64{912412210, 9124213, 212412210}},
	}

	for i := 0; i < b.N; i++ {
		buf := MarshalIdRefsBunch(bunch)
		UnmarshalIdRefsBunch(buf)
	}
}
