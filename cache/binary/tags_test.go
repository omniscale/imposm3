package binary

import (
	"imposm3/element"
	"sort"
	"testing"
)

func TestTagsAsAndFromArray(t *testing.T) {
	tags := element.Tags{"name": "foo", "highway": "residential", "oneway": "yes"}
	array := tagsAsArray(tags)

	if len(array) != 3 {
		t.Fatal("invalid length", array)
	}

	sort.Strings(array)
	for i, expected := range []string{
		"\x01foo",
		string(tagsToCodePoint["highway"]["residential"]),
		string(tagsToCodePoint["oneway"]["yes"]),
	} {
		if array[i] != expected {
			t.Fatal("invalid value", array, i, expected)
		}
	}

	tags = tagsFromArray(array)
	if len(tags) != 3 {
		t.Fatal("invalid length", tags)
	}
	if tags["name"] != "foo" || tags["highway"] != "residential" || tags["oneway"] != "yes" {
		t.Fatal("invalid tags", tags)
	}
}

func TestCodePoints(t *testing.T) {
	// codepoints should never change, so check a few for sanity
	if c := tagsToCodePoint["building"]["yes"]; c != codepoint('\ue000') {
		t.Fatalf("%x\n", c)
	}
	if c := tagsToCodePoint["surface"]["grass"]; c != codepoint('\ue052') {
		t.Fatalf("%x\n", c)
	}
	if c := tagsToCodePoint["type"]["associatedStreet"]; c != codepoint('\ue0a5') {
		t.Fatalf("%x\n", c)
	}
}
