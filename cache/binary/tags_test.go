package binary

import (
	"reflect"
	"sort"
	"testing"

	"github.com/omniscale/imposm3/element"
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

func TestTagsArrayEncoding(t *testing.T) {
	for i, check := range []struct {
		// Test tags with string array of key/value instead of
		// element.Tags to have fixed order
		tags     []string
		expected []string
	}{
		{
			[]string{"name", "foo", "highway", "residential", "oneway", "yes", "addr:housenumber", ""},
			[]string{
				"\x01foo",
				"\ue001",
				"\ue008",
				"\x06",
			},
		},
		{ // ascii control characters are escaped
			[]string{"name", "\tfoo", "\tfoo", "bar"},
			[]string{
				"\x01\tfoo",
				"\ufffd\tfoo", "bar",
			},
		},
		{ // private unicode characters are escaped
			[]string{"\ue008foo", "bar", "\ue008", "baz"},
			[]string{
				"\ufffd\ue008foo", "bar",
				"\ufffd\ue008", "baz",
			},
		},
		{ // replacement characters (our escape token) are escaped as well
			[]string{"\ufffd\ue008foo", "bar", "\ufffd\ufffd\ufffd\ue008foo", "bar"},
			[]string{
				"\ufffd\ufffd\ue008foo", "bar",
				"\ufffd\ufffd\ufffd\ufffd\ue008foo", "bar",
			},
		},
		{ // empty keys are handled #122
			[]string{"foo", "bar", "", "empty"},
			[]string{
				"foo", "bar",
				"", "empty",
			},
		},
	} {
		var actual []string
		for i := 0; i < len(check.tags); i += 2 {
			actual = appendTag(actual, check.tags[i], check.tags[i+1])
		}
		if len(check.expected) != len(actual) {
			t.Errorf("case %d: unexpected tag array %#v != %#v", i, actual, check.expected)
			continue
		}
		for j := range check.expected {
			if check.expected[j] != actual[j] {
				t.Errorf("case %d: encoded string %d does not match array %#v != %#v", i, j, actual[j], check.expected[j])
			}
		}
		actualTags := tagsFromArray(actual)
		expectedTags := make(element.Tags)
		for i := 0; i < len(check.tags); i += 2 {
			expectedTags[check.tags[i]] = check.tags[i+1]
		}
		if !reflect.DeepEqual(actualTags, expectedTags) {
			t.Errorf("case %d: unexpected tags %#v != %#v", i, actualTags, expectedTags)
		}
	}
}

func TestTagsArrayIssue122Panic(t *testing.T) {
	tagsFromArray([]string{"foo", "bar"})
	defer func() {
		p := recover()
		if p == nil {
			t.Fatal("did not panic")
		}
	}()
	tagsFromArray([]string{"foo"})
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
