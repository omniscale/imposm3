package diff

import (
	"testing"
)

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

// func _TestMissingDiffs(t *testing.T) {
// 	download := &diffDownload{"http://planet.openstreetmap.org/replication/hour/", "/tmp/diffs", 0}
// 	_, err := missingDiffs(time.Now().Add(-3*time.Hour), source)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	t.Fatal("")
// }

// func TestDownloadDiffs(t *testing.T) {
// 	diffs := &diffDownload{"http://planet.openstreetmap.org/replication/minute/", "/tmp/diffs", 0}
// 	err := diffs.DownloadSince(time.Now().Add(-5 * time.Minute))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// }
