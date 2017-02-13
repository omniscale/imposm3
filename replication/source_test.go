package replication

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"testing"
)

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

func TestWaitTillPresent(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "imposm3tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	exists := filepath.Join(tmpdir, "exists")
	f, err := os.Create(exists)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	waitTillPresent(exists)

	create := filepath.Join(tmpdir, "create")
	go func() {
		time.Sleep(200 * time.Millisecond)
		f, err := os.Create(create)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}()
	waitTillPresent(create)

	sub := filepath.Join(tmpdir, "sub", "dir", "create")
	go func() {
		time.Sleep(200 * time.Millisecond)
		if err := os.Mkdir(filepath.Join(tmpdir, "sub"), 0755); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
		if err := os.Mkdir(filepath.Join(tmpdir, "sub", "dir"), 0755); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
		f, err := os.Create(sub)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}()
	waitTillPresent(sub)
}
