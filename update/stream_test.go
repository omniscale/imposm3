package update

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestStreamBatchValidation(t *testing.T) {
	for _, input := range []string{
		`{}`, `null`, `{"id":0,"files":["a.osc.gz"]}`,
		`{"id":-1,"files":["a.osc.gz"]}`, `{"id":1.5,"files":["a.osc.gz"]}`,
		`{"id":1,"files":[]}`, `{"id":1,"files":null}`, `{"id":1,"files":["x.pbf"]}`,
		`{"id":1,"files":["x.osc.gz","x.osc.gz"]}`, `invalid`,
	} {
		t.Run(input, func(t *testing.T) {
			if _, err := decodeStreamBatch([]byte(input)); err == nil {
				t.Fatalf("accepted invalid batch %s", input)
			}
		})
	}
	batch, err := decodeStreamBatch([]byte(`{"id":2,"files":["a space/1.osc.gz","2.osc.gz"]}`))
	if err != nil || batch.ID != 2 || len(batch.Files) != 2 {
		t.Fatalf("unexpected decode: %+v %v", batch, err)
	}
	for _, count := range []int{64, 65} {
		files := make([]string, count)
		for i := range files {
			files[i] = fmt.Sprintf("%d.osc.gz", i)
		}
		data, err := json.Marshal(streamBatch{ID: 1, Files: files})
		if err != nil {
			t.Fatal(err)
		}
		_, err = decodeStreamBatch(data)
		if (err == nil) != (count == 64) {
			t.Fatalf("unexpected validation of %d files: %v", count, err)
		}
	}
}

func TestStreamAcknowledgesAfterApplyingEachBatch(t *testing.T) {
	input := strings.NewReader("{\"id\":41,\"files\":[\"1.osc.gz\",\"2.osc.gz\"]}\n{\"id\":42,\"files\":[\"3.osc.gz\"]}\n")
	var output bytes.Buffer
	var calls [][]string
	err := processStream(input, &output, func(files []string) error {
		// The next OK must not be visible until the callback has completed.
		want := "READY imposm-stream-v1\n"
		if len(calls) == 1 {
			want += "OK 41\n"
		}
		if output.String() != want {
			t.Fatalf("premature/unflushed acknowledgement: %q", output.String())
		}
		calls = append(calls, files)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, [][]string{{"1.osc.gz", "2.osc.gz"}, {"3.osc.gz"}}) {
		t.Fatalf("unexpected batches: %v", calls)
	}
	if output.String() != "READY imposm-stream-v1\nOK 41\nOK 42\n" {
		t.Fatalf("unexpected output: %q", output.String())
	}
}

func TestStreamDoesNotAcknowledgeFailureOrProcessLaterRequests(t *testing.T) {
	input := strings.NewReader("{\"id\":1,\"files\":[\"1.osc.gz\"]}\n{\"id\":2,\"files\":[\"2.osc.gz\"]}\n{\"id\":3,\"files\":[\"3.osc.gz\"]}\n")
	var output bytes.Buffer
	failure := errors.New("import/flush/sync failed")
	calls := 0
	err := processStream(input, &output, func([]string) error {
		calls++
		if calls == 2 {
			return failure
		}
		return nil
	})
	if !errors.Is(err, failure) || calls != 2 {
		t.Fatalf("unexpected failure handling: %v, calls=%d", err, calls)
	}
	if output.String() != "READY imposm-stream-v1\nOK 1\n" {
		t.Fatalf("failed batch was acknowledged: %q", output.String())
	}
}

func TestStreamEOFAndMalformedInput(t *testing.T) {
	for _, input := range []string{"", "invalid\n", strings.Repeat("x", 1024*1024+1)} {
		var output bytes.Buffer
		err := processStream(strings.NewReader(input), &output, func([]string) error {
			t.Fatal("unexpected import")
			return nil
		})
		if (err == nil) != (input == "") {
			t.Fatalf("unexpected EOF/parse result: %v", err)
		}
		if output.String() != "READY imposm-stream-v1\n" {
			t.Fatalf("unexpected output: %q", output.String())
		}
	}
}

type streamFailWriter struct{ err error }

func (w streamFailWriter) Write([]byte) (int, error) { return 0, w.err }

func TestStreamOutputFailure(t *testing.T) {
	failure := errors.New("closed output pipe")
	err := processStream(strings.NewReader("{}\n"), streamFailWriter{failure}, func([]string) error {
		t.Fatal("must not import without a ready output pipe")
		return nil
	})
	if !errors.Is(err, failure) {
		t.Fatalf("unexpected output error: %v", err)
	}
}

func TestSyncStreamState(t *testing.T) {
	path := filepath.Join(t.TempDir(), LastStateFilename)
	if err := syncStreamState(path); err == nil {
		t.Fatal("accepted missing state file")
	}
	if err := os.WriteFile(path, []byte("state fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := syncStreamState(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "state fixture" {
		t.Fatalf("state contents changed: %q %v", data, err)
	}
}
