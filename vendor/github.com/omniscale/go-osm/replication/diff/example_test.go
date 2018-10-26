package diff_test

import (
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/omniscale/go-osm/replication/diff"
)

func Example() {
	// This example shows how to automatically download OSM diff files.

	// We store all diffs in a temporary directory for this example.
	diffDir, err := ioutil.TempDir("", "")
	if err != nil {
		log.Fatal("unable to create diff dir:", err)
	}
	defer os.RemoveAll(diffDir)

	// Where do we fetch our diffs from?
	url := "https://planet.openstreetmap.org/replication/minute/"

	// Query the ID of the latest diff.
	seqID, err := diff.CurrentSequence(url)
	if err != nil {
		log.Fatal("unable to fetch current sequence:", err)
	}

	// Start downloader to fetch diffs. Start with a previous sequence ID, so
	// that we don't have to wait till the files are available.
	dl := diff.NewDownloader(diffDir, url, seqID-5, time.Minute)

	// Iterate all diffs as they are downloaded
	downloaded := 0
	for seq := range dl.Sequences() {

		if seq.Error != nil {
			// Error is set if an error occurred during download (network issues, etc.).
			// Filename and Time is not set, but you can access the Sequence and Error.
			log.Printf("error while downloading diff #%d: %s", seq.Sequence, seq.Error)
			// The downloader automatically retries after a short delay, so we
			// can just continue.
			continue
		}

		downloaded++

		// seq contains the Filename of the downloaded diff file. You can use parser/diff to parse the content.
		log.Printf("downloaded diff #%d with changes till %s to %s", seq.Sequence, seq.Time, seq.Filename)

		if downloaded == 3 {
			// Stop downloading after 3 diffs for this example.
			// (Stop() closes the channel from dl.Sequences and for our loop will stop).
			dl.Stop()
		}
	}
	// Output:
}
