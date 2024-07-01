package replication

import "time"

// A Sequence contains metadata for a replication file (diff or changeset).
type Sequence struct {
	// Sequence specifies the number of this replication file.
	Sequence int
	// Error describes any an error that occurred the during download of the
	// replication file. The filenames and Time are zero if Error is set.
	Error error
	// Filename specifies the full path to the replication file.
	Filename string
	// StateFilename specifies the full path to the .state.txt file for this sequence.
	StateFilename string
	// Time specifies the creation time of this replication sequence. The
	// replication file will only contain data older then this timestamp.
	Time time.Time
	// Latest is true if the next Sequence is not yet available.
	Latest bool
}

// A Source provides a stream of replication files.
type Source interface {
	// Sequences returns the channel with metadata for each replication file.
	Sequences() <-chan Sequence

	// Stop signals the source that it should stop loading more replication
	// files and that Sequences channel should be closed.
	Stop()
}
