package osm

import (
	"time"
)

// A Changeset contains metadata about a single OSM changeset.
type Changeset struct {
	ID         int64
	CreatedAt  time.Time
	ClosedAt   time.Time
	Open       bool
	UserID     int32
	UserName   string
	NumChanges int32
	MaxExtent  [4]float64
	Comments   []Comment
	Tags       Tags
}

// A Comment contains a single comment made to a Changeset.
type Comment struct {
	UserID    int32
	UserName  string
	CreatedAt time.Time
	Text      string
}
