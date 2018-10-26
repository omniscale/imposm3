package osmxml

import (
	"time"
)

type ChangeFile struct {
	XMLName   string      `xml:"osm"`
	Generator string      `xml:"generator,attr"`
	Changes   []Changeset `xml:"changeset"`
}

type Changeset struct {
	ID         int64     `xml:"id,attr"`
	CreatedAt  time.Time `xml:"created_at,attr"`
	ClosedAt   time.Time `xml:"closed_at,attr"`
	Open       bool      `xml:"open,attr"`
	UserName   string    `xml:"user,attr"`
	UserID     int32     `xml:"uid,attr"`
	NumChanges int32     `xml:"num_changes,attr"`
	MinLon     float64   `xml:"min_lon,attr"`
	MinLat     float64   `xml:"min_lat,attr"`
	MaxLon     float64   `xml:"max_lon,attr"`
	MaxLat     float64   `xml:"max_lat,attr"`
	Comments   []Comment `xml:"discussion>comment"`
	Tags       []Tag     `xml:"tag"`
}

type Comment struct {
	UserID   int32     `xml:"uid,attr"`
	UserName string    `xml:"user,attr"`
	Date     time.Time `xml:"date,attr"`
	Text     string    `xml:"text"`
}

type Tag struct {
	Key   string `xml:"k,attr"`
	Value string `xml:"v,attr"`
}
