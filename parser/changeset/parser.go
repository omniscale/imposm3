package changeset

import (
	"compress/gzip"
	"encoding/xml"
	"os"
	"time"
)

type changeFile struct {
	XMLName   string      `xml:"osm"`
	Generator string      `xml:"generator,attr"`
	Changes   []Changeset `xml:"changeset"`
}

type Changeset struct {
	Id         int       `xml:"id,attr"`
	CreatedAt  IsoTime   `xml:"created_at,attr"`
	ClosedAt   IsoTime   `xml:"closed_at,attr"`
	Open       bool      `xml:"open,attr"`
	User       string    `xml:"user,attr"`
	UserId     int       `xml:"uid,attr"`
	NumChanges int       `xml:"num_changes,attr"`
	MinLon     float64   `xml:"min_lon,attr"`
	MinLat     float64   `xml:"min_lat,attr"`
	MaxLon     float64   `xml:"max_lon,attr"`
	MaxLat     float64   `xml:"max_lat,attr"`
	Comments   []Comment `xml:"discussion>comment"`
	Tags       []Tag     `xml:"tag"`
}

type Comment struct {
	UserId int     `xml:"uid,attr"`
	User   string  `xml:"user,attr"`
	Date   IsoTime `xml:"date,attr"`
	Text   string  `xml:"text"`
}

type IsoTime struct {
	time.Time
}

type Tag struct {
	Key   string `xml:"k,attr"`
	Value string `xml:"v,attr"`
}

func (t *IsoTime) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var v string
	d.DecodeElement(&v, &start)
	parse, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return err
	}
	*t = IsoTime{parse}
	return nil
}

// ParseAllOsmGz parses all changesets from a .osm.gz file.
func ParseAllOsmGz(change string) ([]Changeset, error) {
	file, err := os.Open(change)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	cf := changeFile{}
	err = xml.NewDecoder(reader).Decode(&cf)
	if err != nil {
		return nil, err
	}

	return cf.Changes, nil
}
