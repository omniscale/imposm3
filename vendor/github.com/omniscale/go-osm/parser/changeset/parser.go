package changeset

import (
	"compress/gzip"
	"context"
	"encoding/xml"
	"io"

	"github.com/omniscale/go-osm"
	"github.com/omniscale/go-osm/parser/changeset/internal/osmxml"
	"github.com/pkg/errors"
)

type Parser struct {
	reader io.Reader
	conf   Config
	err    error
}

type Config struct {
	// Changesets specifies the destination for parsed changesets.
	Changesets chan osm.Changeset

	// KeepOpen specifies whether the destination channel should be keept open
	// after Parse(). By default, the Changesets channel is closed after Parse().
	KeepOpen bool
}

// New creates a new parser for the provided input. Config specifies the destinations for the parsed changesets.
func New(r io.Reader, conf Config) *Parser {
	return &Parser{reader: r, conf: conf}
}

// NewGZIP returns a parser from a GZIP compressed io.Reader
func NewGZIP(r io.Reader, conf Config) (*Parser, error) {
	r, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return New(r, conf), nil
}

// Error returns the first error that occurred during Header/Parse calls.
func (p *Parser) Error() error {
	return p.err
}

func (p *Parser) Parse(ctx context.Context) (err error) {
	if p.err != nil {
		return p.err
	}
	defer func() {
		if err != nil {
			p.err = err
		}
	}()

	if !p.conf.KeepOpen {
		defer func() {
			if p.conf.Changesets != nil {
				close(p.conf.Changesets)
			}
		}()
	}

	dec := xml.NewDecoder(p.reader)
	cf := osmxml.ChangeFile{}
	if err := dec.Decode(&cf); err != nil {
		return errors.Wrap(err, "decoding changes file")
	}

	for _, ch := range cf.Changes {
		result := osm.Changeset{
			ID:         ch.ID,
			CreatedAt:  ch.CreatedAt,
			ClosedAt:   ch.ClosedAt,
			Open:       ch.Open,
			UserID:     ch.UserID,
			UserName:   ch.UserName,
			NumChanges: ch.NumChanges,
			MaxExtent: [4]float64{
				ch.MinLon,
				ch.MinLat,
				ch.MaxLon,
				ch.MaxLat,
			},
		}

		tags := make(osm.Tags, len(ch.Tags))
		for _, t := range ch.Tags {
			tags[t.Key] = t.Value
		}
		result.Tags = tags

		comment := make([]osm.Comment, len(ch.Comments))
		for i, t := range ch.Comments {
			comment[i] = osm.Comment{
				UserID:    t.UserID,
				UserName:  t.UserName,
				CreatedAt: t.Date,
				Text:      t.Text,
			}
		}
		result.Comments = comment

		select {
		case <-ctx.Done():
		case p.conf.Changesets <- result:
		}
	}

	return nil
}
