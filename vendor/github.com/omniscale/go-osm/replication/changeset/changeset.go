package changeset

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/omniscale/go-osm/replication"
	"github.com/omniscale/go-osm/replication/internal/source"
	"gopkg.in/yaml.v2"
)

// NewDownloader starts a background downloader for OSM changesets.
// Changesets are fetched from url and stored in changesetDir. seq is the first
// sequence that should be downloaded. Changesets are downloaded as fast as
// possible with a single connection until the first changeset is missing.
// After that, it uses the interval to estimate when a new changeset should
// appear. The returned replication.Source provides metadata for each
// downloaded changeset.
func NewDownloader(changesetDir, url string, seq int, interval time.Duration) replication.Source {
	dl := source.NewDownloader(changesetDir, url, seq, interval)
	dl.FileExt = ".osm.gz"
	dl.StateExt = ".state.txt"
	dl.StateTime = parseYamlTime
	go dl.Start()
	return dl
}

// NewReader starts a goroutine to search for OSM changeset files (.osm.gz).
// This can be used if another tool is already downloading changeset files.
// Changesets are searched in changesetDir. seq is the first sequence that
// should be returned. Changesets are returned as fast as possible if they are
// already available in changesetDir. After that, it uses file change
// notifications provided by your OS to detect new files. The returned
// replication.Source provides metadata for each changeset.
func NewReader(changesetDir string, seq int) replication.Source {
	r := source.NewReader(changesetDir, seq)
	r.FileExt = ".osm.gz"
	r.StateExt = ".state.txt"
	r.StateTime = parseYamlTime
	go r.Start()
	return r
}

// CurrentSequence returns the ID of the latest changeset available at the
// given replication URL (e.g.
// https://planet.openstreetmap.org/replication/changesets/)
func CurrentSequence(replURL string) (int, error) {
	resp, err := http.Get(replURL + "state.yaml")
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, errors.New(fmt.Sprintf("invalid repsonse: %v", resp))
	}
	defer resp.Body.Close()
	b := &bytes.Buffer{}
	if _, err := io.Copy(b, resp.Body); err != nil {
		return 0, err
	}
	state, err := parseYamlState(b.Bytes())
	if err != nil {
		return 0, err
	}
	return state.Sequence, nil
}

type changesetState struct {
	Time     yamlStateTime `yaml:"last_run"`
	Sequence int           `yaml:"sequence"`
}

type yamlStateTime struct {
	time.Time
}

func (y *yamlStateTime) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var ts string
	if err := unmarshal(&ts); err != nil {
		return err
	}
	t, err := time.Parse("2006-01-02 15:04:05.999999999 -07:00", ts)
	y.Time = t
	return err
}

func parseYamlStateFile(filename string) (changesetState, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return changesetState{}, err
	}
	return parseYamlState(b)
}

func parseYamlState(b []byte) (changesetState, error) {
	state := changesetState{}
	if err := yaml.Unmarshal(b, &state); err != nil {
		return changesetState{}, err
	}
	return state, nil
}

func parseYamlTime(filename string) (time.Time, error) {
	state, err := parseYamlStateFile(filename)
	if err != nil {
		return time.Time{}, err
	}
	return state.Time.Time, nil
}
