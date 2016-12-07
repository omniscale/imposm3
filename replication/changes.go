package replication

import (
	"io/ioutil"
	"time"

	"gopkg.in/yaml.v2"
)

func NewChangesetDownloader(dest, url string, seq int, interval time.Duration) *downloader {
	dl := newDownloader(dest, url, seq, interval)
	dl.fileExt = ".osm.gz"
	dl.stateExt = ".state.txt"
	dl.stateTime = parseYamlTime
	go dl.fetchNextLoop()
	return dl
}

type changesetState struct {
	Time     time.Time `yaml:"last_run"`
	Sequence int       `yaml:"sequence"`
}

func parseYamlState(filename string) (changesetState, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return changesetState{}, err
	}
	state := changesetState{}
	if err := yaml.Unmarshal(b, &state); err != nil {
		return changesetState{}, err
	}
	return state, nil
}

func parseYamlTime(filename string) (time.Time, error) {
	state, err := parseYamlState(filename)
	if err != nil {
		return time.Time{}, err
	}
	return state.Time, nil
}
