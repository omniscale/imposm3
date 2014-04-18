package state

import (
	"bufio"
	"errors"
	"fmt"
	"imposm3/logging"
	"imposm3/parser/pbf"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

var log = logging.NewLogger("diff")

type DiffState struct {
	Time     time.Time
	Sequence int32
	Url      string
}

func (d DiffState) String() string {
	return fmt.Sprintf("Diff #%d from %s", d.Sequence, d.Time.Local())
}

func (d DiffState) WriteToFile(file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := bufio.NewWriter(f)

	lines := []string{}
	lines = append(lines, "timestamp="+d.Time.Format("2006-01-02T15\\:04\\:05Z"))
	if d.Sequence != 0 {
		lines = append(lines, "sequenceNumber="+fmt.Sprintf("%d", d.Sequence))
	}
	lines = append(lines, "replicationUrl="+d.Url)

	for _, line := range lines {
		_, err = writer.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}
	return writer.Flush()
}

func WriteLastState(cacheDir string, state *DiffState) error {
	stateFile := path.Join(cacheDir, "last.state.txt")
	return state.WriteToFile(stateFile)
}

func ParseFromOsc(oscFile string) (*DiffState, error) {
	var stateFile string
	if !strings.HasSuffix(oscFile, ".osc.gz") {
		log.Warn("cannot read state file for non .osc.gz files")
		return nil, nil
	}

	stateFile = oscFile[:len(oscFile)-len(".osc.gz")] + ".state.txt"
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		log.Warn("cannot find state file ", stateFile)
		return nil, nil
	}

	f, err := os.Open(stateFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ParseFile(stateFile)
}

func FromPbf(pbfFile *pbf.Pbf) *DiffState {
	var timestamp time.Time
	if pbfFile.Header.Time.Unix() != 0 {
		timestamp = pbfFile.Header.Time
	} else {
		fstat, err := os.Stat(pbfFile.Filename)
		if err != nil {
			log.Warn("unable to stat pbffile: ", err)
			return nil
		}
		timestamp = fstat.ModTime()
	}

	replicationUrl := "http://planet.openstreetmap.org/replication/minute/"

	seq := estimateSequence(replicationUrl, timestamp)
	if seq != 0 {
		// start a two hours earlier
		seq -= 120
	}
	return &DiffState{Time: timestamp, Url: replicationUrl, Sequence: seq}
}

func ParseFile(stateFile string) (*DiffState, error) {
	f, err := os.Open(stateFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Parse(f)
}

func Parse(f io.Reader) (*DiffState, error) {
	values, err := parseSimpleIni(f)
	if err != nil {
		return nil, err
	}

	timestamp, err := parseTimeStamp(values["timestamp"])
	if err != nil {
		return nil, err
	}
	sequence, err := parseSequence(values["sequenceNumber"])
	if err != nil {
		return nil, err
	}

	url := values["replicationUrl"]
	return &DiffState{timestamp, sequence, url}, nil
}

func ParseLastState(cacheDir string) (*DiffState, error) {
	stateFile := path.Join(cacheDir, "last.state.txt")
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		return nil, err
	}
	return ParseFile(stateFile)
}

func parseSimpleIni(f io.Reader) (map[string]string, error) {
	result := make(map[string]string)

	reader := bufio.NewScanner(f)
	for reader.Scan() {
		line := reader.Text()
		if line != "" && line[0] == '#' {
			continue
		}
		if strings.Contains(line, "=") {
			keyVal := strings.SplitN(line, "=", 2)
			result[strings.TrimSpace(keyVal[0])] = strings.TrimSpace(keyVal[1])
		}

	}
	if err := reader.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func parseTimeStamp(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, errors.New("missing timestamp in state")
	}
	return time.Parse("2006-01-02T15\\:04\\:05Z", value)
}

func parseSequence(value string) (int32, error) {
	if value == "" {
		log.Warn("missing sequenceNumber in state file")
		return 0, nil
	}
	val, err := strconv.ParseInt(value, 10, 32)
	return int32(val), err
}

func currentState(url string) (*DiffState, error) {
	resp, err := http.Get(url + "state.txt")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("invalid repsonse: %v", resp))
	}
	defer resp.Body.Close()
	return Parse(resp.Body)
}

func estimateSequence(url string, timestamp time.Time) int32 {
	state, err := currentState(url)
	if err != nil {
		log.Warn("Unable to fetch current state from ", url, ":", err)
		return 0
	}
	behind := state.Time.Sub(timestamp)
	return state.Sequence - int32(behind.Minutes())
}
