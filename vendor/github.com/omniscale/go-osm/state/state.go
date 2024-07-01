package state

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type DiffState struct {
	Time     time.Time
	Sequence int
	URL      string
}

func (d DiffState) write(w io.Writer) error {
	lines := []string{}
	lines = append(lines, "timestamp="+d.Time.Format(timestampFormat))
	if d.Sequence != 0 {
		lines = append(lines, "sequenceNumber="+fmt.Sprintf("%d", d.Sequence))
	}
	lines = append(lines, "replicationUrl="+d.URL)

	for _, line := range lines {
		_, err := w.Write([]byte(line + "\n"))
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteFile(filename string, state *DiffState) error {
	tmpname := filename + "~"
	f, err := os.Create(tmpname)
	if err != nil {
		return fmt.Errorf("creating temp file for writing state file: %w", err)
	}
	err = state.write(f)
	if err != nil {
		f.Close()
		os.Remove(tmpname)
		return fmt.Errorf("writing state to %q: %w", tmpname, err)
	}
	f.Close()
	return os.Rename(tmpname, filename)
}

func ParseFile(stateFile string) (*DiffState, error) {
	f, err := os.Open(stateFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Parse(f)
}

// Parse parses an INI style state.txt file.
// timestamp is required, sequenceNumber and replicationUrl can be empty.
func Parse(f io.Reader) (*DiffState, error) {
	values, err := parseSimpleIni(f)
	if err != nil {
		return nil, fmt.Errorf("parsing state file as INI: %w", err)
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
	return &DiffState{
		Time:     timestamp,
		Sequence: sequence,
		URL:      url,
	}, nil
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

const timestampFormat = "2006-01-02T15\\:04\\:05Z"

func parseTimeStamp(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, errors.New("missing timestamp in state")
	}
	return time.Parse(timestampFormat, value)
}

func parseSequence(value string) (int, error) {
	if value == "" {
		return 0, nil
	}
	val, err := strconv.ParseInt(value, 10, 32)
	return int(val), err
}
