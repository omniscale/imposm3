package diff

import (
	"bufio"
	"errors"
	"fmt"
	"goposm/logging"
	"io"
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
	lines = append(lines, "sequenceNumber="+fmt.Sprintf("%d", d.Sequence))

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

func ParseStateFromOsc(oscFile string) (*DiffState, error) {
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
	return parseStateFile(stateFile)
}

func parseStateFile(stateFile string) (*DiffState, error) {
	f, err := os.Open(stateFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseState(f)
}

func parseState(f io.Reader) (*DiffState, error) {
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

	return &DiffState{timestamp, sequence}, nil
}

func ParseLastState(cacheDir string) (*DiffState, error) {
	stateFile := path.Join(cacheDir, "last.state.txt")
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		log.Warn("cannot find state file ", stateFile)
		return nil, nil
	}
	return parseStateFile(stateFile)
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
		return 0, errors.New("missing sqeuenceNumber in state")
	}
	val, err := strconv.ParseInt(value, 10, 32)
	return int32(val), err
}
