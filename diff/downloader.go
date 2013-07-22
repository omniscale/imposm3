package diff

import (
	"bytes"
	"errors"
	"fmt"
	"goposm/diff/state"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"time"
)

// N = AAA*1000000 + BBB*1000 + CCC
func diffPath(sequenceNumber int32) string {
	c := sequenceNumber % 1000
	b := sequenceNumber / 1000 % 1000
	a := sequenceNumber / 1000000

	return fmt.Sprintf("%03d/%03d/%03d", a, b, c)
}

type diffDownload struct {
	url          string
	dest         string
	lastSequence int32
}

func NewDiffDownload(dest string) *diffDownload {
	state, err := state.ParseLastState(dest)
	return &diffDownload{state.Url, dest, 0}
}

func (d *diffDownload) downloadDiff(sequence int32) error {
	dest := path.Join(d.dest, diffPath(sequence))
	err := os.MkdirAll(path.Dir(dest), 0755)

	if err != nil {
		return err
	}

	out, err := os.Create(dest + ".osc.gz")
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := http.Get(d.url + diffPath(sequence) + ".osc.gz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.New(fmt.Sprintf("invalid repsonse: %v", resp))
	}

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

func (d *diffDownload) currentState() (*state.DiffState, error) {
	resp, err := http.Get(d.url + "state.txt")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("invalid repsonse: %v", resp))
	}
	defer resp.Body.Close()
	return state.Parse(resp.Body)
}

func (d *diffDownload) downloadState(sequence int32) (*state.DiffState, error) {
	dest := path.Join(d.dest, diffPath(sequence))
	err := os.MkdirAll(path.Dir(dest), 0755)
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(d.url + diffPath(sequence) + ".state.txt")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("invalid repsonse: %v", resp))
	}

	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, resp.Body)
	if err != nil {
		return nil, err
	}

	err = ioutil.WriteFile(dest+".state.txt", buf.Bytes(), 0644)
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(buf.Bytes())
	return state.Parse(reader)
}

// func missingDiffs(since time.Time, source *diffDownload) ([]int, error) {
// 	state, err := downloadState(source.url + "state.txt")
// 	if err != nil {
// 		return nil, err
// 	}
// 	for since.Before(state.Time) {
// 		state, err = downloadState(source.url + diffPath(state.Sequence-1) + ".state.txt")
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	return nil, nil
// }

func (d *diffDownload) DownloadSince(since time.Time) error {
	state, err := d.currentState()
	if err != nil {
		return err
	}

	for since.Before(state.Time) {
		state, err = d.downloadState(state.Sequence - 1)
		fmt.Println(state)
		if err != nil {
			return err
		}
	}
	return nil
}
