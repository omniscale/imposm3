package download

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/omniscale/imposm3"
	"github.com/omniscale/imposm3/diff/state"
	"github.com/omniscale/imposm3/logging"
)

var log = logging.NewLogger("downloader")

// N = AAA*1000000 + BBB*1000 + CCC
func diffPath(sequenceNumber seqId) string {
	c := sequenceNumber % 1000
	b := sequenceNumber / 1000 % 1000
	a := sequenceNumber / 1000000

	return fmt.Sprintf("%03d/%03d/%03d", a, b, c)
}

type seqId int32

type Diff struct {
	FileName string
	State    *state.DiffState
}

type diffDownload struct {
	url          string
	dest         string
	lastSequence seqId
	diffInterval time.Duration
	errWaittime  time.Duration
	naWaittime   time.Duration
	NextDiff     chan Diff
	client       *http.Client
}

type NotAvailable struct {
	Url      string
	Sequence seqId
}

func (na NotAvailable) Error() string {
	return fmt.Sprintf("OSC #%d not available at %s", na.Sequence, na.Url)
}

func NewDiffDownload(dest, url string, interval time.Duration) (*diffDownload, error) {
	s, err := state.ParseLastState(dest)
	if err != nil {
		return nil, err
	}
	if url == "" {
		url = s.Url
	}
	if url == "" {
		return nil, errors.New("no replicationUrl in last.state.txt " +
			"or replication_url in -config file")
	}
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 1 * time.Second, // do not keep alive till next interval
			}).Dial,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	downloader := &diffDownload{
		url:          url,
		dest:         dest,
		lastSequence: seqId(s.Sequence),
		diffInterval: interval,
		errWaittime:  60 * time.Second,
		naWaittime:   10 * time.Second,
		NextDiff:     make(chan Diff, 1),
		client:       client,
	}

	go downloader.fetchNextLoop()
	return downloader, nil
}

func (d *diffDownload) oscFileName(sequence seqId) string {
	return path.Join(d.dest, diffPath(sequence)) + ".osc.gz"
}

func (d *diffDownload) oscStateFileName(sequence seqId) string {
	return path.Join(d.dest, diffPath(sequence)) + ".state.txt"
}

func (d *diffDownload) downloadDiff(sequence seqId) error {
	dest := d.oscFileName(sequence)

	if _, err := os.Stat(dest); err == nil {
		return nil
	}

	err := os.MkdirAll(path.Dir(dest), 0755)

	if err != nil {
		return err
	}

	tmpDest := fmt.Sprintf("%s~%d", dest, os.Getpid())
	out, err := os.Create(tmpDest)
	if err != nil {
		return err
	}
	defer out.Close()

	req, err := http.NewRequest("GET", d.url+diffPath(sequence)+".osc.gz", nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Imposm3 "+imposm3.Version)
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return NotAvailable{d.url, sequence}
	}

	if resp.StatusCode != 200 {
		return errors.New(fmt.Sprintf("invalid repsonse: %v", resp))
	}

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	out.Close()

	err = os.Rename(tmpDest, dest)
	if err != nil {
		return err
	}

	return nil
}

func (d *diffDownload) downloadState(sequence seqId) (*state.DiffState, error) {
	dest := path.Join(d.dest, diffPath(sequence)) + ".state.txt"

	if _, err := os.Stat(dest); err == nil {
		return state.ParseFile(dest)
	}

	err := os.MkdirAll(path.Dir(dest), 0755)
	if err != nil {
		return nil, err
	}

	url := d.url + diffPath(sequence) + ".state.txt"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Imposm3 "+imposm3.Version)
	resp, err := d.client.Do(req)

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, NotAvailable{d.url, sequence}
	}

	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("invalid repsonse from %s: %v", url, resp))
	}

	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, resp.Body)
	if err != nil {
		return nil, err
	}

	err = ioutil.WriteFile(dest, buf.Bytes(), 0644)
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(buf.Bytes())
	return state.Parse(reader)
}

func (d *diffDownload) fetchNextLoop() {
	for {
		stateFile := path.Join(d.dest, diffPath(d.lastSequence)) + ".state.txt"
		s, err := state.ParseFile(stateFile)
		if err == nil {
			nextDiffTime := s.Time.Add(d.diffInterval)
			if nextDiffTime.After(time.Now()) {
				// we catched up and the next diff file is in the future.
				// wait till last diff time + interval, before fetching next
				nextDiffTime = s.Time.Add(d.diffInterval + 2*time.Second /* allow small time diff between server*/)
				waitFor := nextDiffTime.Sub(time.Now())
				time.Sleep(waitFor)
			}
		}
		nextSeq := d.lastSequence + 1
		// downloadXxxTillSuccess will retry until they succeed
		d.downloadStateTillSuccess(nextSeq)
		d.downloadDiffTillSuccess(nextSeq)
		d.lastSequence = nextSeq
		state, _ := state.ParseFile(d.oscStateFileName(nextSeq))
		d.NextDiff <- Diff{FileName: d.oscFileName(nextSeq), State: state}
	}
}

func (d *diffDownload) downloadStateTillSuccess(seq seqId) {
	for {
		_, err := d.downloadState(seq)
		if err == nil {
			break
		}
		if _, ok := err.(NotAvailable); ok {
			time.Sleep(d.naWaittime)
		} else {
			log.Warn(err)
			time.Sleep(d.errWaittime)
		}
	}
}

func (d *diffDownload) downloadDiffTillSuccess(seq seqId) {
	for {
		err := d.downloadDiff(seq)
		if err == nil {
			break
		}
		if _, ok := err.(NotAvailable); ok {
			time.Sleep(d.naWaittime)
		} else {
			log.Warn(err)
			time.Sleep(d.errWaittime)
		}
	}
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

func (d *diffDownload) DownloadSince(since time.Time) error {
	state, err := d.currentState()
	if err != nil {
		return err
	}

	for since.Before(state.Time) {
		state, err = d.downloadState(seqId(state.Sequence - 1))
		fmt.Println(state)
		if err != nil {
			return err
		}
	}
	return nil
}
