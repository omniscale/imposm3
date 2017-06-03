package replication

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"

	"gopkg.in/fsnotify.v1"

	"github.com/omniscale/imposm3"
	"github.com/omniscale/imposm3/logging"
)

var log = logging.NewLogger("replication")

var NotAvailable = errors.New("file not available")

type Sequence struct {
	Filename      string
	StateFilename string
	Time          time.Time
	Sequence      int
}

type Source interface {
	Sequences() <-chan Sequence
}

// N = AAA*1000000 + BBB*1000 + CCC
func seqPath(seq int) string {
	c := seq % 1000
	b := seq / 1000 % 1000
	a := seq / 1000000

	return fmt.Sprintf("%03d/%03d/%03d", a, b, c)
}

var _ Source = &downloader{}

type downloader struct {
	baseUrl      string
	dest         string
	fileExt      string
	stateExt     string
	lastSequence int
	stateTime    func(string) (time.Time, error)
	interval     time.Duration
	errWaittime  time.Duration
	naWaittime   time.Duration
	sequences    chan Sequence
	client       *http.Client
}

func newDownloader(dest, url string, seq int, interval time.Duration) *downloader {
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

	dl := &downloader{
		baseUrl:      url,
		dest:         dest,
		lastSequence: seq,
		interval:     interval,
		errWaittime:  60 * time.Second,
		naWaittime:   10 * time.Second,
		sequences:    make(chan Sequence, 1),
		client:       client,
	}

	return dl
}

func (d *downloader) Sequences() <-chan Sequence {
	return d.sequences
}

func (d *downloader) download(seq int, ext string) error {
	dest := path.Join(d.dest, seqPath(seq)+ext)
	url := d.baseUrl + seqPath(seq) + ext

	if _, err := os.Stat(dest); err == nil {
		return nil
	}

	if err := os.MkdirAll(path.Dir(dest), 0755); err != nil {
		return err
	}

	tmpDest := fmt.Sprintf("%s~%d", dest, os.Getpid())
	out, err := os.Create(tmpDest)
	if err != nil {
		return err
	}
	defer out.Close()

	req, err := http.NewRequest("GET", url, nil)
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
		return NotAvailable
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

func (d *downloader) downloadTillSuccess(seq int, ext string) {
	for {
		err := d.download(seq, ext)
		if err == nil {
			break
		}
		if err == NotAvailable {
			time.Sleep(d.naWaittime)
		} else {
			log.Warn(err)
			time.Sleep(d.errWaittime)
		}
	}
}

func (d *downloader) fetchNextLoop() {
	stateFile := path.Join(d.dest, seqPath(d.lastSequence)+d.stateExt)
	lastTime, err := d.stateTime(stateFile)
	for {
		if err == nil {
			nextDiffTime := lastTime.Add(d.interval)
			if nextDiffTime.After(time.Now()) {
				// we catched up and the next diff file is in the future.
				// wait till last diff time + interval, before fetching next
				nextDiffTime = lastTime.Add(d.interval + 2*time.Second /* allow small time diff between server*/)
				waitFor := nextDiffTime.Sub(time.Now())
				time.Sleep(waitFor)
			}
		}
		nextSeq := d.lastSequence + 1
		// download will retry until they succeed
		d.downloadTillSuccess(nextSeq, d.stateExt)
		d.downloadTillSuccess(nextSeq, d.fileExt)
		d.lastSequence = nextSeq
		base := path.Join(d.dest, seqPath(d.lastSequence))
		lastTime, _ = d.stateTime(base + d.stateExt)
		d.sequences <- Sequence{
			Sequence:      d.lastSequence,
			Filename:      base + d.fileExt,
			StateFilename: base + d.stateExt,
			Time:          lastTime,
		}
	}
}

var _ Source = &reader{}

type reader struct {
	dest         string
	fileExt      string
	stateExt     string
	lastSequence int
	stateTime    func(string) (time.Time, error)
	sequences    chan Sequence
}

func newReader(dest string, seq int) *reader {
	r := &reader{
		dest:         dest,
		lastSequence: seq,
		sequences:    make(chan Sequence, 1),
	}

	return r
}

func (d *reader) Sequences() <-chan Sequence {
	return d.sequences
}

func (d *reader) waitTillPresent(seq int, ext string) error {
	filename := path.Join(d.dest, seqPath(seq)+ext)
	return waitTillPresent(filename)
}

func (d *reader) fetchNextLoop() {
	for {
		nextSeq := d.lastSequence + 1
		if err := d.waitTillPresent(nextSeq, d.stateExt); err != nil {
			log.Error(err)
		}
		if err := d.waitTillPresent(nextSeq, d.fileExt); err != nil {
			log.Error(err)
		}
		d.lastSequence = nextSeq
		base := path.Join(d.dest, seqPath(d.lastSequence))
		lastTime, _ := d.stateTime(base + d.stateExt)
		d.sequences <- Sequence{
			Sequence:      d.lastSequence,
			Filename:      base + d.fileExt,
			StateFilename: base + d.stateExt,
			Time:          lastTime,
		}
	}
}

// waitTillPresent blocks till file is present.
func waitTillPresent(filename string) error {
	if _, err := os.Stat(filename); err == nil {
		return nil
	}

	// fsnotify does not work recursive. wait for parent dirs first (e.g. 002/134)
	parent := filepath.Dir(filename)
	if err := waitTillPresent(parent); err != nil {
		return err
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer w.Close()
	// need to watch on parent if we want to get events for new file
	w.Add(parent)

	// check again, in case file was created before we added the file
	if _, err := os.Stat(filename); err == nil {
		return nil
	}

	for {
		select {
		case evt := <-w.Events:
			if evt.Op&fsnotify.Create == fsnotify.Create && evt.Name == filename {
				return nil
			}
		}
	}
	return nil
}
