package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"

	"gopkg.in/fsnotify.v1"

	"github.com/omniscale/go-osm/replication"
)

var isDebug = false

func debug(v ...interface{}) {
	if isDebug {
		log.Println(v...)
	}
}

type NotAvailable struct {
	url string
}

func (e *NotAvailable) Error() string {
	return fmt.Sprintf("File not available: %s", e.url)
}

// N = AAA*1000000 + BBB*1000 + CCC
func seqPath(seq int) string {
	c := seq % 1000
	b := seq / 1000 % 1000
	a := seq / 1000000

	return fmt.Sprintf("%03d/%03d/%03d", a, b, c)
}

var _ replication.Source = &downloader{}

type downloader struct {
	baseUrl      string
	dest         string
	FileExt      string
	StateExt     string
	lastSequence int
	StateTime    func(string) (time.Time, error)
	interval     time.Duration
	errWaittime  time.Duration
	naWaittime   time.Duration
	sequences    chan replication.Sequence
	client       *http.Client
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewDownloader(dest, url string, seq int, interval time.Duration) *downloader {
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

	var naWaittime time.Duration
	switch {
	case interval >= 24*time.Hour:
		naWaittime = 5 * time.Minute
	case interval >= time.Hour:
		naWaittime = 60 * time.Second
	default:
		naWaittime = 10 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	dl := &downloader{
		baseUrl:      url,
		dest:         dest,
		lastSequence: seq - 1, // we want to start with seq, so lastSequence is -1
		interval:     interval,
		errWaittime:  60 * time.Second,
		naWaittime:   naWaittime,
		sequences:    make(chan replication.Sequence, 4),
		client:       client,
		ctx:          ctx,
		cancel:       cancel,
	}

	return dl
}

func (d *downloader) Sequences() <-chan replication.Sequence {
	return d.sequences
}

func (d *downloader) download(seq int, ext string) error {
	dest := path.Join(d.dest, seqPath(seq)+ext)
	url := d.baseUrl + seqPath(seq) + ext
	debug("[debug] Downloading diff file from ", url)

	if _, err := os.Stat(dest); err == nil {
		return nil
	}

	if err := os.MkdirAll(path.Dir(dest), 0755); err != nil {
		return err
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "github.com/omniscale/go-osm")
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return &NotAvailable{url}
	}

	if resp.StatusCode != 200 {
		return errors.New(fmt.Sprintf("invalid response: %v", resp))
	}

	tmpDest := fmt.Sprintf("%s~%d", dest, os.Getpid())
	out, err := os.Create(tmpDest)
	if err != nil {
		return err
	}
	defer out.Close()

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

// downloadTillSuccess tries to download file till it is available, returns
// true if available on first try.
func (d *downloader) downloadTillSuccess(ctx context.Context, seq int, ext string) bool {
	for tries := 0; ; tries++ {
		if ctx.Err() != nil {
			return false
		}
		err := d.download(seq, ext)
		if err == nil {
			return tries == 0
		}
		if _, ok := err.(*NotAvailable); ok {
			wait(ctx, d.naWaittime)
		} else {
			debug("[error] Downloading file:", err)
			d.sequences <- replication.Sequence{
				Sequence: seq,
				Error:    err,
			}
			wait(ctx, d.errWaittime)
		}
	}
}

func wait(ctx context.Context, duration time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(duration):
	}
}

func (d *downloader) Start() {
	d.fetchNextLoop()
}

func (d *downloader) Stop() {
	d.cancel()
}

func (d *downloader) fetchNextLoop() {
	stateFile := path.Join(d.dest, seqPath(d.lastSequence)+d.StateExt)
	lastTime, err := d.StateTime(stateFile)
	for {
		nextSeq := d.lastSequence + 1
		debug("[debug] Processing download for sequence", nextSeq)
		if err == nil {
			nextDiffTime := lastTime.Add(d.interval)
			if nextDiffTime.After(time.Now()) {
				// we catched up and the next diff file is in the future.
				// wait till last diff time + interval, before fetching next
				nextDiffTime = lastTime.Add(d.interval + 2*time.Second /* allow small time diff between servers */)
				waitFor := nextDiffTime.Sub(time.Now())
				debug("[debug] Waiting for next download in", waitFor)
				wait(d.ctx, waitFor)
			}
		}
		// download will retry until they succeed
		d.downloadTillSuccess(d.ctx, nextSeq, d.StateExt)
		noWait := d.downloadTillSuccess(d.ctx, nextSeq, d.FileExt)
		if d.ctx.Err() != nil {
			close(d.sequences)
			return
		}
		d.lastSequence = nextSeq
		base := path.Join(d.dest, seqPath(d.lastSequence))
		lastTime, _ = d.StateTime(base + d.StateExt)

		var latest bool
		if noWait {
			if d.download(nextSeq+1, d.StateExt) == nil {
				// next sequence is immediately available
				latest = false
			} else {
				// download of next seq failed (404 or error)
				latest = true
			}
		} else { // waited for this seq, so assume it's the latest
			latest = true
		}

		d.sequences <- replication.Sequence{
			Sequence:      d.lastSequence,
			Filename:      base + d.FileExt,
			StateFilename: base + d.StateExt,
			Time:          lastTime,
			Latest:        latest,
		}
	}
}

var _ replication.Source = &reader{}

type reader struct {
	dest         string
	FileExt      string
	StateExt     string
	lastSequence int
	StateTime    func(string) (time.Time, error)
	errWaittime  time.Duration
	sequences    chan replication.Sequence
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewReader(dest string, seq int) *reader {
	ctx, cancel := context.WithCancel(context.Background())
	r := &reader{
		dest:         dest,
		lastSequence: seq,
		sequences:    make(chan replication.Sequence, 1),
		errWaittime:  60 * time.Second,
		ctx:          ctx,
		cancel:       cancel,
	}

	return r
}

func (d *reader) Sequences() <-chan replication.Sequence {
	return d.sequences
}

func (d *reader) waitTillPresent(ctx context.Context, seq int, ext string) error {
	filename := path.Join(d.dest, seqPath(seq)+ext)
	return waitTillPresent(ctx, filename)
}

func (d *reader) Start() {
	d.fetchNextLoop()
}

func (d *reader) Stop() {
	d.cancel()
}

func (d *reader) fetchNextLoop() {
	for {
		nextSeq := d.lastSequence + 1
		if err := d.waitTillPresent(d.ctx, nextSeq, d.StateExt); err != nil {
			d.sequences <- replication.Sequence{
				Sequence: nextSeq,
				Error:    err,
			}
			wait(d.ctx, d.errWaittime)
			continue
		}
		if err := d.waitTillPresent(d.ctx, nextSeq, d.FileExt); err != nil {
			d.sequences <- replication.Sequence{
				Sequence: nextSeq,
				Error:    err,
			}
			wait(d.ctx, d.errWaittime)
			continue
		}
		if d.ctx.Err() != nil {
			close(d.sequences)
			return
		}
		d.lastSequence = nextSeq
		base := path.Join(d.dest, seqPath(d.lastSequence))
		lastTime, _ := d.StateTime(base + d.StateExt)

		latest := !d.seqIsAvailable(d.lastSequence+1, d.StateExt)
		d.sequences <- replication.Sequence{
			Sequence:      d.lastSequence,
			Filename:      base + d.FileExt,
			StateFilename: base + d.StateExt,
			Time:          lastTime,
			Latest:        latest,
		}
	}
}

func (d *reader) seqIsAvailable(seq int, ext string) bool {
	filename := path.Join(d.dest, seqPath(seq)+ext)
	_, err := os.Stat(filename)
	return err == nil
}

// waitTillPresent blocks till file is present. Returns without error if context was canceled.
func waitTillPresent(ctx context.Context, filename string) error {
	if _, err := os.Stat(filename); err == nil {
		return nil
	}

	// fsnotify does not work recursive. wait for parent dirs first (e.g. 002/134)
	parent := filepath.Dir(filename)
	if err := waitTillPresent(ctx, parent); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
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
		case <-ctx.Done():
			return nil
		case evt := <-w.Events:
			if evt.Op&fsnotify.Create == fsnotify.Create && evt.Name == filename {
				return nil
			}
		}
	}
	return nil
}
