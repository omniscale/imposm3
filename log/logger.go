package log

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"time"
)

type Logger interface {
	Println(v ...interface{})
	Printf(format string, v ...interface{})
}

var DefaultLogger *log.Logger
var defaultFilter *logFilter

type Level string

const (
	LDebug    = Level("debug")
	LProgress = Level("progress")
	LStep     = Level("step")
	LInfo     = Level("info")
	LWarn     = Level("warn")
	LError    = Level("error")
	LFatal    = Level("fatal")
)

func init() {
	defaultFilter = &logFilter{
		start:    time.Now(),
		writer:   os.Stderr,
		levels:   []Level{LDebug, LProgress, LStep, LInfo, LWarn, LError, LFatal},
		minLevel: LProgress,
	}
	defaultFilter.init()
	DefaultLogger = log.New(defaultFilter, "", 0)
}

type logFilter struct {
	start     time.Time
	writer    io.Writer
	badLevels map[Level]struct{}
	minLevel  Level
	levels    []Level
}

func (f *logFilter) SetMinLevel(lvl Level) {
	f.minLevel = lvl
	f.init()
}

func (f *logFilter) init() {
	badLevels := make(map[Level]struct{})
	for _, level := range f.levels {
		if level == f.minLevel {
			break
		}
		badLevels[level] = struct{}{}
	}
	f.badLevels = badLevels
}

func (f *logFilter) Check(line []byte) bool {

	// Check for a log level
	var level Level
	x := bytes.IndexByte(line, '[')
	if x >= 0 {
		y := bytes.IndexByte(line[x:], ']')
		if y >= 0 {
			level = Level(line[x+1 : x+y])
		}
	}

	_, ok := f.badLevels[level]
	return !ok
}

func (f *logFilter) Write(p []byte) (n int, err error) {
	if !f.Check(p) {
		return 0, nil
	}
	// The Go log package always guarantees that we only
	// get a single line.
	b := bytes.Buffer{}
	now := time.Now()

	d := now.Sub(f.start)
	fmt.Fprintf(&b, "[%s] %d:%02d:%02d ",
		now.Format(time.RFC3339),
		int(d.Hours()),
		int(math.Mod(d.Minutes(), 60)),
		int(math.Mod(d.Seconds(), 60)),
	)
	b.Write(p)

	return f.writer.Write(b.Bytes())

}

func SetMinLevel(lvl Level) {
	defaultFilter.SetMinLevel(lvl)
}

func Println(v ...interface{}) {
	DefaultLogger.Println(v...)
}

func Printf(format string, v ...interface{}) {
	DefaultLogger.Printf(format, v...)
}

func Fatal(v ...interface{}) {
	DefaultLogger.Fatal(v...)
}

func Fatalf(format string, v ...interface{}) {
	DefaultLogger.Fatalf(format, v...)
}

func Step(name string) func() {
	start := time.Now()
	Println("[step] Starting:", name)
	return func() {
		Printf("[step] Finished: %s in %s", name, time.Since(start))
	}
}
