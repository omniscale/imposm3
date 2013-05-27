package logging

import (
	"fmt"
	"sync"
	"time"
)

type Level int

const (
	FATAL Level = iota
	ERROR
	WARNING
	INFO
	DEBUG
)

type Record struct {
	Level     Level
	Component string
	Message   string
}

const (
	CLEARLINE = "\x1b[2K"
)

func Debugf(msg string, args ...interface{}) {
	defaultLogBroker.Records <- Record{DEBUG, "", fmt.Sprintf(msg, args...)}
}

func Infof(msg string, args ...interface{}) {
	defaultLogBroker.Records <- Record{INFO, "", fmt.Sprintf(msg, args...)}
}

func Warnf(msg string, args ...interface{}) {
	defaultLogBroker.Records <- Record{WARNING, "", fmt.Sprintf(msg, args...)}
}

func Errorf(msg string, args ...interface{}) {
	defaultLogBroker.Records <- Record{ERROR, "", fmt.Sprintf(msg, args...)}
}

func Fatalf(msg string, args ...interface{}) {
	defaultLogBroker.Records <- Record{FATAL, "", fmt.Sprintf(msg, args...)}
}

func Progress(msg string) {
	defaultLogBroker.Progress <- msg
}

type Logger struct {
	Component string
}

func (l *Logger) Printf(msg string, args ...interface{}) {
	defaultLogBroker.Records <- Record{INFO, l.Component, fmt.Sprintf(msg, args...)}
}

func (l *Logger) Fatal(args ...interface{}) {
	defaultLogBroker.Records <- Record{FATAL, l.Component, fmt.Sprint(args...)}
}

func (l *Logger) Fatalf(msg string, args ...interface{}) {
	defaultLogBroker.Records <- Record{FATAL, l.Component, fmt.Sprintf(msg, args...)}
}

func (l *Logger) Errorf(msg string, args ...interface{}) {
	defaultLogBroker.Records <- Record{ERROR, l.Component, fmt.Sprintf(msg, args...)}
}

func (l *Logger) Printfl(level Level, msg string, args ...interface{}) {
	defaultLogBroker.Records <- Record{level, l.Component, fmt.Sprintf(msg, args...)}
}

func NewLogger(component string) *Logger {
	return &Logger{component}
}

type LogBroker struct {
	Records  chan Record
	Progress chan string
	quit     chan bool
	wg       *sync.WaitGroup
}

func (l *LogBroker) loop() {
	l.wg.Add(1)
	newline := true
	lastProgress := ""
For:
	for {
		select {
		case record := <-l.Records:
			if !newline {
				fmt.Print(CLEARLINE)
			}
			l.printRecord(record)
			newline = true
			if lastProgress != "" {
				l.printProgress(lastProgress)
				newline = false
			}
		case progress := <-l.Progress:
			l.printProgress(progress)
			lastProgress = progress
			newline = false
		case <-l.quit:
			break For
		}
	}
	l.wg.Done()
}

func (l *LogBroker) printPrefix() {
	fmt.Print("[", time.Now().Format(time.Stamp), "] ")
}
func (l *LogBroker) printComponent(component string) {
	if component != "" {
		fmt.Print("[", component, "] ")
	}
}

func (l *LogBroker) printRecord(record Record) {
	l.printPrefix()
	l.printComponent(record.Component)
	fmt.Println(record.Message)
}
func (l *LogBroker) printProgress(progress string) {
	l.printPrefix()
	fmt.Print(progress)
}

func Shutdown() {
	defaultLogBroker.quit <- true
	defaultLogBroker.wg.Wait()
}

var defaultLogBroker LogBroker

func init() {
	defaultLogBroker = LogBroker{
		Records:  make(chan Record, 8),
		Progress: make(chan string),
		quit:     make(chan bool),
		wg:       &sync.WaitGroup{},
	}
	go defaultLogBroker.loop()
}

// func init() {
// 	go func() {
// 		log := NewLogger("Tourette")
// 		for {
// 			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
// 			log.Printfl(Level(rand.Intn(5)), "Bazinga")
// 		}
// 	}()
// }
