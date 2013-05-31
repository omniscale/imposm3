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

func SetQuiet(quiet bool) {
	defaultLogBroker.SetQuiet(quiet)
}

type Logger struct {
	Component string
}

func (l *Logger) Print(args ...interface{}) {
	defaultLogBroker.Records <- Record{INFO, l.Component, fmt.Sprint(args...)}
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

func (l *Logger) Warn(args ...interface{}) {
	defaultLogBroker.Records <- Record{WARNING, l.Component, fmt.Sprint(args...)}
}

func (l *Logger) Warnf(msg string, args ...interface{}) {
	defaultLogBroker.Records <- Record{WARNING, l.Component, fmt.Sprintf(msg, args...)}
}

func (l *Logger) Printfl(level Level, msg string, args ...interface{}) {
	defaultLogBroker.Records <- Record{level, l.Component, fmt.Sprintf(msg, args...)}
}

func (l *Logger) StartStep(msg string) string {
	defaultLogBroker.StepStart <- Step{l.Component, msg}
	return msg
}

func (l *Logger) StopStep(msg string) {
	defaultLogBroker.StepStop <- Step{l.Component, msg}
}

func NewLogger(component string) *Logger {
	return &Logger{component}
}

type Step struct {
	Component string
	Name      string
}

type LogBroker struct {
	Records      chan Record
	Progress     chan string
	StepStart    chan Step
	StepStop     chan Step
	quiet        bool
	quit         chan bool
	wg           *sync.WaitGroup
	newline      bool
	lastProgress string
}

func (l *LogBroker) SetQuiet(quiet bool) {
	l.quiet = quiet
}

func (l *LogBroker) loop() {
	l.wg.Add(1)
	steps := make(map[Step]time.Time)
For:
	for {
		select {
		case record := <-l.Records:
			l.printRecord(record)
		case progress := <-l.Progress:
			if !l.quiet {
				l.printProgress(progress)
			}
		case step := <-l.StepStart:
			steps[step] = time.Now()
			l.printProgress(step.Name)
		case step := <-l.StepStop:
			startTime := steps[step]
			delete(steps, step)
			duration := time.Since(startTime)
			l.printRecord(Record{INFO, step.Component, step.Name + " took: " + duration.String()})
		case <-l.quit:
			break For
		}
	}
Flush:
	// after quit, print all records from chan
	for {
		select {
		case record := <-l.Records:
			l.printRecord(record)
		default:
			break Flush
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
	if !l.newline {
		fmt.Print(CLEARLINE)
	}
	l.printPrefix()
	l.printComponent(record.Component)
	fmt.Println(record.Message)
	l.newline = true
	if l.lastProgress != "" {
		l.printProgress(l.lastProgress)
		l.newline = false
	}
}
func (l *LogBroker) printProgress(progress string) {
	l.printPrefix()
	fmt.Print(progress)
	fmt.Print("\r")
	l.lastProgress = progress
	l.newline = false
}

func Shutdown() {
	defaultLogBroker.quit <- true
	defaultLogBroker.wg.Wait()
}

var defaultLogBroker LogBroker

func init() {
	defaultLogBroker = LogBroker{
		Records:   make(chan Record, 8),
		Progress:  make(chan string),
		StepStart: make(chan Step),
		StepStop:  make(chan Step),
		quit:      make(chan bool),
		wg:        &sync.WaitGroup{},
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
