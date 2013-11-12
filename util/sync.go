package util

import (
	"sync"
)

type SyncPoint struct {
	synced     bool
	wg         sync.WaitGroup
	once       sync.Once
	callbackWg sync.WaitGroup
	callback   func()
}

func NewSyncPoint(n int, callback func()) *SyncPoint {
	s := &SyncPoint{callback: callback}
	s.wg.Add(n)
	s.callbackWg.Add(1)
	return s
}

func (s *SyncPoint) Sync() {
	if s.synced {
		return
	}
	s.wg.Done()
	s.wg.Wait()
	s.once.Do(s.Call)
	s.callbackWg.Wait()
}

func (s *SyncPoint) Call() {
	s.callback()
	s.synced = true
	s.callbackWg.Done()
}
