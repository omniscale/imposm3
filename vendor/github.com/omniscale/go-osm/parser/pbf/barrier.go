package pbf

import (
	"sync"
	"sync/atomic"
)

// barrier is a struct to synchronize multiple goroutines.
// Works similar to a WaitGroup. Except:
// Calls callback function once all goroutines called doneWait().
// doneWait() blocks until the callback returns. doneWait() does not
// block after all goroutines were blocked once.
type barrier struct {
	synced     int32
	wg         sync.WaitGroup
	once       sync.Once
	callbackWg sync.WaitGroup
	callback   func()
}

func newBarrier(callback func()) *barrier {
	s := &barrier{callback: callback}
	s.callbackWg.Add(1)
	return s
}

func (s *barrier) add(delta int) {
	s.wg.Add(delta)
}

func (s *barrier) doneWait() {
	if atomic.LoadInt32(&s.synced) == 1 {
		return
	}
	s.wg.Done()
	s.wg.Wait()
	s.once.Do(s.call)
	s.callbackWg.Wait()
}

func (s *barrier) call() {
	s.callback()
	atomic.StoreInt32(&s.synced, 1)
	s.callbackWg.Done()
}
