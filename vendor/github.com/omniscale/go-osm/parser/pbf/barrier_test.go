package pbf

import (
	"testing"
)

func TestBarrier(t *testing.T) {
	done := make(chan bool)
	check := int32(0)
	bar := newBarrier(func() {
		done <- true
		check = 1
	})
	bar.add(2)

	wait := func() {
		if check != 0 {
			panic("check set")
		}
		bar.doneWait()
		if check != 1 {
			panic("check not set")
		}
	}
	go wait()
	go wait()

	<-done

	// does not wait/block
	bar.doneWait()
}
