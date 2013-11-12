package util

import (
	"testing"
)

func TestSyncPoint(t *testing.T) {
	done := make(chan bool)
	check := int32(0)
	sp := NewSyncPoint(2, func() {
		done <- true
		check = 1
	})

	wait := func() {
		if check != 0 {
			panic("check set")
		}
		sp.Sync()
		if check != 1 {
			panic("check not set")
		}
	}
	go wait()
	go wait()

	<-done

	// does not wait/block
	sp.Sync()

}
