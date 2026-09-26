package cache

import (
	"bytes"
	"testing"
)

func TestSyncBarrierPreservesAllCacheStores(t *testing.T) {
	dir := t.TempDir()
	osmCache := NewOSMCache(dir)
	if err := osmCache.Open(); err != nil {
		t.Fatal(err)
	}
	defer osmCache.Close()
	diffCache := NewDiffCache(dir)
	if err := diffCache.Open(); err != nil {
		t.Fatal(err)
	}
	defer diffCache.Close()
	stores := func() []*cache {
		return []*cache{
			&osmCache.Coords.cache, &osmCache.Nodes.cache, &osmCache.Ways.cache, &osmCache.Relations.cache,
			&diffCache.Coords.cache, &diffCache.CoordsRel.cache, &diffCache.Ways.cache,
		}
	}
	key := idToKeyBuf(1234)
	value := []byte("raw storage fixture")
	for _, c := range stores() {
		if err := c.db.Put(c.wo, key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := SyncBarrier(osmCache, diffCache); err != nil {
		t.Fatal(err)
	}
	osmCache.Close()
	diffCache.Close()
	if err := osmCache.Open(); err != nil {
		t.Fatal(err)
	}
	if err := diffCache.Open(); err != nil {
		t.Fatal(err)
	}
	for i, c := range stores() {
		data, err := c.db.Get(c.ro, key)
		if err != nil || !bytes.Equal(data, value) {
			t.Fatalf("store %d changed after sync/reopen: %q %v", i, data, err)
		}
		it := c.db.NewIterator(c.ro)
		count := 0
		for it.SeekToFirst(); it.Valid(); it.Next() {
			count++
		}
		err = it.GetError()
		it.Close()
		if err != nil || count != 1 {
			t.Fatalf("sync introduced extra keys in store %d: count=%d err=%v", i, count, err)
		}
	}
}
