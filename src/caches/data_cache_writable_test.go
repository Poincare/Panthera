package caches

import (
	//go packages
	"testing"
	"fmt"

	//local packages
	"writables"
)

var cache *WritableDataCache
var cacheSize int = 10

var request *writables.ReadBlockHeader

var pair *writables.ReadPair

func setupWDC() {
	cache = NewWritableDataCache(cacheSize)

	request = writables.NewReadBlockHeader()
	pair = writables.NewReadPair(request)
}

func TestNewWDC(t *testing.T) {
	setupWDC()
	if cache == nil {
		t.Fail()
	}

	if cache.RpcStore == nil {
		t.Fail()
	}

	if cache.CacheSize != cacheSize {
		t.Fail()
	}
}

func TestAddReadPair(t *testing.T) {
	setupWDC()

	cache.AddReadPair(pair)
	if cache.CurrSize() != 1 {
		t.Fail()
	}

	if cache.RpcStore[0] != pair {
		t.Fail()
	}

	//add 15 more, then the cache should 
	//consistently discard items and keep
	//the number of items at cacheSize
	for i := 0; i < cacheSize+45; i++ {
		cache.AddReadPair(pair)
	}

	if cache.CurrSize() != 10 {
		fmt.Println("Current size doesn't match, expected 1, got: ", 
			cache.CurrSize())
		t.Fail()
	}

	if cache.RpcStore[0] != pair {
		t.Fail()
	}
}

func TestWDCQuery(t *testing.T) {
	setupWDC()

	cache.AddReadPair(pair)
	resPair := cache.Query(pair.Request)

	//nil => not found
	if resPair == nil {
		t.Fail()
	}

	if resPair != pair {
		t.Fail()
	}
}

func TestWDCAddBlockPacket(t *testing.T) {
	setupWDC()

	cache.AddReadPair(pair)
	header := writables.NewBlockResponseHeader()
	bp := writables.NewBlockPacket(header)
	cache.AddBlockPacket(request, bp)

	resPair := cache.Query(pair.Request)
	if resPair.ResponseSet.Chunks[0] != bp {
		t.Fail()
	}
}