package caches

import (
	//go packages
	"testing"

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
}