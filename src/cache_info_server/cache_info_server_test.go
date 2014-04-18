package cache_info_server

import (
	//go packages
	"testing"

	//local packages
	"caches"
)

var setupDone bool = false
var port string = "1337"
var dataCache *caches.WritableDataCache
var dataCacheSize int = 15

func setup() {
	setupDone = true
	dataCache = caches.NewWritableDataCache(dataCacheSize)
}

func TestNewCacheInfoServer(t *testing.T) {
	port := "1337"
	d := caches.NewWritableDataCache(15)
	c := NewCacheInfoServer(port, d)
	if c == nil {
		t.Fail()
	}

	if c.Port != port {
		t.Fail()
	}

	if c.DataCache != d {
		t.Fail()
	}
}

func TestCacheInfoServerStart(t *testing.T) {
	if !setupDone {
		setup()
	}

	c := NewCacheInfoServer(port, dataCache)
	go c.Start()
}