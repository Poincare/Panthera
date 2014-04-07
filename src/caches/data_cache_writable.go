/**
* Implements a cache for the 
* writable_processor. It caches
* OP_READ_BLOCK requests and responses
*/
package caches

import (
	//go packages
	"sync"

	//local packages
	"writables"
)

type WritableDataCache struct {
	//mutexed because multiple
	//goroutines/threads will be accessing the structure
	sync.RWMutex

	CacheSize int

	//this structure is a map between OP_READ_BLOCK requests
	//and the set of BlockPacket responses as well as a header
	RpcStore []*writables.ReadPair

	//hits, misses not yet implemented
	Hits int
	Misses int

	Enabled bool
}

func NewWritableDataCache(cacheSize int) *WritableDataCache {
	w := WritableDataCache{CacheSize: cacheSize,
		Enabled: true,
		Hits: 0,
		Misses: 0}

	w.RpcStore = make([]*writables.ReadPair, 0)

	return &w
}

func (w *WritableDataCache) CurrSize() int {
	w.RLock()
	defer w.RUnlock()
	return w.currSize()
}

func (w *WritableDataCache) currSize() int {
	return len(w.RpcStore)
}

func (w *WritableDataCache) AddReadPair(pair *writables.ReadPair) {
	w.Lock()
	defer w.Unlock()

	//if the size is already at the max point, we have to remove 
	//the last element before we can insert another one
	if w.currSize() >= w.CacheSize {
		w.RpcStore = w.RpcStore[1:]
	}

	w.RpcStore = append(w.RpcStore, pair)
}

