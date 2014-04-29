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
	if !w.Enabled {
		return 0
	}
	return len(w.RpcStore)
}

func (w *WritableDataCache) AddReadPair(pair *writables.ReadPair) {
	w.Lock()
	defer w.Unlock()

	if !w.Enabled {
		return
	}

	//see if the pair is already in the cache
	resPair := w.Query(pair.Request)
	//if it is already in the cache, we do not
	//need to add it again
	if resPair != nil {
		return
	}

	//if the size is already at the max point, we have to remove 
	//the last element before we can insert another one
	if w.currSize() >= w.CacheSize {
		w.RpcStore = w.RpcStore[1:]
	}

	w.RpcStore = append(w.RpcStore, pair)
}

//return a pair by providing a ReadBlockHeader
func (w *WritableDataCache) Query(
	toFind *writables.ReadBlockHeader) *writables.ReadPair {

	if !w.Enabled {
		return nil
	}

	//iterate through the availbale pairs
	//and check if any of the requests matches toFind
	for i := 0; i < w.currSize(); i++ {
		pair := w.RpcStore[i]
		if pair.Request.Equals(toFind) {
			return pair
		}
	}

	return nil
}

//add a BlockPacket to the pair in RpcStore that contains
//header argument as the "Request" field.
func (w *WritableDataCache) AddBlockPacket(
	header *writables.ReadBlockHeader,
	blockPacket *writables.BlockPacket) {

	if !w.Enabled {
		return
	}

	//get the pair that the header is in
	pair := w.Query(header)

	//ad the BlockPacket to that pair
	pair.AddBlockPacket(blockPacket)
}
