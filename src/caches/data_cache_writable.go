/**
* Implements a cache for the 
* writable_processor. It caches
* OP_READ_BLOCK requests and responses
*/
package caches

import (
	"sync"
)

type WritableDataCache struct {
	//mutexed because multiple
	//goroutines/threads will be accessing the structure
	sync.RWMutex

	CacheSize int

	//this structure is a map between OP_READ_BLOCK requests
	//and the set of BlockPacket responses as well as a header

	//INCOMPLETE
	RpcStore []
}