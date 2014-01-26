package caches

import (
	//golang imports
	"sync"

	
	//local imports
	"datanode_rpc"
	"util"
)

/* Describes a basic data caching mechanism
(more or less LRU). DataNode RPC requests are 
compared with reflect.deepEquals.
*/

//note: this does not implement the request 
//cache structure for a few reasons
type DataCache struct {
	//this will likely be a global object that lots
	//of goroutines will need to access, so there is a
	//mutex on it
	sync.RWMutex

	CacheSize int

	//stores pairs of requests and responses so 
	//we can query them later
	//this is the chief difference between DataCache
	//RequestCache
	RpcStore []datanode_rpc.RequestResponse

	Hits int
	Misses int

	//set whether or not this cache is enabled
	Enabled bool
}

func NewDataCache(cache_size int) *DataCache {
	dc := DataCache{CacheSize: cache_size,
		Hits: 0,
		Misses: 0,
		Enabled: true}
	dc.RpcStore = make([]datanode_rpc.RequestResponse, 0)

	return &dc
}

func (dc *DataCache) CurrSize() int {
	return len(dc.RpcStore)
}

//return a list of the cached requests
func (dc *DataCache) CachedRequests() []datanode_rpc.DataRequest {
	res := make([]datanode_rpc.DataRequest, 0)
	for i := 0; i < len(dc.RpcStore); i++ {
		pair := dc.RpcStore[i]

		//add the request packet
		res = append(res, *pair.Request)
	}

	return res
}

//return a list of the cached responses
func (dc *DataCache) CachedResponses() []datanode_rpc.DataResponse {
	res := make([]datanode_rpc.DataResponse, 0)
	for i := 0; i<len(dc.RpcStore); i++ {
		pair := dc.RpcStore[i]
		
		//add the response packet
		res = append(res, *pair.Response)
	}

	return res
}

func (dc *DataCache) Disable() {
	dc.Lock()
	defer dc.Unlock()

	dc.Enabled = false
}

func (dc *DataCache) AddRpcPair(pair datanode_rpc.RequestResponse) {
	dc.Lock()
	defer dc.Unlock()

	dc.RpcStore = append(dc.RpcStore, pair)

	//check if the buffer is going past the size limit
	if len(dc.RpcStore) > dc.CacheSize {
		//get rid of the first thing in the list
		//which should have been added first and not used
		//since
		//NOTE it is very important to make sure that when an 
		//item is used from the cache, it is moved to the back
		//of the list, otherwise this scheme will fall apart
		dc.RpcStore = dc.RpcStore[1:]
	}
}

//use a request object to get the corresponding response object
func (dc *DataCache) Query(req datanode_rpc.DataRequest) *datanode_rpc.DataResponse {

	//lock for reading; can have multiple readers, can't
	//have multiple writers
	dc.RLock()

	//defer the reader unlock
	defer dc.RUnlock()

	for i := 0; i < len(dc.RpcStore); i++ {
		pair := dc.RpcStore[i]
		util.DataReqLogger.Println("Pair request: ", *pair.Request)
		util.DataReqLogger.Println("Req:          ", req)
		//NOTE: this is an important bit of this file.
		//it compares two requests with a complete deepEquals,
		//so if the request packet has a different client 
		//signing, etc. the cache will not produce a hit
		if pair.Request.Equals(&req) {
			return pair.Response
		}
	}
	return nil
}

//clears the cache
func (dc *DataCache) Clear() {
	dc.Lock()
	defer dc.Unlock()

	//clear the entire array
	dc.RpcStore = make([]datanode_rpc.RequestResponse, 0)
}









