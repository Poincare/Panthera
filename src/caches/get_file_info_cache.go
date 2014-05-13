package caches

/* this file implements a cache for the GetFileInfo() call to the NameNode.
Essentially, a GetFileInfo call's results are not typically changed from call
to call, so the cache layer can respond instead of having to go to the server.
*/

import (
	//local packages
	"namenode_rpc"
	"util"
)

type GetFileInfoCache struct {
	//past requests received by this cache
	Cache *RequestCache

	//set whether or the cache is enabled
	enabled bool
}

//constructor
func NewGetFileInfoCache(cache_size int) *GetFileInfoCache {
	gf := GetFileInfoCache{}
	gf.Cache =  NewRequestCache(cache_size)
	gf.enabled = true
	return &gf
}

func (gfi_cache *GetFileInfoCache) Disable() {
	gfi_cache.enabled = false

	//we have to set the RequestCache to the correct
	//value as well
	gfi_cache.Cache.Enabled = false
}

//Query the cache. Returns nil if req is not found in the cache or the Enabled is set to 
//false.
//gfi_cache.Cache.Query should NOT be called since it suses the wrong kind of equality 
//comparator
func (gfi_cache *GetFileInfoCache) Query(
	req namenode_rpc.ReqPacket) namenode_rpc.ResponsePacket {
	
	util.DebugLogger.Println("in GetFileInfoCache.Query()")
	//if the cache is not enabled, we keep returning nil
	if !gfi_cache.enabled {
		return nil
	}

	equals := EqualityFunc(func(req1 namenode_rpc.ReqPacket, req2 namenode_rpc.ReqPacket) 
	bool {
		//the conditions mean the directory being queried and the method called
		//are the same
		methodName1 := string(req1.GetMethodName())
		methodName2 := string(req2.GetMethodName())
		dir1 := string(req1.GetParameter(0).Value)
		dir2 := string(req2.GetParameter(0).Value)

		if methodName1 == methodName2 && dir1 == dir2 {
			return true
		}
		return false
	})

	util.DebugLogger.Println("Defined Equals method.")

	res := gfi_cache.Cache.QueryCustom(req, equals)
	util.DebugLogger.Println("Done querying cache.")

	if res == nil {
		return nil
	}

	return res
}

