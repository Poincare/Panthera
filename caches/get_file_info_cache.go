package caches

/* this file implements a cache for the GetFileInfo() call to the NameNode.
Essentially, a GetFileInfo call's results are not typically changed from call
to call, so the cache layer can respond instead of having to go to the server.
*/

import (
	"namenode_rpc"
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

//Query the cache. Returns nil if req is not found in the cache or the Enabled is set to false.
func (gfi_cache *GetFileInfoCache) Query(req namenode_rpc.ReqPacket) namenode_rpc.ResponsePacket {
	//if the cache is not enabled, we keep returning nil
	if !gfi_cache.enabled {
		return nil
	}

	res := gfi_cache.Cache.Query(req)
	if res == nil {
		return nil
	}
	return res
}

