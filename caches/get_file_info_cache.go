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
}

//constructor
func NewGetFileInfoCache(cache_size int) *GetFileInfoCache {
	gf := GetFileInfoCache{}
	gf.Cache =  NewRequestCache(cache_size)
	return &gf
}

//Query the cache. Returns nil if req is not found in the Cache.
func (gfi_cache *GetFileInfoCache) Query(req *namenode_rpc.RequestPacket) namenode_rpc.ResponsePacket {
	res := gfi_cache.Cache.Query(req)
	if res == nil {
		return nil
	}
	return res
}

