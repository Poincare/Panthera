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
	PastRequests []namenode_rpc.RequestPacket
	CacheSize int
}

func NewGetFileInfoCache(cache_size int) *GetFileInfoCache {
	gf := GetFileInfoCache{}
	gf.PastRequests = make([]namenode_rpc.RequestPacket, cache_size)
	gf.CacheSize = cache_size
	return &gf
}