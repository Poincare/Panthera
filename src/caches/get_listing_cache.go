package caches

/*
* Caches getListing calls made to the HDFS server.
* (dfs -ls makes these in order to get a directory
* listing)
*/

import (
	"namenode_rpc"
)

type GetListingCache struct {
	Cache *RequestCache

	enabled bool
}

func NewGetListingCache(cacheSize int) *GetListingCache {
	glc := GetListingCache{}
	glc.Cache = NewRequestCache(cacheSize)
	glc.enabled = true
	return &glc
}

func (glc *GetListingCache) IsEnabled() bool {
	return glc.enabled
}

func (glc *GetListingCache) Disable() {
	glc.enabled = false
	glc.Cache.Disable()
}


func (glc *GetListingCache) Query(req namenode_rpc.ReqPacket) namenode_rpc.ResponsePacket {
	//if the cache is not enabled, we keep returning nil
	if !glc.IsEnabled() {
		return nil
	}

	equals := EqualityFunc(func(req1 namenode_rpc.ReqPacket, req2 namenode_rpc.ReqPacket) bool {
		//the conditions mean the directory being queried and the method called
		//are the same
		methodName1 := string(req1.GetMethodName())
		methodName2 := string(req2.GetMethodName())
		dir1 := string(req1.GetParameter(0).Value)
		dir2 := string(req2.GetParameter(0).Value)
		opt1 := string(req1.GetParameter(1).Value)
		opt2 := string(req1.GetParameter(1).Value)

		if methodName1 == methodName2 && dir1 == dir2 && opt1 == opt2 {
			return true
		}
		return false
	})

	res := glc.Cache.QueryCustom(req, equals)

	if res == nil {
		return nil
	}
	return res
}