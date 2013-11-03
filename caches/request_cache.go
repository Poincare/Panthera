package caches

/* this file implements a generic, semi-LRU cache that other 
specific cache systems use */

import (
	"namenode_rpc"
	"reflect"
	//"fmt"
)

type RequestCache struct {
	CacheSize int

	//TODO figure out whether to use a slice or a vector here
	//because the latency caused by traversing this is *extremely*
	//important
	Packets []namenode_rpc.RequestPacket

	//corresponding to the Packets listed above
	ResponsePackets []namenode_rpc.ResponsePacket
	Hits int
	Misses int
}

func NewRequestCache(cache_size int) *RequestCache {
	rs := RequestCache{}
	rs.CacheSize = cache_size
	return &rs
}

func (rc *RequestCache) Add(rp *namenode_rpc.RequestPacket, resp namenode_rpc.ResponsePacket) {
	rc.Packets = append(rc.Packets, *rp)
	rc.ResponsePackets = append(rc.ResponsePackets, resp)

	//we have to get rid of something from the beginning of the list
	//since that is supposed to have been inserted earlier
	if len(rc.Packets) > rc.CacheSize {

		//have to keep the len(Packets) = len(ResponsePackets)
		rc.Packets = rc.Packets[1:]
		rc.ResponsePackets = rc.ResponsePackets[1:]
	}
}

func (rc *RequestCache) Clear() {
	rc.Packets = make([]namenode_rpc.RequestPacket, 0)
	rc.ResponsePackets = make([]namenode_rpc.ResponsePacket, 0)
}

//TODO optimize this function somehow?
//TODO returns nil if nothing is found, is that a bad idea?
func (rc *RequestCache) Query(rp *namenode_rpc.RequestPacket) namenode_rpc.ResponsePacket {
	for i := 0; i<len(rc.Packets); i++ {
		//TODO maybe don't use reflect here if we need to speed it up
		if reflect.DeepEqual(*rp, rc.Packets[i]) {
			rc.Hits += 1
			return rc.ResponsePackets[i]
		}
	}
	rc.Misses += 1
	return nil
}