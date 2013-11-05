package caches

/* this file implements a generic, semi-LRU cache that other 
specific cache systems use */

import (
	"namenode_rpc"
	"reflect"
	"errors"
	//"fmt"
)

type PacketNumber uint32

type RequestCache struct {
	CacheSize int

	/*
	//TODO figure out whether to use a slice or a vector here
	//because the latency caused by traversing this is *extremely*
	//important
	Packets []namenode_rpc.RequestPacket

	//corresponding to the Packets listed above
	ResponsePackets []namenode_rpc.ResponsePacket */

	//maps packet numbers to request-response pairs
	//the whole concept runs on the "fact" that a request response 
	//pair will always have the same packet number with respect to the
	//NameNode
	RequestResponse map[PacketNumber](namenode_rpc.PacketPair)

	//records the order in which packet numbers were placed in RequestResponse
	PacketNumbers []PacketNumber

	Hits int
	Misses int
}

func NewRequestCache(cache_size int) *RequestCache {
	rs := RequestCache{}
	rs.RequestResponse = make(map[PacketNumber](namenode_rpc.PacketPair))
	rs.CacheSize = cache_size
	return &rs
}

func (rc *RequestCache) Add(rp namenode_rpc.ReqPacket, resp namenode_rpc.ResponsePacket) error {
	//the central assumption does not hold if 
	//we do not have equal packet numbers
	if rp.GetPacketNumber() != resp.GetPacketNumber() {
		return errors.New("Packet numbers are not equal for request and response")
	}

	pp := namenode_rpc.NewPacketPair();
	pp.Request = rp;
	pp.Response = resp;

	packetNum := PacketNumber(rp.GetPacketNumber())
	rc.RequestResponse[packetNum] = *pp
	rc.PacketNumbers = append(rc.PacketNumbers, packetNum)

	//we have to get rid of something from the beginning of the list
	//since that is supposed to have been inserted earlier
	if len(rc.RequestResponse) > rc.CacheSize {
		//get the least recently used packet number
		lruPacketNumber := rc.PacketNumbers[0]

		delete(rc.RequestResponse, lruPacketNumber)

		//get rid of that packet number
		rc.PacketNumbers = rc.PacketNumbers[1:]
	}
	
	return nil
}

func (rc *RequestCache) Clear() {
	rc.RequestResponse = make(map[PacketNumber](namenode_rpc.PacketPair))
}

//TODO optimize this function somehow?
//TODO returns nil if nothing is found, is that a bad idea?
func (rc *RequestCache) Query(rp namenode_rpc.ReqPacket) namenode_rpc.ResponsePacket {
	for packetNum, _ := range rc.RequestResponse {
		if reflect.DeepEqual(rc.RequestResponse[packetNum].Request, rp) {
			rc.Hits += 1
			return rc.RequestResponse[packetNum].Response
		}	
	}

	rc.Misses += 1
	return nil
} 