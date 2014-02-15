package caches

/* this file implements a generic, semi-LRU cache that other 
specific cache systems use */

import (
	"namenode_rpc"
	"reflect"
	"errors"
	//"fmt"
	"sync"
)

type PacketNumber uint32

type RequestCache struct {
	//notice that the Cache is operated with a 
	//mutex in order to make sure that different hdfs_requests.Processor
	//instances do not run over each other in trying to read/write from/to
	//RequestCache instances
	sync.RWMutex

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

	//set whether or not this cache is enabled
	Enabled bool
}

func NewRequestCache(cache_size int) *RequestCache {
	rs := RequestCache{}
	rs.RequestResponse = make(map[PacketNumber](namenode_rpc.PacketPair))
	rs.CacheSize = cache_size
	rs.Enabled = true
	return &rs
}

//disables the entire cache
func (rc *RequestCache) Disable() {
	rc.Lock()
	defer rc.Unlock()

	rc.Enabled = false
}

//this a private method because it assumes that the mutex has already been locked
func (rc *RequestCache) add(rp namenode_rpc.ReqPacket, resp namenode_rpc.ResponsePacket) error {

	//the central assumption does not hold if 
	//we do not have equal packet numbers
	if rp != nil && resp != nil {
		if rp.GetPacketNumber() != resp.GetPacketNumber() {
			return errors.New("Packet numbers are not equal for request and response")
		}
	}

	pp := namenode_rpc.NewPacketPair();
	pp.Request = rp;
	pp.Response = resp;

	packetNum := PacketNumber(0)
	if rp != nil {
		packetNum = PacketNumber(rp.GetPacketNumber())
	} else {
		packetNum = PacketNumber(resp.GetPacketNumber())
	}

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

func (rc *RequestCache) Add(rp namenode_rpc.ReqPacket, resp namenode_rpc.ResponsePacket) error {
	rc.Lock()
	//unlock the mutex after done w/ processing this function
	defer rc.Unlock()

	return rc.add(rp, resp)
}

//same as Add() but adds a PacketPair with a nil in place of the response packet
func (rc *RequestCache) AddRequest(req namenode_rpc.ReqPacket) error {
	rc.Lock()
	defer rc.Unlock()
	return rc.add(req, nil)
}

//adds a response to the packet pair that was partially filled with AddRequest
//HOWEVER, this assumes that the bucket already has a Request inside it (i.e we
//can't have a Response before a Request)
func (rc *RequestCache) AddResponse(resp namenode_rpc.ResponsePacket) error {
	rc.Lock()
	defer rc.Unlock()

	req := rc.RequestResponse[PacketNumber(resp.GetPacketNumber())].Request
	return rc.add(req, resp)
}

func (rc *RequestCache) Clear() {
	rc.Lock()
	defer rc.Unlock()
	rc.RequestResponse = make(map[PacketNumber](namenode_rpc.PacketPair))
}

//TODO optimize this function somehow?
//TODO returns nil if nothing is found, is that a bad idea?
func (rc *RequestCache) Query(rp namenode_rpc.ReqPacket) namenode_rpc.ResponsePacket {
	rc.RLock()
	defer rc.RUnlock()

	//use the reflect.DeepEqual as the default
	//equality comparator
	equals := EqualityFunc(func(rp1 namenode_rpc.ReqPacket, rp2 namenode_rpc.ReqPacket) bool {
			return reflect.DeepEqual(rp1, rp2)
		})

	return rc.queryCustom(rp, equals)
}

//this function is like the Query() function, but takes an
//equality method (i.e. whether or not two request packets are
//equal to one another). Different cache types can use
//different equality measures
type EqualityFunc func(namenode_rpc.ReqPacket, namenode_rpc.ReqPacket) bool

func (rc *RequestCache) queryCustom(rp namenode_rpc.ReqPacket, equals EqualityFunc) namenode_rpc.ResponsePacket {
	if !rc.Enabled {
		return nil
	}

	for packetNum, _ := range rc.RequestResponse {
		isEqual := equals(rc.RequestResponse[packetNum].Request, rp)
		if isEqual {
			rc.Hits += 1
			return rc.RequestResponse[packetNum].Response
		}
	}

	rc.Misses += 1
	return nil	
}

func (rc *RequestCache) QueryCustom(rp namenode_rpc.ReqPacket, equals EqualityFunc) namenode_rpc.ResponsePacket {
	rc.RLock()
	defer rc.RUnlock()

	//call the private method, which assumes that the lock has been
	//obtained already
	return rc.queryCustom(rp, equals)
}

func (rc *RequestCache) HasPacketNumber(packetNum PacketNumber) bool {
	rc.RLock()
	defer rc.RUnlock()
	_, present := rc.RequestResponse[packetNum]
	return present
}