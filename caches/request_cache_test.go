package caches

import (
	"testing"
	"namenode_rpc"
	"reflect"
	"fmt"
)

func TestRequestCacheConstructor(t *testing.T) {
	rs := NewRequestCache(15)
	if rs.CacheSize != 15 {
		t.Fail()
	}
}

//TODO revise structure of this unit test
func TestRequestCacheAdd(t *testing.T) {
	rs := NewRequestCache(2)

	resp := namenode_rpc.NewGetFileInfoResponse()
	rp := namenode_rpc.NewRequestPacket()

	rs.Add(rp, resp)

	if len(rs.RequestResponse) != 1 {
		fmt.Println("Failed length test, length: ", len(rs.RequestResponse))
		t.Fail()
	}

	//check if the packet we put in the array
	//is the same packet that we loaded up
	if !reflect.DeepEqual(rs.RequestResponse[0].Request, rp) {
		fmt.Println("Failed packets test")
		t.Fail()
	}

	//add two more request packets, check for overflow
	rp = namenode_rpc.NewRequestPacket()
	rp.PacketNumber = 1
	resp.PacketNumber = 1
	rs.Add(rp, resp)

	rp = namenode_rpc.NewRequestPacket()
	rp.PacketNumber = 2
	resp.PacketNumber = 2
	rs.Add(rp, resp)

	if len(rs.RequestResponse) != 2 {
		fmt.Println("Failed overflow test, length of RequestResponse: ", len(rs.RequestResponse))
		t.Fail()
	}

	if !reflect.DeepEqual(rs.RequestResponse[2].Request, rp) {
		fmt.Println("Failed comparison: ", rs.RequestResponse[2], rp)
		t.Fail()
	}
}

func TestRequestCacheClear(t *testing.T) {
	rs := NewRequestCache(2)
	rp := namenode_rpc.NewRequestPacket()
	resp := namenode_rpc.NewGetFileInfoResponse()
	rs.Add(rp, resp)
	rs.Clear()

	if len(rs.RequestResponse) != 0 {
		t.Fail()
	}
}


func TestRequestCacheQuery(t *testing.T) {
	rc := NewRequestCache(1)
	rp := namenode_rpc.NewRequestPacket()
	resp := namenode_rpc.NewGetFileInfoResponse()
	rc.Add(rp, resp)

	if resp == nil {
		t.Fail()
	}

	//TODO this dereferencing going on here is too complicated
	if !reflect.DeepEqual(resp, rc.Query(rp)) {
		fmt.Println("Failed query result: ", rc.Query(rp))
		fmt.Println("First response packet: ", rc.RequestResponse[PacketNumber(resp.GetPacketNumber())].Response)
		fmt.Println("Expected: ", *resp)
		t.Fail()
	}
}


func TestRequestCacheHitMiss(t *testing.T) {
	rc := NewRequestCache(1)
	rp := namenode_rpc.NewRequestPacket()
	resp := namenode_rpc.NewGetFileInfoResponse()
	rc.Add(rp, resp)

	rc.Query(rp)

	rp = namenode_rpc.NewRequestPacket()
	rp.Length = 500

	rc.Query(rp)

	if rc.Misses != 1 {
		fmt.Println("incorrect misses: ", rc.Misses)
		t.Fail()
	}

	if rc.Hits != 1 {
		fmt.Println("incorrect hits: ", rc.Hits)
		t.Fail()
	}
} 

