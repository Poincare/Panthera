package hdfs_requests

import (
	"testing"
	"namenode_rpc"
	"reflect"
	"fmt"
	"caches"
	"configuration"
)

var eventChan chan ProcessorEvent = make(chan ProcessorEvent)
var cacheSet = caches.NewCacheSet()
var p *Processor

//set up the test before running it
func init() {
	//the cache size doesn't matter for this test suite
	gfiCacheSize := 15
	cacheSet.GfiCache = caches.NewGetFileInfoCache(gfiCacheSize)
	cacheSet.GetListingCache = caches.NewGetListingCache(gfiCacheSize)
	
	dataNode := configuration.NewDataNodeLocation("127.0.0.1", "1389")
	dataNodeList := make([]*configuration.DataNodeLocation, 0)
	dataNodeList = append(dataNodeList, dataNode)
	portOffset := 2010
	dataNodeMap := configuration.MakeDataNodeMap(dataNodeList, portOffset)

	p = NewProcessor(eventChan, cacheSet, &dataNodeMap)
}

func TestNewProcess(t *testing.T) {
	if p == nil {
		t.FailNow()
	}

	if p.EventChannel == nil {
		t.FailNow()
	}
	fmt.Println("Finished NewProcess.");
}

//GFI request
var RequestPacketTestCase []byte = []byte{0, 0, 0, 60, 0, 0, 0, 2, 0, 10, 103, 101, 116, 
		76, 105, 115, 116, 105, 110, 103, 0, 0, 0, 2, 0, 16, 106, 
		97, 118, 97, 46, 108, 97, 110, 103, 46, 83, 116, 114, 105, 
		110, 103, 0, 12, 47, 117, 115, 101, 114, 47, 104, 100, 117, 
		115, 101, 114, 0, 2, 91, 66, 0, 0, 0, 0}

func TestMapRequest(t *testing.T) {
	req := namenode_rpc.NewRequestPacket()
	req.Load(RequestPacketTestCase)

	p.MapRequest(req)

	if !reflect.DeepEqual(p.RequestResponse[PacketNumber(req.PacketNumber)].Request, req) {
		fmt.Println("Not equal: ", p.RequestResponse[PacketNumber(req.PacketNumber)].Request, req)
		t.Fail()
	}
	fmt.Println("Finished TestMapRequest.");
}

var GetFileInfoResponseTestCase []byte = []byte{0,0,0,1,0,0,0,0,0,46,111,114,103,
	46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,100,102,115,46,112,114,
	111,116,111,99,111,108,46,72,100,102,115,70,105,108,101,83,116,97,116,117,115,0,
	46,111,114,103,46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,100,102,
	115,46,112,114,111,116,111,99,111,108,46,72,100,102,115,70,105,108,101,83,116,97,
	116,117,115,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,1,65,230,55,105,95,
	0,0,0,0,0,0,0,0,1,237,6,104,100,117,115,101,114,10,115,117,112,101,114,103,114,111,
	117,112}

func TestMapResponse(t *testing.T) {
	resp := namenode_rpc.NewGetFileInfoResponse()
	resp.Load(GetFileInfoResponseTestCase)

	p.MapResponse(resp)

	packetNumber := PacketNumber(resp.GetPacketNumber())

	if !reflect.DeepEqual(p.RequestResponse[packetNumber].Response, resp) {
		fmt.Println("Not equal: ")
		fmt.Println(p.RequestResponse[packetNumber])
		fmt.Println(resp)
		t.Fail()
	}
	fmt.Println("Finished TestMapResponse.");
}

func TestMap(t *testing.T) {
	resp := namenode_rpc.NewGetFileInfoResponse()
	resp.Load(GetFileInfoResponseTestCase)

	req := namenode_rpc.NewRequestPacket()
	req.Load(RequestPacketTestCase)

	//we're sideloading the packet number to make it match up
	req.PacketNumber = 1

	p.MapRequest(req)
	p.MapResponse(resp)

	pair := p.RequestResponse[PacketNumber(req.PacketNumber)]
	if !reflect.DeepEqual(pair.Response, resp) {
		fmt.Println("Not equal: ", pair.Response, resp)
		t.Fail()
	}

	if !reflect.DeepEqual(pair.Request, req) {
		t.Fail()
	}
	fmt.Println("Finished TestMap.");
}

func TestCacheRequestWithCacheSize (t *testing.T) {

	req := namenode_rpc.NewRequestPacket()
	req.Load(RequestPacketTestCase)

	fmt.Println("Caching request...");
	p.CacheRequest(req)
	fmt.Println("Cached Request.");

	fmt.Println("iffy");
	if len(p.cacheSet.GetListingCache.Cache.RequestResponse) != 1 {
		fmt.Println("Got size: ", len(p.cacheSet.GfiCache.Cache.RequestResponse), "cache structure: ", p.cacheSet.GfiCache.Cache.RequestResponse)
		t.Fail()
	}
	fmt.Println("Endif");

	fmt.Println("Finished CacheRequestWithCacheSize");
}

//a block report test case
var BlockReportRequestTestCase = []byte{0,0,0,254,0,0,0,5,0,11,98,108,111,99,107,82,101,112,111,114,116,0,0,0,2,0,59,111,114,103,46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,100,102,115,46,115,101,114,118,101,114,46,112,114,111,116,111,99,111,108,46,68,97,116,97,110,111,100,101,82,101,103,105,115,116,114,97,116,105,111,110,0,59,111,114,103,46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,100,102,115,46,115,101,114,118,101,114,46,112,114,111,116,111,99,111,108,46,68,97,116,97,110,111,100,101,82,101,103,105,115,116,114,97,116,105,111,110,0,14,49,50,55,46,48,46,48,46,49,58,49,51,56,57,0,41,68,83,45,54,55,56,48,48,50,48,54,49,45,49,50,55,46,48,46,49,46,49,45,49,51,56,57,45,49,51,56,55,55,51,52,56,50,50,52,50,54,195,155,195,100,255,255,255,215,4,220,11,33,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,255,0,0,0,0,0,2,91,74,0,0,0,0,0}

func TestModifyBlockReport(t *testing.T) {
	req := namenode_rpc.NewRequestPacket()
	req.Load(BlockReportRequestTestCase)
	
	p.ModifyBlockReport(req)
	
	if string(req.Parameters[1].Type) != "127.0.0.1:2010" {
		t.Fail()
	}

	if string(req.Parameters[1].Value) != "DS-678002061-127.0.1.1-2010-1387734822426" {
		t.Fail()
	}

	fmt.Println("Finished ModifyBlockReport()");
}
