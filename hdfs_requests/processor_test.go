package hdfs_requests

import (
	"testing"
	"namenode_rpc"
	"reflect"
	"fmt"
)

var eventChan chan ProcessorEvent = make(chan ProcessorEvent)

func TestNewProcess(t *testing.T) {
	p := NewProcessor(eventChan)
	if p == nil {
		t.FailNow()
	}

	if p.EventChannel == nil {
		t.FailNow()
	}
}

var RequestPacketTestCase []byte = []byte{0, 0, 0, 60, 0, 0, 0, 2, 0, 10, 103, 101, 116, 
		76, 105, 115, 116, 105, 110, 103, 0, 0, 0, 2, 0, 16, 106, 
		97, 118, 97, 46, 108, 97, 110, 103, 46, 83, 116, 114, 105, 
		110, 103, 0, 12, 47, 117, 115, 101, 114, 47, 104, 100, 117, 
		115, 101, 114, 0, 2, 91, 66, 0, 0, 0, 0}

func TestMapRequest(t *testing.T) {
	p := NewProcessor(eventChan)
	req := namenode_rpc.NewRequestPacket()
	req.Load(RequestPacketTestCase)

	p.MapRequest(req)

	if !reflect.DeepEqual(p.RequestResponse[PacketNumber(req.PacketNumber)].Request, req) {
		fmt.Println("Not equal: ", p.RequestResponse[PacketNumber(req.PacketNumber)].Request, req)
		t.Fail()
	}
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
	p := NewProcessor(eventChan)
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
}

func TestMap(t *testing.T) {
	p := NewProcessor(eventChan)
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
}

//TODO incomplete
func TestCacheRequest(t *testing.T) {
	p := NewProcessor(eventChan)

	req := namenode_rpc.NewRequestPacket()
	req.Load(RequestPacketTestCase)

	p.CacheRequest(req)

}

//this isn't really a complete test
//hopefully, after some tests are added, it should print out
//a message which sould serve as a unit test
func TestEventLoop (t *testing.T) {
	p := NewProcessor(eventChan)
	p.EventChannel = make(chan ProcessorEvent)

	go p.EventLoop()

	event := NewObjectCreatedEvent("some/random/path")
	p.EventChannel <- event
}

