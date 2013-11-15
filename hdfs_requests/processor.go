package hdfs_requests

import (
	"namenode_rpc"
	"net"
	"util"
	"caches"
	"log"
	"fmt"
	"bytes"
	"encoding/binary"
)

type PacketNumber uint32

type Processor struct {
	//array of the past requests received by this processor
	//presumably by the same client (i.e. one client per processor)
	PastRequests []namenode_rpc.RequestPacket

	//the configured, ready-to-go caches (supplied by main.go)
	cacheSet *caches.CacheSet

	//this map links up packet numbers with
	//request, response pairs. So, a response packet
	//with a packet number of 1 is responding to the request 
	//with the packet number of 1.
	//The Map() method fills in response and request packets as needed
	RequestResponse map[PacketNumber]namenode_rpc.PacketPair

	//the channel processed by EventLoop()
	EventChannel chan ProcessorEvent
}

func NewProcessor(event_chan chan ProcessorEvent, cacheSet *caches.CacheSet) *Processor { 
	p := Processor{}
	p.RequestResponse = make(map[PacketNumber]namenode_rpc.PacketPair)

	p.EventChannel = event_chan
	p.cacheSet = cacheSet

	go p.EventLoop()

	/* disable the caches or enable them here */
	//p.cacheSet.GfiCache.Disable()

	return &p
}

//this gets called by both HandleConnection in order to put a 
//Request into one of the caches. Once a corresponding call is made
//to CacheResponse(), the cache spot is "filled"
//TODO need to implement
func (p *Processor) CacheRequest(req *namenode_rpc.RequestPacket) {
	if(string(req.MethodName) == "getFileInfo") {
		//follow through with the GetFileInfoCache
		fmt.Println("Caching request..., cache size: ", len(p.cacheSet.GfiCache.Cache.RequestResponse))
		p.cacheSet.GfiCache.Cache.AddRequest(req)
		fmt.Println("Cached request. Cache size: ", len(p.cacheSet.GfiCache.Cache.RequestResponse))
		log.Println("Cached GFI Request: ")
	}
}

//this gets called both HandleHDFS in order to put a Response into 
//one of the caches. It looks up a request with the same packet number
//and if such a request exists in the cache, it places itself into that
//spot.
//TODO need to implement
func (p *Processor) CacheResponse(resp namenode_rpc.ResponsePacket) {
	packetNum := caches.PacketNumber(resp.GetPacketNumber())

	//we can hook up a response with a request
	if p.cacheSet.GfiCache.Cache.HasPacketNumber(packetNum) {
		p.cacheSet.GfiCache.Cache.AddResponse(resp)
	}

	log.Println("Cached response: ", resp)
}

//this gets called by the main function on a new instance of Processor
//when we get a new connection
func (p *Processor) HandleConnection(conn net.Conn, hdfs net.Conn) {
	util.Log("Handling connection...")
	for {
		byteBuffer := make([]byte, 1024)
		//blocks
		bytesRead, read_err := conn.Read(byteBuffer);
		byteBuffer = byteBuffer[0:bytesRead]

		if read_err != nil {
			util.LogError(read_err.Error())
			conn.Close()
			hdfs.Close()
			return
		}

		util.Log("Handling the packet...")

		if bytesRead > 0 {
			rp := namenode_rpc.NewRequestPacket()
			err := rp.Load(byteBuffer)
			if err != nil {
				util.LogError("Error in loading request packet: " + err.Error())
			} else {
				//deal with checking the cache
				resp := p.Process(rp)
				log.Println("Resp: ", resp)
				//if a response was found in one the caches currently running
				//we can respond to the client immediately, we don't need to 
				//wait for HDFS do anything
				if resp != nil {
					fmt.Println("Cache hit!")
					util.Log("found in the cache")
				} else {
					fmt.Println("Cache miss!, methodname: ", string(rp.MethodName))
					//if it wasn't found in any of the
					//caches, we should cache the request
					util.Log("Trying to cache a request...")
					p.CacheRequest(rp)

					//TODO this is an important consideration - do we still need
					//to send it to the server? For some methods, this might be
					//important! For example, analytics would be affected if we
					//kept swooping these up
					util.Log("not found in cache")
					hdfs.Write(byteBuffer);
				}
			}
		}
	}
}

func (p *Processor) HandleHDFS(conn net.Conn, hdfs net.Conn) {
	for {
		buf := make([]byte, 1024)

		//Read() blocks
		bytesRead, readErr := hdfs.Read(buf)
		buf = buf[0:bytesRead]

		//TODO POTENTIAL BUG the packet number should always be at the front of the response packet
		//but, the docs don't actually explicitly mention this
		var packetNumber uint32
		byteBuffer := bytes.NewBuffer(buf)

		//read in the packet number (should be a uint32)
		binary.Read(byteBuffer, binary.BigEndian, &packetNumber)

		//create a generic response packet. Since we know the packet
		//number, it doesn't particularly matter what type of packet it 
		//actually is
		genericResp := namenode_rpc.NewGenericResponsePacket(buf, packetNumber)

		//detects EOF's etc.
		if readErr != nil {
			util.LogError(readErr.Error())
			conn.Close()
			hdfs.Close()
			return
		}
		if(bytesRead > 0) {
			//cache the response (CacheResponse should find out if there is
			//a matching request to this response)
			p.CacheResponse(genericResp)

			//proxy the read data to the associated client socket
			conn.Write(buf)
		}
	}
}


//process a request packet
//with the cache, this should look inside the cache
//and either return a response to send to the client
//or, return a nil packet

//TODO at the moment, this doesn't actually look inside a 
//cache at all, it just sends back a nil packet; i.e.
//it is not doing any processing with results at all
func (p *Processor) Process(req *namenode_rpc.RequestPacket) namenode_rpc.ResponsePacket {
	methodName := string(req.MethodName)
	log.Println("Trying to do something here...")
	if methodName == "getFileInfo" {
		fmt.Println("method is GFI, querying GFICache...")

		//this will return a correct response if we can find one
		//cached, or it will simply return nil so that we now
		//that a result was not found in the
		res := p.cacheSet.GfiCache.Query(req)
		return res
	} else if methodName == "create" {
		//if a new object is being created, we're going to to wrap it
		//in a ProcessorEvent object and fire it off to the other processors
		filepath := req.GetCreateRequestPath()
		event := NewObjectCreatedEvent(filepath)
		
		//we send the event in a nonblocking fashion so that this
		//goroutine does *not* block since it replies directly to
		//the client
		go func() {
			p.EventChannel <- *event
		}()
	} else {
		log.Println("Not getFileInfo call, trying to return nil now")
	}
	return nil
}

//fills in the correct packets into RequestResponse mapping
func (p *Processor) MapRequest(req namenode_rpc.ReqPacket) {
	//get the value and check if the packetnumber currently exists
	//in the mapping
	packetNum := PacketNumber(req.GetPacketNumber())
	pair, present := p.RequestResponse[packetNum]

	//if it is not present, a new "slot" has to be made in the map
	if !present {
		pair := namenode_rpc.NewPacketPair()
		pair.Request = req
		p.RequestResponse[packetNum] = *pair
	} else {
		//if it is present, put in the correct slot
		pair.Request = 	req
		p.RequestResponse[packetNum] = pair
	}
}

func(p *Processor) MapResponse(resp namenode_rpc.ResponsePacket) {
	packetNum := PacketNumber(resp.GetPacketNumber())
	pair, present := p.RequestResponse[packetNum]

	//if it is not present, a new "slot" has to be made in the map
	if !present {
		pair := namenode_rpc.NewPacketPair()
		pair.Response = resp
		p.RequestResponse[packetNum] = *pair
	} else {
		//if it is present, put in the correct slot
		pair.Response = resp
		p.RequestResponse[packetNum] = pair
	}
}

//this method runs in a separate goroutine and processes an 'event list';
//it listens for events, for example, clearing the GFI cache when a 
//different processor gets an event that shows that a file has been added
func (p *Processor) EventLoop() {
	for {
		event := <- p.EventChannel
		log.Println("Event received: ", event)
	}
}
