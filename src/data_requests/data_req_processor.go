package data_requests

/*
This file describes a request processor
for the data cache layer (as opposed to
metadata). Each processor communicates with
one DataNode, as specified by main.go.
The filename for this makes 0 sense.
*/

import (
	//go packages
	"net"
	"fmt"
	"math/rand"
	"time"
	"errors"


	//local packages
	"util"
	"datanode_rpc"
	"caches"
	"configuration"
)

//these messages are passed through a channel
//for the handleConnection and handleDataNode
//threads to communicate
type CommMessage struct {
	CloseSocket bool

	//set to true if the currentRequestVariable is
	//no longer nil or has been set to a new value
	//i.e. means that handleDataNode should try to 
	//handle a datanode response
	CurrentRequestAvailable bool
}

//processes requests from clients and forwards
//them to the datanodes
type Processor struct {

	//the object of the request that was most recently
	//sent out. There isn't something like a packet number 
	//method in the protocol, so the response *must* be
	//paired with the currentRequest in the cache
	currentRequest datanode_rpc.ReqPacket

	//pointer to a global DataCache shared across all the
	//processors
	dataCache *caches.DataCache

	//retry a connection with the dataNode
	retryDataNode bool

	//the datanode location that this processor is 
	//dealing with
	nodeLocation *configuration.DataNodeLocation

	//communication between the handleConnection and handleHDFS methods
	//if one of them has closed, it informs the other method
	commChan chan CommMessage

	//set to true if we want to skip relaying a response from HDFS
	skipResponse bool

	//id number that identifies this processor
	id int64

	//time variables to get latency values
	startTime time.Time
	startTimeCached time.Time
}

func NewProcessor(dataCache *caches.DataCache, nodeLocation *configuration.DataNodeLocation) *Processor {
	p := Processor{
		dataCache: dataCache,
		retryDataNode: true,
		nodeLocation: nodeLocation}
	p.commChan = make(chan CommMessage)

	//generate a psuedo-random id number for this processor
	//doesn't really *have* to be unique; mostly only used
	//for debugging purposes
	p.id = rand.Int63n(999999999)
	return &p
}

//check the communication channel, operate on the socket
//as necessary. 
//returns false if the processor needs to end
func (p *Processor) checkComm(sock net.Conn) bool {
	select {
		case msg := <- p.commChan:
			if msg.CloseSocket { 
				return false
			}
			return true
		default:
			return true
	}
	return true
}

//get message from the communication message queue/channel
func (p *Processor) getMessage() CommMessage {
	return <- p.commChan
}

//check the communication channel to see if the datanode 
//can read in a new message
func (p *Processor) currentRequestAvailable(msg CommMessage) bool {
	return msg.CurrentRequestAvailable
}

func (p *Processor) sendCloseSocket() {
	fmt.Println("Sending close socket message...")
	msg := CommMessage{
		CloseSocket: true}
	p.commChan <- msg
}

//records the latency of writing from cache
func (p *Processor) RecordCachedLatency() {
	now := time.Now()
	duration := now.Sub(p.startTimeCached)
	util.CachedLatencyLog.Println(duration.Nanoseconds())
}

//closes the connection socket
func (p *Processor) closeConnSocket(conn net.Conn) {
	util.DataReqLogger.Println("Closing socket...")
	conn.Close()
	go p.sendCloseSocket()
}

func (p *Processor) setCurrentRequest(dataRequest datanode_rpc.ReqPacket) {
	p.currentRequest = dataRequest
	util.DataReqLogger.Println(p.id, " Set current request.")
}

func (p *Processor) queryCache(dataRequest *datanode_rpc.DataRequest) *datanode_rpc.DataResponse {
	resp := p.dataCache.Query(*dataRequest)
	util.DataReqLogger.Println(p.id, " Returned from cache request: ", resp)
	return resp
}

//handle the request if it is an instance of datanode_rpc.DataRequest
func (p *Processor) handleDataRequest(conn net.Conn, dataNode net.Conn, dataRequest *datanode_rpc.DataRequest) error {
	if dataRequest == nil {
		return errors.New("Data request value is nil")
	}

	p.setCurrentRequest(dataRequest)
	resp := p.queryCache(dataRequest)
	
	if resp != nil {
		util.DataReqLogger.Println(p.id, "Cache hit!")
		fmt.Println("Cache hit!")
		
		respBytes, _ := resp.Bytes()
		conn.Write(respBytes)
		p.RecordCachedLatency();
		p.skipResponse = true
	} else {
		util.DataReqLogger.Println(p.id, " Cache miss. (cache size: ", p.dataCache.CurrSize(), ")")
		util.DataReqLogger.Println(p.id, " Cache contents: ", p.dataCache.CachedRequests())
		util.DataReqLogger.Println(p.id, " Cached responses: ", p.dataCache.CachedResponses())
	}

	dataBytes, err := dataRequest.Bytes()
	if err != nil {
		util.DataReqLogger.Println(p.id, "Could not get dataRequest bytes: ", err)
		p.closeConnSocket(conn)
		return err
	}
	dataNode.Write(dataBytes)
	p.startTime = time.Now()
	return nil
}

//handle requests that are instances of datanode_rpc.PutDataRequest
//this should initially work as a proxy since we are not interested in caching
//or modifying put data requests
func (p *Processor) handlePutDataRequest(conn net.Conn, dataNode net.Conn, 
putDataRequest *datanode_rpc.PutDataRequest) error {
	if putDataRequest == nil {
		return errors.New("PutDataRequest is nil; cannot proceed with HandlingPutDataRequest")
	}

	p.setCurrentRequest(putDataRequest)

	dataBytes, err := putDataRequest.Bytes()
	if err != nil {
		util.DataReqLogger.Println(p.id, "Could not get putDataRequestBytes, err: ", err)
		p.closeConnSocket(conn)
		return err
	}
	dataNode.Write(dataBytes)
	p.startTime = time.Now()
	return nil
}

func (p *Processor) HandleConnectionSingular(conn net.Conn, dataNode net.Conn) {
	for {
		go p.checkComm(conn)
		util.DataReqLogger.Println("Connected to client.")
		dataRequest := datanode_rpc.NewDataRequest()

		//read in the request object (should block)
		err := dataRequest.FullLiveLoad(conn)
		util.DataReqLogger.Println(p.id, " Received request: ", dataRequest)
		p.startTimeCached = time.Now()

		if err != nil {
			util.DataReqLogger.Println(p.id, " Could not load data request object; assuming socket is closed.")
			util.DataReqLogger.Println("---")
			conn.Close()

			go p.sendCloseSocket()
			return
		}

		util.Log("Loaded object...")

		if dataRequest != nil {
			p.currentRequest = dataRequest
			util.DataReqLogger.Println(p.id, " Set current request.")

			resp := p.dataCache.Query(*dataRequest)
			util.DataReqLogger.Println(p.id, " Returned from cache request: ", resp)
			if resp != nil {
				util.DataReqLogger.Println(p.id, " Cache hit!")
				fmt.Println("Cache hit!")
				
				//write the response from the cache
				respBytes, _ := resp.Bytes();
				conn.Write(respBytes)
				p.RecordCachedLatency();
				p.skipResponse = true
			} else {
				util.DataReqLogger.Println(p.id, " Cache miss. (cache size: ", p.dataCache.CurrSize(), ")")
				util.DataReqLogger.Println(p.id, " Cache contents: ", p.dataCache.CachedRequests())
				util.DataReqLogger.Println(p.id, " Cached responses: ", p.dataCache.CachedResponses())
			}
			//write the buffer to the datanode, essentially relaying the information
			//between the client and the data node
			dataReqBytes, _ := dataRequest.Bytes()
			dataNode.Write(dataReqBytes)
			p.startTime = time.Now()
		}
	}
}

//tell the datanode that a new current request is available 
//and it will need to handle a response soon
func (p *Processor) sendCurrentRequestAvailable() {
	msg := new(CommMessage)
	msg.CloseSocket = false
	msg.CurrentRequestAvailable = true

	//push the message into the communication channel between the goroutines
	p.commChan <- *msg
}

//called as a goroutine - handles the connection with a client; forwards
//requests to the datanode and responds from memory cache when possible
func (p *Processor) HandleConnection(conn net.Conn, dataNode net.Conn) {
	for {
		go p.checkComm(conn)
		util.DebugLogger.Println("Waiting for request packet...")
		util.DataReqLogger.Println("Connected to client.")

		//read in the request object (should block)
		dataRequest, err := datanode_rpc.LoadRequestPacket(conn)
		go p.sendCurrentRequestAvailable()
		util.DebugLogger.Println("Loaded packet.")

		if err != nil {
			util.DataReqLogger.Println("Failed LiveReadInitial(): ", err)
			p.closeConnSocket(conn)
			return
		}
		util.DataReqLogger.Println(p.id, " Received request: ", dataRequest)
		p.startTimeCached = time.Now()


		//switch according to the type of packet we are managing
		switch dataRequest.(type) {
		case *datanode_rpc.DataRequest:
			dataRequest := dataRequest.(*datanode_rpc.DataRequest)
			p.handleDataRequest(conn, dataNode, dataRequest)
		case *datanode_rpc.PutDataRequest:
			putDataRequest := dataRequest.(*datanode_rpc.PutDataRequest)
			p.handlePutDataRequest(conn, dataNode, putDataRequest)
		}
		
	}
}

//return value: if true, the retry was successful, if false, it failed
func (p *Processor) retryDataNodeConnection(conn net.Conn, dataNode net.Conn) bool {
	util.LogError("Could not connect to data node.")

	if !p.retryDataNode {
		util.LogError("Since not retrying connections, closing data node client connections")
		conn.Close()
		return false
	}

	for p.retryDataNode {
		var retryErr error
		dataNode, retryErr = net.Dial("tcp", p.nodeLocation.Address())
		if retryErr != nil {
			util.LogError("Retry connection failed. Trying again...")
		} else {
			util.Log("Retry connection sucessful. Continuing...")
			return true
		}
	}

	return true
}

//record the latency time (w/o a cache) to the log
func (p *Processor) RecordNoCacheLatency() {
	now := time.Now()
	duration := now.Sub(p.startTime)
	util.NoCacheLatencyLog.Println(duration.Nanoseconds())
}

func (p *Processor) handleDataResponse(conn net.Conn, dataNode net.Conn) {
	util.DebugLogger.Println("current request: ", p.currentRequest)
	dataResponse := datanode_rpc.NewDataResponse()
	err := dataResponse.LiveLoad(dataNode)
	byt, _ := dataResponse.Bytes()
	util.DebugLogger.Println("Data response bytes len: ", len(byt))
	util.DebugLogger.Println("dataResponse.DataLength: ", dataResponse.DataLength)
	util.DebugLogger.Println("dataResponse.data bytes: ", dataResponse.Data)

	if err != nil {
		util.DataReqLogger.Println("DataNode connection closed.")
		res := p.retryDataNodeConnection(conn, dataNode)
		if !res {
			util.DataReqLogger.Println("Could not retry successfully. Returning from HandleDataNode")
			//connections were closed, no point in continuing with the processor
			return
		}
	}
	
	if dataResponse != nil {
		p.RecordNoCacheLatency()
		//we skip a response because we might be responding to something
		//that has already been responded to by the cache
		if p.skipResponse {
			util.DataReqLogger.Println("Response skipped.")
			p.skipResponse = false
			return
		}

		pair := datanode_rpc.NewRequestResponse(p.currentRequest, dataResponse)

		//add the pair to the cache
		p.dataCache.AddRpcPair(*pair)
		util.DataReqLogger.Println(p.id, "Cached request/response pair.")
		//write the information back to the client
		dataRespBytes, _ := dataResponse.Bytes()
		_, err := conn.Write(dataRespBytes)
		if err != nil {
			util.DebugLogger.Println("Error ocurred in writing data response to client: ", dataRespBytes)
		}
		util.DataReqLogger.Println(p.id, "Wrote response to client.")
	}
}

func (p *Processor) handlePutDataResponse(conn net.Conn, dataNode net.Conn) {
	//there are actually three responses  to be read for each put request
	for i := 0; i<1; i++ {
		util.DebugLogger.Println("Reading put data response #", i)
		pdr := datanode_rpc.NewPutDataResponse()

		util.DebugLogger.Println("Starting liveLoad() of pdr...")
		//read the pdr from the dataNode connection
		pdr.LiveLoad(dataNode)
		util.DebugLogger.Println("Finished liveLoad() of pdr...")

		//write the pdr to the client
		pdrBytes, _ := pdr.Bytes()
		bytesWritten, err := conn.Write(pdrBytes)
		conn.Write(pdrBytes)
		conn.Write(pdrBytes)

		if err != nil || bytesWritten != len(pdrBytes) {
			util.DataReqLogger.Println("Error ocurred in writing to DataNode. Possible closed socket (no action taken)")
		}
	}
}

func (p *Processor) loadResponse(conn net.Conn, dataNode net.Conn) {
	if p.currentRequest == nil {
		return
	}

	//the type of response packet to load depends on what type 
	//the current request is
	switch p.currentRequest.(type) {
	case *datanode_rpc.DataRequest:
		util.DataReqLogger.Println(p.id, " CurrRequest is DataRequest, now handling dataResponse")
		p.handleDataResponse(conn, dataNode)
	case *datanode_rpc.PutDataRequest:
		util.DataReqLogger.Println(p.id, " CurrRequest is PutDataRequest, now handling putDataResponse")
		p.handlePutDataResponse(conn, dataNode)
	}
}

func (p *Processor) HandleDataNode(conn net.Conn, dataNode net.Conn) {
	fmt.Println("here, dataNode: ", dataNode)

	if dataNode == nil {
		util.DebugLogger.Println("Datanode is nil...")
		fmt.Println("the datanode is nil, address", p.nodeLocation.Address())
		var err error
		dataNode, err = net.Dial("tcp", p.nodeLocation.Address())
		fmt.Println("new datanode: ", dataNode)
		if err != nil || dataNode == nil {
			util.Log("here")
			res := p.retryDataNodeConnection(conn, dataNode)
			util.Log("there")
			if !res {
				return
			}
		}
	}
	
	for {
		util.DebugLogger.Println("In the DataNode mainloop...")

		msg := p.getMessage()
		util.DebugLogger.Println("Got message from the channel: ", msg)
		keepRunning := !msg.CloseSocket
		//telling us that we've received a close down message
		if !keepRunning {
			util.DataReqLogger.Println("Close down message received. Shutting down...")
			defer util.DataReqLogger.Println("HandleDataNode() closed.")
			return
		}

		util.DebugLogger.Println("Starting to load response from DataNode...")
		util.Log("Handling dataNode connection...")
		//handles all of the intricacies of the different response types, etc.
		if msg.CurrentRequestAvailable {
			p.loadResponse(conn, dataNode)
			util.DebugLogger.Println("Finished with response from DataNode.")
		}
	}

	/*
	fmt.Println("here, dataNode: ", dataNode)
	if dataNode == nil {
		fmt.Println("the datanode is nil, address", p.nodeLocation.Address())
		var err error
		dataNode, err = net.Dial("tcp", p.nodeLocation.Address())
		fmt.Println("new datanode: ", dataNode)
		if err != nil || dataNode == nil {
			util.Log("here")
			res := p.retryDataNodeConnection(conn, dataNode)
			util.Log("there")
			if !res {
				return
			}
		}
	}
	
	for {
		keepRunning := p.checkComm(dataNode)
		//telling us that we've received a close down message
		if !keepRunning {
			util.DataReqLogger.Println("Close down message received. Shutting down...")
			defer util.DataReqLogger.Println("HandleDataNode() closed.")
			return
		}

		util.Log("Handling dataNode connection...")
		dataResponse := datanode_rpc.NewDataResponse()

		err := dataResponse.LiveLoad(dataNode)

		//unable to connect/read from the dataNode?
		if err != nil {
			util.DataReqLogger.Println("DataNode connection closed.")
			res := p.retryDataNodeConnection(conn, dataNode)
			if !res {
				util.LogError("Could not retry successfully. Returning from HandleDataNode")
				//connections were closed, no point in continuing with the processor
				return
			}
		}

		if dataResponse != nil {
			p.RecordNoCacheLatency();

			//we skip a response because we might be responding to something
			//that has already been responded to by the cache
			if p.skipResponse {
				util.DataReqLogger.Println("Response skipped.")
				p.skipResponse = false
				continue
			}

			pair := datanode_rpc.NewRequestResponse(p.currentRequest, dataResponse)

			//add the pair to the cache
			p.dataCache.AddRpcPair(*pair)
			util.DataReqLogger.Println(p.id, "Cached request/response pair.")
			//write the information back to the client
			dataRespBytes, _ := dataResponse.Bytes()
			conn.Write(dataRespBytes)
			util.DataReqLogger.Println(p.id, "Wrote response to client.")
		}
	} */
}


