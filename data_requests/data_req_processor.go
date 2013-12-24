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
}

//processes requests from clients and forwards
//them to the datanodes
type Processor struct {

	//the object of the request that was most recently
	//sent out. There isn't something like a packet number 
	//method in the protocol, so the response *must* be
	//paired with the currentRequest in the cache
	currentRequest datanode_rpc.DataRequest

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
}

func NewProcessor(dataCache *caches.DataCache, nodeLocation *configuration.DataNodeLocation) *Processor {
	p := Processor{
		dataCache: dataCache,
		retryDataNode: true,
		nodeLocation: nodeLocation}
	p.commChan = make(chan CommMessage)
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

func (p *Processor) sendCloseSocket() {
	fmt.Println("Sending close socket message...")
	msg := CommMessage{
		CloseSocket: true}
	p.commChan <- msg
}

func (p *Processor) HandleConnection(conn net.Conn, dataNode net.Conn) {
	for {
		go p.checkComm(conn)
		util.Log("Handling client data connection...")
		dataRequest := datanode_rpc.NewDataRequest()

		//read in the request object (should block)
		err := dataRequest.LiveLoad(conn)
		if err != nil {
			util.LogError("Could not load data request object; assuming socket is closed.")
			conn.Close()

			//tell the DataNode routine that it has to return
			go p.sendCloseSocket()
			return
		}

		util.Log("Loaded object...")

		/*
		buf := make([]byte, 1024)
		bytesRead := 0
		bytesRead, err := conn.Read(buf)
		if err != nil {
			util.LogError("Error in reading from client connection.")
		} */

		if dataRequest != nil {
			p.currentRequest = *dataRequest
			
			resp := p.dataCache.Query(*dataRequest)
			if resp != nil {
				fmt.Println("Cache hit!")
				//write the response from the cache
				conn.Write(resp.Bytes())
				p.skipResponse = true
			}
			//write the buffer to the datanode, essentially relaying the information
			//between the client and the data node
			dataNode.Write(dataRequest.Bytes())
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

func (p *Processor) HandleDataNode(conn net.Conn, dataNode net.Conn) {
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
			return
		}

		util.Log("Handling dataNode connection...")
		dataResponse := datanode_rpc.NewDataResponse()

		err := dataResponse.LiveLoad(dataNode)

		//unable to connect/read from the dataNode?
		if err != nil {
			util.LogError("DataNode connection seems to be closed.")
			res := p.retryDataNodeConnection(conn, dataNode)
			if !res {
				util.LogError("Could not retry successfully. Returning from HandleDataNode")
				//connections were closed, no point in continuing with the processor
				return
			}
		}

		/*
		buf := make([]byte, 1024)
		bytesRead := 0
		bytesRead, err := dataNode.Read(buf)
		if err != nil {
			util.LogError("Error in reading from data connection.")
		} */

		if dataResponse != nil {

			//we skip a response because we might be responding to something
			//that has already been responded to by the cache
			if p.skipResponse {
				p.skipResponse = false
				continue
			}

			pair := datanode_rpc.NewRequestResponse(&p.currentRequest, dataResponse)
			//add the pair to the cache
			p.dataCache.AddRpcPair(*pair)

			//write the information back to the client
			conn.Write(dataResponse.Bytes())
		}
	}
}


