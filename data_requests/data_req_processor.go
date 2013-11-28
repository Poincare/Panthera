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

	//local packages
	"util"
	"datanode_rpc"
)


//processes requests from clients and forwards
//them to the datanodes
type Processor struct {

	//the object of the request that was most recently
	//sent out. There isn't something like a packet number 
	//method in the protocol, so the response *must* be
	//paired with the currentRequest in the cache
	currentRequest datanode_rpc.DataRequest
}

func NewProcessor() *Processor {
	p := Processor{}
	return &p
}

func (p *Processor) HandleConnection(conn net.Conn, dataNode net.Conn) {
	for {
		util.Log("Handling client data connection...")
		dataRequest := datanode_rpc.NewDataRequest()

		//read in the request object (should block)
		dataRequest.LiveLoad(conn)
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

			//write the buffer to the datanode, essentially relaying the information
			//between the client and the data node
			dataNode.Write(dataRequest.Bytes())
		}
	}
}

func (p *Processor) HandleDataNode(conn net.Conn, dataNode net.Conn) {
	for {
		util.Log("Handling dataNode connection...")
		dataResponse := datanode_rpc.NewDataResponse()
		dataResponse.LiveLoad(conn)
		util.Log("Loaded response object.")

		/*
		buf := make([]byte, 1024)
		bytesRead := 0
		bytesRead, err := dataNode.Read(buf)
		if err != nil {
			util.LogError("Error in reading from data connection.")
		} */

		if dataResponse != nil {
			pair := datanode_rpc.NewRequestResponse(&p.currentRequest, dataResponse)

			//write the information back to the client
			conn.Write(dataResponse.Bytes())
		}
	}
}


