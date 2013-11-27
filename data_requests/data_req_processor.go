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
)


//processes requests from clients and forwards
//them to the datanodes
type Processor struct {
}

func NewProcessor() *Processor {
	p := Processor{}
	return &p
}

func (p *Processor) HandleConnection(conn net.Conn, dataNode net.Conn) {
	for {
		util.Log("Handling data connection...")
		buf := make([]byte, 1024)
		bytesRead := 0
		bytesRead, err := conn.Read(buf)
		if err != nil {
			util.LogError("Error in reading from client connection.")
		}

		if bytesRead > 0 {

			//write the buffer to the datanode, essentially relaying the information
			//between the client and the data node
			dataNode.Write(buf)
		}
	}
}

func (p *Processor) HandleDataNode(conn net.Conn, dataNode net.Conn) {
		for {
		util.Log("Handling dataNode connection...")
		buf := make([]byte, 1024)
		bytesRead := 0
		bytesRead, err := dataNode.Read(buf)
		if err != nil {
			util.LogError("Error in reading from data connection.")
		}

		if bytesRead > 0 {
			//write the information back to the client
			conn.Write(buf)
		}
	}
}


