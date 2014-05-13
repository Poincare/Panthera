package data_requests

import (
	//go packages
	"net"

	//local packages
	"util"
	"datanode_rpc"
)

/* 
This file describes the Put Request file processor.
Since Put file requests are not cached, it doesn't
make any sense to run the same (higher latency) process
on Put requests. Instead, Panthera simply serves
as a relay between Hadoop and the DataNode.
*/

//this file takes a PutDataRequest and forwards it 
//onto the DataNode. This method is needed because 
//once the request is read, it still has to be forwarded
//to the DataNode
func ForwardPutDataRequest(request *datanode_rpc.PutDataRequest, 
conn net.Conn, dataNode net.Conn) {
	dataBytes, err := request.Bytes()
	if err != nil {
		util.DebugLogger.Println("Could not ForwardPutDataRequest(): ", err)
		return
	}

	dataNode.Write(dataBytes)
}

type PutRequestProcessor struct {
	//this id is inherited from the data_requests.Processor
	//that creates the PutRequestProcessor
	id int64
}


func NewPutRequestProcessor(id int64) *PutRequestProcessor {
	prp := PutRequestProcessor{id: id}
	return &prp
}

//reads from the client, forwards to the server
func (p *PutRequestProcessor) HandleConnection(conn net.Conn, dataNode net.Conn) {
	utill.DebugLogger.Println("HANDLING CONNECTION FROM PutRequestProcessor")
	for {
		buf := make([]byte, 1024)
		bytesRead, err := conn.Read(buf)
		if err != nil {
			//we can't really do anything if there's an error: just gotta keep going
			util.DebugLogger.Println("Error occurred while reading in 
			PutRequestProcessor.HandleConnection: ", err)
			continue
		}

		//truncate the buffer to the correct index
		buf = buf[0:bytesRead]
		_, err = dataNode.Write(buf)
		if err != nil {
			util.DebugLogger.Println("Errror occurred while writing in 
			PutRequestProcessor.HandleConnection: ", err)
		}
	}
}

//reads from the server forwards to the client
func (p *PutRequestProcessor) HandleHDFS(conn net.Conn, dataNode net.Conn) {
	for {
		buf := make([]byte, 1024)
		bytesRead, err := dataNode.Read(buf)

		if err != nil {
			util.DebugLogger.Println("Error occurred while reading in
			 PutRequestProcessor.HandleHDFS: ", err)
			continue
		}

		buf = buf[0:bytesRead]
		_, err = conn.Write(buf)
		if err != nil {
			util.DebugLogger.Println("Error occurred while writing in 
			PutRequestProcessor.HandleHDFS", err)
			continue
		}
	}
}


