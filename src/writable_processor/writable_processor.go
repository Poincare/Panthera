package writable_processor

import (
	//go packages
	"net"
	"math/rand"
	"fmt"

	//local packages
	"writables"
)

/*
* This file describes a packet processor that
* uses the writables package instead of trying
* to read in entire packets at a time */

type WritableProcessor struct {
	//id of this processor; mostly only used
	//in the logs
	id int64
}

func New() *WritableProcessor {
	w := WritableProcessor{}
	
	//generate a random id number for this processor
	w.id = rand.Int63n(999999999)

	return &w
}

func (w *WritableProcessor) ReadRequestHeader(reader writables.Reader) *writables.DataRequestHeader {
	drh := writables.NewDataRequestHeader()
	drh.Read(reader)
	return drh
}

func (w *WritableProcessor) processRequest(requestHeader *writables.DataRequestHeader) {
	//what kind of processing we do depends on
	//the type of command given (stored as field Op 
	//in DataRequestHeader)
	switch(requestHeader.Op) {
	case writables.OP_READ_BLOCK:
		fmt.Println("Received a OP_READ_BLOCK request.")
	default:
		fmt.Println("Received some other kind of request, Op: ", requestHeader.Op)
	}
}

//talk with the client; cache and forward requests
//run as goroutine from main.go
func (w *WritableProcessor) HandleClient(conn net.Conn, dataNode net.Conn) {
	for {
		//we convert the net.Conn's into Connection objects
		connObj := NewConnection(conn)
		//dataNodeObj := NewConnection(dataNode)

		//read in the request header (blocking call)
		requestHeader := w.ReadRequestHeader(connObj)
		
		//now we can process the request
		w.processRequest(requestHeader)
	}
}

func (w *WritableProcessor) HandleDataNode(conn net.Conn, dataNode net.Conn) {
	for {

	}
}