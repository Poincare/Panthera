package writable_processor

import (
	//go packages
	"net"
	"math/rand"
	"bytes"

	//local packages
	"writables"
	"util"
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
	util.TempLogger.Println("Reading request header...")
	drh := writables.NewDataRequestHeader()
	drh.Read(reader)
	util.TempLogger.Println("Finished reading request header...")

	return drh
}

func (w *WritableProcessor) readReadBlockRequest(reader writables.Reader) (*writables.ReadBlockHeader, error) {
	r := writables.NewReadBlockHeader()

	//read in the block request
	err := r.Read(reader)
	return r, err
}

func (w *WritableProcessor) processReadBlock(requestHeader *writables.DataRequestHeader, 
	conn writables.ReaderWriter, dataNode writables.ReaderWriter) {

	blockRequest, err := w.readReadBlockRequest(conn)
	if err != nil {
		util.DebugLogger.Println(w.id, "Error occurred in reading block request from client: ", err)
		return
	}

	resBuf := new(bytes.Buffer)
	requestHeader.Write(resBuf)
	blockRequest.Write(resBuf)

	_, err = dataNode.Write(resBuf.Bytes())
	if err != nil {
		util.DebugLogger.Println(w.id, "Error occurred in writing block to dataNode: ", err)
		return
	}
}

func (w *WritableProcessor) processRequest(requestHeader *writables.DataRequestHeader, conn writables.ReaderWriter,
	dataNode writables.ReaderWriter) {
	//what kind of processing we do depends on
	//the type of command given (stored as field Op 
	//in DataRequestHeader)
	switch(requestHeader.Op) {
	case writables.OP_READ_BLOCK:
		util.TempLogger.Println("Received a OP_READ_BLOCK request.")
		w.processReadBlock(requestHeader, conn, dataNode)

	default:
		util.TempLogger.Println("Received some other kind of request, Op: ", requestHeader.Op)
	}
}

//talk with the client; cache and forward requests
//run as goroutine from main.go
func (w *WritableProcessor) HandleClient(conn net.Conn, dataNode net.Conn) {
	util.TempLogger.Println("HandleClient() called.")

	for {
		//we convert the net.Conn's into Connection objects
		connObj := NewConnection(conn)
		dataNodeObj := NewConnection(dataNode)

		//read in the request header (blocking call)
		requestHeader := w.ReadRequestHeader(connObj)
		util.TempLogger.Println("Request header: ", requestHeader)

		//now we can process the request
		w.processRequest(requestHeader, connObj, dataNodeObj)
	}
}

func (w *WritableProcessor) HandleDataNode(conn net.Conn, dataNode net.Conn) {
	for {

	}
}