package writable_processor

import (
	//go packages
	"net"
	"math/rand"
	"bytes"
	"encoding/hex"

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

	//communication channel between goroutines
	//carries messages such as socket closes, etc.
	commChan chan *CommMessage
}

func New() *WritableProcessor {
	w := WritableProcessor{}
	
	//generate a random id number for this processor
	w.id = rand.Int63n(999999999)
	w.commChan = make(chan *CommMessage)
	return &w
}

//this function will tell other goroutines to close sockets and return
//should be run as a goroutine (i.e. async)
func (w *WritableProcessor) sendSocketClose() {
	s := NewSocketCloseMsg()
	w.commChan <- s
}

//read the communication channel for messages
func (w *WritableProcessor) readComm() *CommMessage {
	select {
	//if there is a message available, we can simply return it
	case msg := <- w.commChan:
		return msg
	//if not...
	default:
		return nil
	}
}

func (w *WritableProcessor) ReadRequestHeader(reader writables.Reader) *writables.DataRequestHeader {
	util.TempLogger.Println("Reading request header...")
	drh := writables.NewDataRequestHeader()
	err := drh.Read(reader)
	if err != nil {
		return nil
	}

	util.TempLogger.Println("Finished reading request header...")

	return drh
}

func (w *WritableProcessor) readReadBlockRequest(reader writables.Reader) (*writables.ReadBlockHeader, error) {
	r := writables.NewReadBlockHeader()

	//read in the block request
	err := r.Read(reader)
	return r, err
}

//read a pipelineAck from the client
//called by handleReadBlockResponse()
func (w *WritableProcessor) readPipelineAck(conn writables.ReaderWriter,
	dataNode writables.ReaderWriter) {
	p := writables.NewPiplineAck()
}

//this method is called to handle responses to an OP_READ_BLOCK request.
//the response contains the contents of the actual block.
func (w *WritableProcessor) handleReadBlockResponse(conn writables.ReaderWriter, 
	dataNode writables.ReaderWriter) {
	
	//check the channel to make sure that the socket isn't closed
	msg := w.readComm()
	if msg != nil {
		if msg.SocketClose {
			return
		}
	}

	//read a response from the datanode
	response := writables.NewReadBlockResponse()
	err := response.Read(dataNode)
	if err != nil {
		util.TempLogger.Println("Error occurred in reading from DataNode: ", err)
	}
	util.TempLogger.Println("response from ReadBlockResponse(): ", response)
	util.TempLogger.Println("respone.Length: ", response.Length)
	util.TempLogger.Println("len(response.Data)", len(response.Data))
	resBuf := new(bytes.Buffer)
	err = response.Write(resBuf)
	if err != nil {
		util.TempLogger.Println("Error occurred in writing to buffer: ", err)
	}

	util.TempLogger.Println("resbuf from handleReadBlockResponse():")
	util.TempLogger.Println("\n" + hex.Dump(resBuf.Bytes()))

	bytesWritten, err := conn.Write(resBuf.Bytes())
	if err != nil {
		util.TempLogger.Println(w.id, "Error occurred in writing to client (in handleReadBlockResponse()):  ", err)
		util.TempLogger.Println(w.id, "Bytes written: ", bytesWritten)
		util.TempLogger.Println(w.id, "Assuming socket is closed.")
		w.sendSocketClose()
	}
}

func (w *WritableProcessor) processReadBlock(requestHeader *writables.DataRequestHeader, 
	conn writables.ReaderWriter, dataNode writables.ReaderWriter) {

	go w.handleReadBlockResponse(conn, dataNode)

	blockRequest, err := w.readReadBlockRequest(conn)
	if err != nil {
		util.DebugLogger.Println(w.id, "Error occurred in reading block request from client: ", err)
		util.DebugLogger.Println(w.id, "Assuming socket is closed.")
		go w.sendSocketClose()
		return
	}

	resBuf := new(bytes.Buffer)
	requestHeader.Write(resBuf)
	blockRequest.Write(resBuf)
	util.TempLogger.Println("Res buf (in processReadBlock()): ")
	util.TempLogger.Println("\n", hex.Dump(resBuf.Bytes()))

	_, err = dataNode.Write(resBuf.Bytes())
	if err != nil {
		util.DebugLogger.Println(w.id, "Error occurred in writing block to dataNode: ", err)
		util.DebugLogger.Println(w.id, "Assuming socket is closed.")
		go w.sendSocketClose()
		return
	}

	//once we have processed the OP_READ_BLOCK request,
	//we can just revert to regular request handling
	go w.generalProcessing(conn, dataNode, false)

	util.TempLogger.Println("Processed readBlock.")
}

//this method is called from generalProcessing()
func (w *WritableProcessor) handleGeneralResponse(conn writables.ReaderWriter, dataNode writables.ReaderWriter) {
	for {
		//check the channel to make sure that the socket isn't closed
		msg := w.readComm()
		if msg != nil {
			if msg.SocketClose {
				return
			}
		}

		buf := make([]byte, 1)
		_, err := dataNode.Read(buf)
		if err != nil {
			util.DebugLogger.Println(w.id, "Error occurred while reading from dataNode in handleGeneralResponse()")
			util.DebugLogger.Println(w.id, "Assuming socket is closed.")
			go w.sendSocketClose()
			return
		}

		_, err = conn.Write(buf)
		if err != nil {
			util.DebugLogger.Println(w.id, "Error occurred while writing to client in handleGeneralResponse()s")
			util.DebugLogger.Println(w.id, "Assuming socket is closed.")
			go w.sendSocketClose()
			return
		}
	}
}

func (w *WritableProcessor) forwardRequestHeader(requestHeader *writables.DataRequestHeader, dataNode writables.ReaderWriter) error {
	resBuf := new(bytes.Buffer)
	err := requestHeader.Write(resBuf)
	if err != nil {
		return err
	}

	_, err = dataNode.Write(resBuf.Bytes())

	return err
}

//this method is called if we have a request other than OP_READ_BLOCK
//since that is the only request that needs to be cached. Here,
//a simple relay is set up so that data that comes in is sent directly
//to the DataNode without modification. It also starts (as a goroutine)
//handleGeneralResponse() that relays packets from the DataNode to the client
func (w *WritableProcessor) generalProcessing(conn writables.ReaderWriter, dataNode writables.ReaderWriter, replaceResponse bool) {


	//start the response method
	if replaceResponse {
		go w.handleGeneralResponse(conn, dataNode)
	}

	for {
		//check the channel to make sure that the socket isn't closed
		msg := w.readComm()
		if msg != nil {
			if msg.SocketClose {
				return
			}
		}	

		buf := make([]byte, 1)
		_, err := conn.Read(buf)
		if err != nil {
			util.DebugLogger.Println(w.id, "Error occurred while reading from client in generalProcessing(): ", err)
			util.DebugLogger.Println(w.id, "Assuming socket is closed.")
			go w.sendSocketClose()
			return
		}

		_, err = dataNode.Write(buf)
		if err != nil {
			util.DebugLogger.Println(w.id, "Error occurred while writing to DataNode in generalProcessing(): ", err)
			util.DebugLogger.Println(w.id, "Assuming socket is closed.")
			go w.sendSocketClose()
			return
		}
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
		err := w.forwardRequestHeader(requestHeader, dataNode)
		if err != nil {
			util.DebugLogger.Println(w.id, "Unable to forward request header to dataNode in generalProcessing(): ", err)
			util.DebugLogger.Println(w.id, "Assuming socket is closed.")
			go w.sendSocketClose()
			return
		}
		w.generalProcessing(conn, dataNode, true)
	}
}

//talk with the client; cache and forward requests
//run as goroutine from main.go
func (w *WritableProcessor) HandleClient(conn net.Conn, dataNode net.Conn) {
	util.TempLogger.Println("HandleClient() called.")

	for {
		//check the channel to make sure that the socket isn't closed
		msg := w.readComm()
		if msg != nil {
			if msg.SocketClose {
				return
			}
		}

		//we convert the net.Conn's into Connection objects
		connObj := NewConnection(conn)
		dataNodeObj := NewConnection(dataNode)

		//read in the request header (blocking call)
		requestHeader := w.ReadRequestHeader(connObj)
		if requestHeader == nil {
			return
		}

		util.TempLogger.Println("Request header: ", requestHeader)

		//now we can process the request
		w.processRequest(requestHeader, connObj, dataNodeObj)
	}
}
