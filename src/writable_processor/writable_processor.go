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

func NewWritableProcessor() *WritableProcessor {
	w := WritableProcessor{}
	
	//generate a random id number for this processor
	w.id = rand.Int63n(999999999)

	return &w
}

func ReadRequestHeader(reader writables.Reader) *writables.DataRequestHeader {
	drh := writables.NewDataRequestHeader()
	drh.Read(reader)
	return drh
}

//talk with the client; cache and forward requests
//run as goroutine from main.go
func HandleClient(conn net.Conn, dataNode net.Conn) {
	for {
		//we convert the net.Conn's into Connection objects
		connObj := NewConnection(conn)
		//dataNodeObj := NewConnection(dataNode)

		//read in the request header
		requestHeader := ReadRequestHeader(connObj)
		fmt.Println("Read request header: ", requestHeader)
	}
}

func HandleDataNode(conn net.Conn, dataNode net.Conn) {
	for {

	}
}