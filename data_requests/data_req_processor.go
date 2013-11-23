package data_requests

/*
This file describes a request processor
for the data cache layer (as opposed to
metadata). Each processor communicates with
one DataNode, as specified by main.go.
The filename for this makes 0 sense.
*/

import (
	"net"
)


//processes requests from clients and forwards
//them to the datanodes
type Processor struct {
}

func NewProcessor() *Processor {
	p := Processor{}
	return &p
}

func (p *Processor) handleConnection(conn net.Conn, dataNode net.Conn) {
	for {
		
	}
}


