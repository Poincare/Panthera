package hdfs_requests

import (
	"namenode_rpc"
)

type Processor struct {
	//array of the past requests received by this processor
	//presumably by the same client (i.e. one client per processor)
	PastRequests []namenode_rpc.RequestPacket
}

func NewProcessor() *Processor { 
	p := Processor{}
	return &p
}

//process a request packet
//with the cache, this should look inside the cache
//and either return a response to send to the client
//or, return a nil packet

//TODO at the moment, this doesn't actually look inside a 
//cache at all, it just sends back a nil packet; i.e.
//it is not doing any processing with results at all
func (Processor *p) Process(req *namenode_rpc.RequestPacket) *namenode_rpc.ResponsePacket {
	
	//see above TODO
	return nil
}

