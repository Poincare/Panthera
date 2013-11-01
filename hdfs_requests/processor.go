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

