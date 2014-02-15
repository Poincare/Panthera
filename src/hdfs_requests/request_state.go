package hdfs_requests

import (
	"bytes"
	"namenode_rpc"
)

//this describes a mechanism for determining whether or
//not the responses from the server have completed responding
//to a request
type RequestState struct {
	//determines if the first packet in the response series
	//has been read or not (the first packet is the one
	//that has the packet number)
	ReadFirstPacket bool

	//we stuff the bytes received from the server into
	//this buffer
	ByteBuffer bytes.Buffer

	//packetNumber of the current request state
	PacketNumber uint32
}

//constructor
func NewRequestState() *RequestState {
	rs := RequestState{}
	rs.ReadFirstPacket = false
	return &rs
}

//this method is called a new request is received from 
//the client. It resets the byteBuffer and readFirstPacket
//fields and caches the current response.
func (rs *RequestState) Empty(packetNumber uint32) *namenode_rpc.GenericResponsePacket {
	rs.ReadFirstPacket = false
	genericResp := namenode_rpc.NewGenericResponsePacket(rs.ByteBuffer.Bytes(), rs.PacketNumber)
	rs.ByteBuffer.Reset()
	rs.PacketNumber = packetNumber

	return genericResp
}
