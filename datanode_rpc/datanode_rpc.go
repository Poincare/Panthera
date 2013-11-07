package datanode_rpc

import (
	"bytes"
	"encoding/binary"
)

//TODO POTENTIAL BUG:
//this packet structure has been
//obtained from the Wireshark
//specification; have to double
//check the validity of the structure
type DataRequest struct {
	ProtocolVersion uint16
	Command uint8
	BlockId uint64
	Timestamp uint64
	StartOffset uint64
	BlockLength uint64
	
	ClientIdLength uint8
	ClientId []byte

	AccessIdLength uint8
	AccessId []byte

	AccessPasswordLength uint8
	AccessPassword []byte

	AccessTypeLength uint8
	AccessType []byte

	AccessServiceLength uint8
	AccessService []byte
}

//constructor
func NewDataRequest() *DataRequest {
	dr := DataRequest{}
	return &dr
}

func (dr *DataRequest) Load(buf []byte) error {
	byte_buffer := bytes.NewBuffer(buf)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.ProtocolVersion))
	binary.Read(byte_buffer, binary.BigEndian, &(dr.Command))
	binary.Read(byte_buffer, binary.BigEndian, &(dr.BlockId))
	binary.Read(byte_buffer, binary.BigEndian, &(dr.Timestamp))
	binary.Read(byte_buffer, binary.BigEndian, &(dr.StartOffset))
	binary.Read(byte_buffer, binary.BigEndian, &(dr.BlockLength))

	binary.Read(byte_buffer, binary.BigEndian, &(dr.ClientIdLength))
	dr.ClientId = make([]byte, dr.ClientIdLength)
	byte_buffer.Read(dr.ClientId)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.AccessIdLength))
	dr.AccessId = make([]byte, dr.AccessIdLength)
	byte_buffer.Read(dr.AccessId)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.AccessPasswordLength))
	dr.AccessPassword = make([]byte, dr.AccessPasswordLength)
	byte_buffer.Read(dr.AccessPassword)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.AccessTypeLength))
	dr.AccessType = make([]byte, dr.AccessTypeLength)
	byte_buffer.Read(dr.AccessType)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.AccessServiceLength))
	dr.AccessService = make([]byte, dr.AccessServiceLength)
	byte_buffer.Read(dr.AccessService)

	//TODO add proper error correction into this whole method
	//maybe using the reflect module
	return nil
}




