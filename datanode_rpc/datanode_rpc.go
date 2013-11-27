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

func (dr *DataRequest) Bytes() []byte {
	byte_buffer := new(bytes.Buffer)

	binary.Write(byte_buffer, binary.BigEndian, dr.ProtocolVersion)
	binary.Write(byte_buffer, binary.BigEndian, dr.Command)
	binary.Write(byte_buffer, binary.BigEndian, dr.BlockId)
	binary.Write(byte_buffer, binary.BigEndian, dr.Timestamp)
	binary.Write(byte_buffer, binary.BigEndian, dr.StartOffset)
	binary.Write(byte_buffer, binary.BigEndian, dr.BlockLength)

	binary.Write(byte_buffer, binary.BigEndian, dr.ClientIdLength)
	byte_buffer.Write(dr.ClientId)

	binary.Write(byte_buffer, binary.BigEndian, dr.AccessIdLength)
	byte_buffer.Write(dr.AccessId)

	binary.Write(byte_buffer, binary.BigEndian, dr.AccessPasswordLength)
	byte_buffer.Write(dr.AccessPassword)

	binary.Write(byte_buffer, binary.BigEndian, dr.AccessTypeLength)
	byte_buffer.Write(dr.AccessType)

	binary.Write(byte_buffer, binary.BigEndian, dr.AccessServiceLength)
	byte_buffer.Write(dr.AccessService)

	return byte_buffer.Bytes()
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

/* the response format that contains the file requested from 
the data node */

//NOTE the responses from HDFS can actually be split
//into multiple packets. SequenceNumber and LastPacketNumber
//determine whether or not this is the last packet
type DataResponse struct {
	StatusCode uint16
	ChecksumType uint8
	ChunkSize uint32
	ChunkOffset uint64
	DataLength uint32
	InBlockOffset uint64
	SequenceNumber uint64
	LastPacketNumber uint8
	
	//TODO not exactly sure *why* there are two values
	//for the length of the data
	DataLength2 uint32
	Data []byte
}

//constructor
func NewDataResponse() *DataResponse {
	dr := DataResponse{}
	return &dr
}

func (dr *DataResponse) Load(buf []byte) error {
	byte_buffer := bytes.NewBuffer(buf)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.StatusCode))
	binary.Read(byte_buffer, binary.BigEndian, &(dr.ChecksumType))
	binary.Read(byte_buffer, binary.BigEndian, &(dr.ChunkSize))
	binary.Read(byte_buffer, binary.BigEndian, &(dr.ChunkOffset))
	binary.Read(byte_buffer, binary.BigEndian, &(dr.DataLength))
	binary.Read(byte_buffer, binary.BigEndian, &(dr.InBlockOffset))
	binary.Read(byte_buffer, binary.BigEndian, &(dr.SequenceNumber))
	binary.Read(byte_buffer, binary.BigEndian, &(dr.LastPacketNumber))

	binary.Read(byte_buffer, binary.BigEndian, &(dr.DataLength2))

	//TODO POTENTIAL BUG not exactly sure why/how this works...
	trash := make([]byte, 4)
	byte_buffer.Read(trash)

	//TODO POTENTIAL BUG Use the first data length or the second one? 
	dr.Data = make([]byte, dr.DataLength2-1)
	_, err := byte_buffer.Read(dr.Data)
	if err != nil {
		return err
	}

	//TODO add proper error detection to this method
	return nil
}

//a pair of request, response
type RequestResponse struct {
	Request *DataRequest
	Response *DataResponse
}

func NewRequestResponse(req *DataRequest, resp *DataResponse) *RequestResponse {
	rr := RequestResponse{
		Request: req,
		Response: resp}
	return &rr
}

