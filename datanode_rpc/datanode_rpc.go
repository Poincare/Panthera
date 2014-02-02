package datanode_rpc

import (
	"bytes"
	"encoding/binary"
	"io"
	"reflect"
	"util"
)

//interface that DataRequest and PutDataRequest
//should satisfy
type ReqPacket interface {
	//equality comparator
	Equals(ReqPacket) bool
}

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

//special type of data request (command # = 80)
//has a different structure than DataRequeest
type PutDataRequest struct {
	ProtocolVersion uint16
	Command uint8
	BlockId uint64
	Timestamp uint64
	NumberInPipeline uint32
	RecoveryBoolean uint8

	ClientIdLength uint8
	ClientId []byte

	SourceNode uint8
	CurrNumNodes uint32

	AccessIdLength uint8
	AccessId []byte
	
	AccessPasswordLength uint8
	AccessPassword []byte

	AccessTypeLength uint8
	AccessType []byte
	
	AccessServiceLength uint8
	AccessService []byte

	ChecksumType uint8
	ChunkSize uint32
}

//constructor
func NewDataRequest() *DataRequest {
	dr := DataRequest{}
	return &dr
}

//TODO this code is incredibly bashy
//Compares two dataRequests. Note that the 
//ClientId and ClientIdLength are not compared
//since those are largely irrelevant.
func (dr *DataRequest) Equals(p *DataRequest) bool {
	if p.ProtocolVersion != dr.ProtocolVersion {
		return false
	}

	if p.Command != dr.Command {
		return false
	}

	if p.BlockId != dr.BlockId {
		return false
	}
	
	if p.Timestamp != dr.Timestamp {
		return false
	}

	if p.StartOffset != dr.StartOffset {
		return false
	}

	if p.BlockLength != dr.BlockLength {
		return false
	}

	if p.AccessIdLength != dr.AccessIdLength {
		return false
	}

	if !reflect.DeepEqual(p.AccessId, dr.AccessId) {
		return false
	}

	if p.AccessPasswordLength != dr.AccessPasswordLength {
		return false
	}

	if !reflect.DeepEqual(p.AccessPassword, dr.AccessPassword) {
		return false
	}

	if p.AccessTypeLength != dr.AccessTypeLength {
		return false
	}

	if p.AccessServiceLength != dr.AccessServiceLength {
		return false
	}

	if !reflect.DeepEqual(p.AccessService, dr.AccessService) {
		return false
	}

	return true
}


//TODO error checking in this function is just nonsensical; find some
//other way to do it.
func (dr *DataRequest) Bytes() ([]byte, error) {
	byte_buffer := new(bytes.Buffer)

	err := binary.Write(byte_buffer, binary.BigEndian, dr.ProtocolVersion)
	if err != nil {
		return []byte{}, err
	}

	err = binary.Write(byte_buffer, binary.BigEndian, dr.Command)
	if err != nil {
		return []byte{}, err
	}

	err = binary.Write(byte_buffer, binary.BigEndian, dr.BlockId)
	if err != nil {
		return []byte{}, err
	}

	err = binary.Write(byte_buffer, binary.BigEndian, dr.Timestamp)
	if err != nil {
		return []byte{}, err
	}

	err = binary.Write(byte_buffer, binary.BigEndian, dr.StartOffset)
	if err != nil {
		return []byte{}, err
	}
	err = binary.Write(byte_buffer, binary.BigEndian, dr.BlockLength)
	if err != nil {
		return []byte{}, err
	}

	err = binary.Write(byte_buffer, binary.BigEndian, dr.ClientIdLength)
	if err != nil {
		return []byte{}, err
	}
	
	_, err = byte_buffer.Write(dr.ClientId)
	if err != nil {
		return []byte{}, err
	}

	err = binary.Write(byte_buffer, binary.BigEndian, dr.AccessIdLength)
	if err != nil {
		return []byte{}, err
	}

	_, err = byte_buffer.Write(dr.AccessId)
	if err != nil {
		return []byte{}, err
	}

	err = binary.Write(byte_buffer, binary.BigEndian, dr.AccessPasswordLength)
	if err != nil {
		return []byte{}, err
	}
	
	_, err = byte_buffer.Write(dr.AccessPassword)
	if err != nil {
		return []byte{}, err
	}

	err = binary.Write(byte_buffer, binary.BigEndian, dr.AccessTypeLength)
	if err != nil {
		return []byte{}, err
	}

	_, err = byte_buffer.Write(dr.AccessType)
	if err != nil {
		return []byte{}, err
	}

	err = binary.Write(byte_buffer, binary.BigEndian, dr.AccessServiceLength)
	if err != nil {
		return []byte{}, err
	}

	_, err = byte_buffer.Write(dr.AccessService)
	if err != nil {
		return []byte{}, err
	}

	return byte_buffer.Bytes(), nil
}

type InitialRead struct {
	ProtocolVersion uint16
	Command uint8
}

//read and return the initial parameters (protocol version and command number) for a request.
//used to decide what kind of packet to parse for.
func LiveReadInitial(byte_buffer io.Reader) (*InitialRead, error) {
	ir := new(InitialRead)

	err := binary.Read(byte_buffer, binary.BigEndian, &(ir.ProtocolVersion))
	if err != nil {
		return nil, err
	}

	err = binary.Read(byte_buffer, binary.BigEndian, &(ir.Command))
	if err != nil {
		return nil, err
	}

	return ir, nil
}

//called to decide what kind of packet to load
func LoadRequestPacket(byteBuffer io.Reader) (ReqPacket, error) {
	initialRead, err := LiveReadInitial(byteBuffer)
	if err != nil {
		return nil, err
	}

	//the command determines what kind of loading we want to be doing
	switch(initialRead.Command) {
	//a command number of 81 signfies a DataRequest packet structure
	case 81:
		dataRequest := NewDataRequest()
		dataRequest.LiveLoad(byteBuffer, initialRead)
		return dataRequest, nil
	}

	return nil, nil
}

//liveload the data from the connection (or any kind of reader, e.g. byte
//buffer).
//there is no length quantity at the head of the packet, so we have 
//to load all the fields manually.
func (dr *DataRequest) LiveLoad(byte_buffer io.Reader, initialRead *InitialRead) error {
	util.DataReqLogger.Println("Live loading DataRequest...")
	
	dr.ProtocolVersion = initialRead.ProtocolVersion
	dr.Command = initialRead.Command

	//binary.Read(byte_buffer, binary.BigEndian, &(dr.ProtocolVersion))
	//binary.Read(byte_buffer, binary.BigEndian, &(dr.Command))
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

	err := binary.Read(byte_buffer, binary.BigEndian, &(dr.AccessServiceLength))
	if err != nil {
		return err
	}

	dr.AccessService = make([]byte, dr.AccessServiceLength)
	byte_buffer.Read(dr.AccessService)

	return nil
}

func (dr *DataRequest) Load(buf []byte) error {
	byte_buffer := bytes.NewBuffer(buf)

	//use the byte buffer as a Reader instance
	//to get it to Load
	ir, err := LiveReadInitial(byte_buffer)
	if err != nil {
		return err
	}
	dr.LiveLoad(byte_buffer, ir)

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

	//all the data that's been read by LiveLoad
	Buf []byte
}

//constructor
func NewDataResponse() *DataResponse {
	dr := DataResponse{}
	return &dr
}

func (dr *DataResponse) Bytes() []byte {
	return dr.Buf
}

//read the data from a connection (or any other kind of reader)
func (dr *DataResponse) LiveLoad(byte_buffer io.Reader) error {
	outputBuffer := new(bytes.Buffer)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.StatusCode))
	//write it to the outputbuffer so we can have a byte copy of
	//what we just read (used in Bytes())
	binary.Write(outputBuffer, binary.BigEndian, dr.StatusCode)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.ChecksumType))
	binary.Write(outputBuffer, binary.BigEndian, dr.ChecksumType)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.ChunkSize))
	binary.Write(outputBuffer, binary.BigEndian, dr.ChunkSize)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.ChunkOffset))
	binary.Write(outputBuffer, binary.BigEndian, dr.ChunkOffset)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.DataLength))
	binary.Write(outputBuffer, binary.BigEndian, dr.DataLength)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.InBlockOffset))
	binary.Write(outputBuffer, binary.BigEndian, dr.InBlockOffset)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.SequenceNumber))
	binary.Write(outputBuffer, binary.BigEndian, dr.SequenceNumber)

	binary.Read(byte_buffer, binary.BigEndian, &(dr.LastPacketNumber))
	binary.Write(outputBuffer, binary.BigEndian, dr.LastPacketNumber)

	err := binary.Read(byte_buffer, binary.BigEndian, &(dr.DataLength2))
	//if there was an issue reading from the buffer at this point, that 
	//means that all the following reads will likely fail.
	if err != nil {
		return err
	}

	binary.Write(outputBuffer, binary.BigEndian, dr.DataLength2)

	//TODO POTENTIAL BUG not exactly sure why/how this works...
	trash := make([]byte, 4)
	byte_buffer.Read(trash)
	outputBuffer.Write(trash)

	//TODO POTENTIAL BUG Use the first data length or the second one? 
	dr.Data = make([]byte, dr.DataLength2-1)
	byte_buffer.Read(dr.Data)	
	outputBuffer.Write(dr.Data)

	trash = make([]byte, 6)
	byte_buffer.Read(trash)

	//add in the last three bytes to the output buffer
	//so that we've copied the *entire* packet
	outputBuffer.Write(trash)

	dr.Buf = outputBuffer.Bytes()
	return err
}

func (dr *DataResponse) Load(buf []byte) error {
	byte_buffer := bytes.NewBuffer(buf)

	dr.LiveLoad(byte_buffer)

	//TODO add proper error detection to this method
	return nil
}

//a pair of request, response
type RequestResponse struct {
	Request ReqPacket
	Response *DataResponse
}

func NewRequestResponse(req *DataRequest, resp *DataResponse) *RequestResponse {
	rr := RequestResponse{
		Request: req,
		Response: resp}
	return &rr
}

