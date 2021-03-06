package datanode_rpc

import (
	"bytes"
	"encoding/binary"
	"io"
	"reflect"
	"util"
	"fmt"
	"errors"
)

//interface that DataRequest and PutDataRequest
//should satisfy
type ReqPacket interface {
	//equality comparator
	Bytes() ([]byte, error)
	Equals(ReqPacket) bool
}


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

//the type of packet that is used to the tell
//the datanode the contents of the files we
//want to put on the DataNode
type PutFileDataRequest struct {
	//note: we are only going to use a byte buffer 
	//for this packet type because there is no
	//point in trying to break down the packet
	//because the fields don't mean anything to
	//Panthera
	Buffer *bytes.Buffer
}

func NewPutFileDataRequest() *PutFileDataRequest {
	pfdr := PutFileDataRequest{}
	return &pfdr
}

func (p *PutFileDataRequest) Bytes() ([]byte, error) {
	return p.Buffer.Bytes(), nil
}

func (p *PutFileDataRequest) Equals(r ReqPacket) bool {
	switch r.(type) {
	case *PutFileDataRequest:
		r := r.(*PutFileDataRequest)
		rBytes, _ := r.Bytes()
		pBytes, _ := p.Bytes()
		if !reflect.DeepEqual(rBytes, pBytes) {
			return true
		}
	default:
		return false
	}

	return false
}

func (p *PutFileDataRequest) LiveLoad(reader io.Reader) error {
	buf := make([]byte, 65536)
	bytesRead, err := reader.Read(buf)
	if err != nil {
		return err
	}

	buf = buf[0:bytesRead]
	p.Buffer = bytes.NewBuffer(buf)

	return nil
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
func NewPutDataRequest() *PutDataRequest {
	dr := PutDataRequest{}
	return &dr
}

//compare tow putDataRequests
//this switch is likely pretty slow
func (p *PutDataRequest) Equals(r ReqPacket) bool {
	switch r.(type) {
	default:
		//return false if the types do not match
		return false
		
	case *PutDataRequest: 
		r := r.(*PutDataRequest)
		//if the type is correct, than we have to do more comparisons
		if p.ProtocolVersion != r.ProtocolVersion {
			return false
		}

		if p.Command != r.Command {
			return false
		}

		if p.BlockId != r.BlockId {
			return false
		}

		//NOTE: the equals method does not compare the timestamps
		//as these can differ even between the same request packet.
		if p.NumberInPipeline != r.NumberInPipeline {
			return false
		}

		if p.RecoveryBoolean != r.RecoveryBoolean {
			return false
		}

		if p.ClientIdLength != r.ClientIdLength {
			return false
		}

		if !reflect.DeepEqual(p.ClientId, r.ClientId) {
			return false
		}

		if p.SourceNode != r.SourceNode {
			return false
		}

		if p.CurrNumNodes != r.CurrNumNodes {
			return false
		}
		return true
	}

	return false
}

//return the byte content of a loaded PutDataRequest struct
func (p *PutDataRequest) Bytes() ([]byte, error) {
	byteBuf := new(bytes.Buffer)
	binary.Write(byteBuf, binary.BigEndian, p.ProtocolVersion)
	binary.Write(byteBuf, binary.BigEndian, p.Command)
	binary.Write(byteBuf, binary.BigEndian, p.BlockId)
	binary.Write(byteBuf, binary.BigEndian, p.Timestamp)
	binary.Write(byteBuf, binary.BigEndian, p.NumberInPipeline)
	binary.Write(byteBuf, binary.BigEndian, p.RecoveryBoolean)

	binary.Write(byteBuf, binary.BigEndian, p.ClientIdLength)
	byteBuf.Write(p.ClientId)
	
	binary.Write(byteBuf, binary.BigEndian, p.SourceNode)
	binary.Write(byteBuf, binary.BigEndian, p.CurrNumNodes)
	binary.Write(byteBuf, binary.BigEndian, p.AccessIdLength)
	byteBuf.Write(p.AccessId)

	binary.Write(byteBuf, binary.BigEndian, p.AccessPasswordLength)
	byteBuf.Write(p.AccessPassword)
	
	binary.Write(byteBuf, binary.BigEndian, p.AccessTypeLength)
	byteBuf.Write(p.AccessType)
	
	binary.Write(byteBuf, binary.BigEndian, p.AccessServiceLength)
	byteBuf.Write(p.AccessService)

	binary.Write(byteBuf, binary.BigEndian, p.ChecksumType)
	binary.Write(byteBuf, binary.BigEndian, p.ChunkSize)

	return byteBuf.Bytes(), nil
}

//constructor
func NewDataRequest() *DataRequest {
	dr := DataRequest{}
	return &dr
}

//Compares two dataRequests. Note that the 
//ClientId and ClientIdLength are not compared
//since those are largely irrelevant.
func (dr *DataRequest) Equals(p ReqPacket) bool {
	switch p.(type) {
	default:
		return false
	case *DataRequest:
		p := p.(*DataRequest)
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
	return false
}


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

	err = binary.Write(byte_buffer, binary.BigEndian, 
	dr.AccessPasswordLength)
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

	util.DebugLogger.Println("Starting read of ProtocolVersion...")
	err := binary.Read(byte_buffer, binary.BigEndian, &(ir.ProtocolVersion))
	util.DebugLogger.Println("Finished read of ProtocolVersion. err: ", err)

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
//Note: prevPDR is a boolean value which tells us whether or not the 
//the previous request was a PutDataRequest.
func LoadRequestPacket(byteBuffer io.Reader, prevPDR bool) (ReqPacket, 
error) {
	if prevPDR {
		putFileDataRequest := NewPutFileDataRequest()
		putFileDataRequest.LiveLoad(byteBuffer)
		return putFileDataRequest, nil
	}

	util.DebugLogger.Println("Starting LiveReadInitial()...")
	initialRead, err := LiveReadInitial(byteBuffer)
	util.DebugLogger.Println("InitialRead structure: ", initialRead)
	util.DebugLogger.Println("LiveReadInitial() completed. Error value: ", 
	err)

	/*
	if initialRead.Command == 0 && initialRead.ProtocolVersion == 0 {
		err := errors.New("Invalid initialRead, assuming socket closed.")
		return nil, err
	} */

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
	case 80:
		putDataRequest := NewPutDataRequest()
		putDataRequest.LiveLoad(byteBuffer, initialRead)
		putDataRequestBytes, _ := putDataRequest.Bytes()
		util.DataReqLogger.Println("PutDataRequest bytes: ", putDataRequestBytes)
		return putDataRequest, nil
	}

	return nil, nil
}

//liveload the data from the connection (or any kind of io.Reader, e.g. bytes.Buffer)
//There is no length quantity at the end of the packet, so we have to load all of the 
//field one by one
func (p *PutDataRequest) LiveLoad(byte_buffer io.Reader, 
	initialRead *InitialRead) error {
	util.DataReqLogger.Println("Live loading PutDataRequest...")
	
	p.ProtocolVersion = initialRead.ProtocolVersion
	p.Command = initialRead.Command

	binary.Read(byte_buffer, binary.BigEndian, &(p.BlockId))
	binary.Read(byte_buffer, binary.BigEndian, &(p.Timestamp))
	binary.Read(byte_buffer, binary.BigEndian, &(p.NumberInPipeline))
	binary.Read(byte_buffer, binary.BigEndian, &(p.RecoveryBoolean))

	binary.Read(byte_buffer, binary.BigEndian, &(p.ClientIdLength))
	p.ClientId = make([]byte, p.ClientIdLength)
	byte_buffer.Read(p.ClientId)
	
	binary.Read(byte_buffer, binary.BigEndian, &(p.SourceNode))
	binary.Read(byte_buffer, binary.BigEndian, &(p.CurrNumNodes))
	
	binary.Read(byte_buffer, binary.BigEndian, &(p.AccessIdLength))
	p.AccessId = make([]byte, p.AccessIdLength)
	byte_buffer.Read(p.AccessId)

	binary.Read(byte_buffer, binary.BigEndian, &(p.AccessPasswordLength))
	p.AccessPassword = make([]byte, p.AccessPasswordLength)
	byte_buffer.Read(p.AccessPassword)

	binary.Read(byte_buffer, binary.BigEndian, &(p.AccessTypeLength))
	p.AccessType = make([]byte, p.AccessTypeLength)
	byte_buffer.Read(p.AccessType)

	binary.Read(byte_buffer, binary.BigEndian, &(p.AccessServiceLength))
	p.AccessService = make([]byte, p.AccessServiceLength)
	byte_buffer.Read(p.AccessService)

	binary.Read(byte_buffer, binary.BigEndian, &(p.ChecksumType))
	binary.Read(byte_buffer, binary.BigEndian, &(p.ChunkSize))
	
	return nil
}

//live load the full packet, including the InitialRead bits
//LiveLoad(), on the other hand, requires an existing InitialRead
//value.
func (dr *DataRequest) FullLiveLoad(byte_buffer io.Reader) error {
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

	err := binary.Read(byte_buffer, binary.BigEndian, &(dr.AccessServiceLength))
	if err != nil {
		return err
	}

	dr.AccessService = make([]byte, dr.AccessServiceLength)
	byte_buffer.Read(dr.AccessService)
	return nil
}

//liveload the data from the connection (or any kind of reader, e.g. byte
//buffer).
//there is no length quantity at the head of the packet, so we have 
//to load all the fields manually.
func (dr *DataRequest) LiveLoad(byte_buffer io.Reader, 
	initialRead *InitialRead) error {
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

	return nil
}

type ResponsePacket interface {
	Bytes() ([]byte, error)
	LiveLoad(io.Reader)
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

func (dr *DataResponse) Bytes() ([]byte, error) {
	return dr.Buf, nil
}

//read the data from a connection (or any other kind of reader)
func (dr *DataResponse) LiveLoad(byte_buffer io.Reader) error {
	outputBuffer := new(bytes.Buffer)

	err := binary.Read(byte_buffer, binary.BigEndian, &(dr.StatusCode))
	if err != nil {
		return err
	}
	//write it to the outputbuffer so we can have a byte copy of
	//what we just read (used in Bytes())
	binary.Write(outputBuffer, binary.BigEndian, dr.StatusCode)

	err = binary.Read(byte_buffer, binary.BigEndian, &(dr.ChecksumType))
	if err != nil {
		return err
	}
	binary.Write(outputBuffer, binary.BigEndian, dr.ChecksumType)

	err = binary.Read(byte_buffer, binary.BigEndian, &(dr.ChunkSize))
	if err != nil {
		return err
	}
	binary.Write(outputBuffer, binary.BigEndian, dr.ChunkSize)

	err = binary.Read(byte_buffer, binary.BigEndian, &(dr.ChunkOffset))
	if err != nil {
		return err
	}
	binary.Write(outputBuffer, binary.BigEndian, dr.ChunkOffset)

	err = binary.Read(byte_buffer, binary.BigEndian, &(dr.DataLength))
	if err != nil {
		return err
	}
	binary.Write(outputBuffer, binary.BigEndian, dr.DataLength)

	err = binary.Read(byte_buffer, binary.BigEndian, &(dr.InBlockOffset))
	if err != nil {
		return err
	}
	binary.Write(outputBuffer, binary.BigEndian, dr.InBlockOffset)

	err = binary.Read(byte_buffer, binary.BigEndian, &(dr.SequenceNumber))
	if err != nil {
		return err
	}
	binary.Write(outputBuffer, binary.BigEndian, dr.SequenceNumber)

	err = binary.Read(byte_buffer, binary.BigEndian, &(dr.LastPacketNumber))
	if err != nil {
		return err
	}
	binary.Write(outputBuffer, binary.BigEndian, dr.LastPacketNumber)

	err = binary.Read(byte_buffer, binary.BigEndian, &(dr.DataLength2))
	//if there was an issue reading from the buffer at this point, that 
	//means that all the following reads will likely fail.
	if err != nil {
		return err
	}

	binary.Write(outputBuffer, binary.BigEndian, dr.DataLength2)

	trash := make([]byte, 4)
	byte_buffer.Read(trash)
	outputBuffer.Write(trash)

	
	if dr.DataLength2-1 > 66000 {
		return errors.New("allocation size too large.")
	}
	dr.Data = make([]byte, dr.DataLength2-1)
	
	fmt.Println("Allocated dr.Data, size: ", len(dr.Data))

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

	
	return nil
}

//response that is delivered if a PutDataRequest is the
//current request
type PutDataResponse struct {
	PiplineStatus uint8
}

func (p *PutDataResponse) LiveLoad(byte_buffer io.Reader) error {
	binary.Read(byte_buffer, binary.BigEndian, &(p.PiplineStatus))
	return nil
}

func (p *PutDataResponse) Bytes() ([]byte, error) {
	byteBuf := new(bytes.Buffer)
	
	err := binary.Write(byteBuf, binary.BigEndian, &(p.PiplineStatus))
	if err != nil {
		return []byte{}, err
	}

	return byteBuf.Bytes(), nil
}

func NewPutDataResponse() *PutDataResponse {
	pdr := PutDataResponse{}
	return &pdr
}

//response that is delivered just after a PutFileDataRequest is the
//current request
type NullPacket struct {
	NullBytes []byte
}

func NewNullPacket() *NullPacket {
	np := NullPacket{
		NullBytes: []byte{0, 0, 0, 0, 0, 0, 0, 1}}

	return &np
}

func (np *NullPacket) LiveLoad(reader io.Reader) error {
	buf := make([]byte, 8)
	binary.Read(reader, binary.BigEndian, buf)
	np.NullBytes = buf
	return nil
}

func (n *NullPacket) Bytes() ([]byte, error) {
	return n.NullBytes, nil
}

//a pair of request, response
type RequestResponse struct {
	Request ReqPacket
	Response *DataResponse
}

func NewRequestResponse(req ReqPacket, resp *DataResponse) *RequestResponse {
	rr := RequestResponse{
		Request: req,
		Response: resp}
	return &rr
}
