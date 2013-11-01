package namenode_rpc

import (
	"bytes"
	"encoding/binary"
	"fmt"
)



//Implementation of the RPC protocol used by 
//Hadoop to talk with the NameNode

//HeaderPacket structure defines the packet
//to be sent by the client when connected to the 
//HDFS in order to agree on version, serialization,
//etc.
type HeaderPacket struct {
	header []byte
	version uint8
	auth_method uint8
	serialization_type uint8
}

//constructor for RPC
func NewHeaderPacket() *HeaderPacket {
	r := new(HeaderPacket)
	//set the header string, sent at the front of the packet
	r.header = []byte("hrpc")
	r.version = 4;
	r.auth_method = 80;
	//default is the protobuf serialization
	r.serialization_type = 0

	return r
}

//uses the HeaderPacket structure to get a 
//furnish a full packet that can be sent
//over the network
func (header_packet *HeaderPacket) Bytes() []byte {
	buffer := new(bytes.Buffer)
	buffer.Write(header_packet.header)

	binary.Write(buffer, binary.BigEndian, header_packet.version)
	binary.Write(buffer, binary.BigEndian, header_packet.auth_method)
	binary.Write(buffer, binary.BigEndian, header_packet.serialization_type)
	
	return buffer.Bytes()
}

//Structure used to receive RPC response
//from HDFS
//all the int64's are actually varints
//from the Google Protocol Buffer, which
//happen to be implemented in golang
//TODO this hasn't been tested at all, since 
//it is more or less useless at the moment
type ResponsePacket struct {
	HeaderLength int64
	header_proto []byte
	rpc_length uint32
	serialized_rpc []byte
}

func NewResponsePacket() *ResponsePacket {
	mp := ResponsePacket{HeaderLength: 0, rpc_length: 0}

	return &mp
}

//reads the fields from buf into the object
//e.g. the varint header length is read
//from the byte array/slice
func (mp *ResponsePacket) Load(buf []byte) error {
	byte_buffer := bytes.NewBuffer(buf)

	//TODO ignoring error is probably a terrible idea
	var err error
	mp.HeaderLength, err = binary.ReadVarint(byte_buffer)

	if(err != nil) {
		return err
	}

	//read in the header proto
	mp.header_proto = make([]byte, mp.HeaderLength)
	byte_buffer.Read(mp.header_proto)

	//not sure whether or not to use LittleEndian
	//or BigEndian here. Assuming it is BigEndian
	//since that is *supposed* to be network order.
	//Nevertheless, this should read the rpc_length
	//from the byte_buffer
	binary.Read(byte_buffer, binary.BigEndian, &(mp.rpc_length))

	//read in the serialized rpc byte buf
	mp.serialized_rpc = make([]byte, mp.rpc_length)

	return nil
}


/* TODO this is ALL NONSENSE */

type Parameter struct {
	TypeLength uint16
	Type []byte
	ValueLength uint16
	Value []byte
}

func NewParameter() *Parameter {
	p := Parameter{}
	return &p
}

//this is a packet that a client to HDFS 
//sends to a NameNode to execute some RPC code

/* This has been derived from what I've reverse 
engineered w/ Wireshark - Hadoop doesn't seem to
have take tne trouble to document the protocol
that they've switched to */
type RequestPacket struct {
	Length uint32
	PacketNumber uint32
	
	//not certain if this is supposed to be
	//one or two bytes
	NameLength uint16

	//name of the method being called (w/ RPC)
	MethodName []byte

	//number of Parameters sent
	ParameterNumber uint32

	//list of the parameters sent (see Parameter type)
	Parameters []Parameter
}

//constructor for RequestPacket
func NewRequestPacket () *RequestPacket {
	r := RequestPacket{}

	//values just have to be zero'ed 
	//so we can use the default constructor
	return &r
}

//load fields from the byte array buf into
//the object
//how should unit test this?!
func (rp *RequestPacket) Load(buf []byte) error {
	byte_buffer := bytes.NewBuffer(buf)
	fmt.Println("Request packet, load BB: ", buf)

	binary.Read(byte_buffer, binary.BigEndian, &(rp.Length))
	binary.Read(byte_buffer, binary.BigEndian, &(rp.PacketNumber))
	binary.Read(byte_buffer, binary.BigEndian, &(rp.NameLength))

	//make the method name buffer have enough space
	//to take in the method name
	rp.MethodName = make([]byte, rp.NameLength)
	byte_buffer.Read(rp.MethodName)

	binary.Read(byte_buffer, binary.BigEndian, &(rp.ParameterNumber))

	rp.Parameters = make([]Parameter, rp.ParameterNumber)
	//loop through and read all of the parameters
	for i := 1; i < int(rp.ParameterNumber); i++ {
		rp.Parameters[i] = *(NewParameter())
		//now read in all the fields one by one
		binary.Read(byte_buffer, binary.BigEndian, &(rp.Parameters[i].TypeLength))

		//create space for and read in the type of this parameter
		rp.Parameters[i].Type = make([]byte, rp.Parameters[i].TypeLength)
		byte_buffer.Read(rp.Parameters[i].Type)


		binary.Read(byte_buffer, binary.BigEndian, &(rp.Parameters[i].ValueLength))

		//create space for and read in the value of this parameter
		rp.Parameters[i].Value = make([]byte, rp.Parameters[i].ValueLength)
		byte_buffer.Read(rp.Parameters[i].Value)
	}

	return nil
}

