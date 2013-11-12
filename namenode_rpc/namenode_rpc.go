package namenode_rpc

import (
	"bytes"
	"encoding/binary"
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

//basic packet structure used by the namenode
//for RPC.
type Packet interface {
	Load(buf []byte) error
}

type ResponsePacket interface {
	//load the fields from the packet
	Load(buf []byte) error

	//take the fields and turn them into bytes
	Bytes() []byte

	//common fields that need to have a Getter
	GetPacketNumber() uint32
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

type ReqPacket interface {
	Load(buf []byte) error
	GetPacketNumber() uint32
}

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


func (rp *RequestPacket) GetPacketNumber() uint32 {
	return rp.PacketNumber
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
//unit tested with data read from Wireshark
//there doesn't seem to be a formal description of the 
//protocol that Hadoop is currently using, since they seem
//to have droopped the custom "Writeables" altogether in favor of 
//Google's Protocol Buffers.
func (rp *RequestPacket) Load(buf []byte) error {
	byte_buffer := bytes.NewBuffer(buf)

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
	for i := 0; i < int(rp.ParameterNumber); i++ {

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

//utility method - reads the packet number from any kind of response packet
func GetPacketNumber(buf []byte) uint32 {
	byte_buffer := bytes.NewBuffer(buf)
	var res uint32
	binary.Read(byte_buffer, binary.BigEndian, res)

	return res
}

/* Request Packet type specific methods */

//this method should only be used on Create packets (as determined by the MethodName
//field). It returns the filepath associated with the create method.
func (rp *RequestPacket) GetCreateRequestPath() string {
	return string(rp.Parameters[0].Value)
}

//it seems that there are separate types of response packets depending on the
//kind of method that is called, so this one is for the getFileInfo
//this information has all been obtained through wireshark and some of the 
//Hadoop docs
//all in order of how they are to be read.
type GetFileInfoResponse struct {
	PacketNumber uint32
	Success uint32

	//first set of objects
	ObjectNameLength uint16
	ObjectName []byte

	//TODO not exactly sure why there are two of these...
	ObjectNameLength2 uint16
	ObjectName2 []byte

	FilePermission uint16
	FileNameLength uint16
	FileName []byte
	FileSize uint64

	IsDirectory byte
	
	BlockReplicationFactor uint16
	BlockSize uint64
	
	ModifiedTime uint64
	AccessTime uint64

	//TODO not exactly sure what the two 
	//file permission headers specify
	FilePermission2 uint16

	OwnerNameLength byte
	OwnerName []byte
	GroupNameLength byte
	GroupName []byte

	//Fields that are *not* part of the packet
	//determines if this packet has actually been
	//loaded with the Load() call
	Loaded bool

	//the buf passed to the Load() call
	LoadedBytes []byte
}

func NewGetFileInfoResponse() *GetFileInfoResponse {
	gf := GetFileInfoResponse{}
	return &gf
}

func (gf *GetFileInfoResponse) GetPacketNumber() uint32 {
	return gf.PacketNumber
}

func (gf *GetFileInfoResponse) Bytes() []byte {
	if gf.Loaded {
		return gf.LoadedBytes
	}

	//if the the packet has not been loaded, we return 
	//a nil because otherwise the resulting packet would be
	//pointless
	return nil
}

//load the response, reading each of the fields in the 
//struct described above.

//TODO neeed to do error checking on a lot of these 'reads'
func (gf *GetFileInfoResponse) Load(buf []byte) error {
	var err error

	byte_buffer := bytes.NewBuffer(buf)

	binary.Read(byte_buffer, binary.BigEndian, &(gf.PacketNumber))
	binary.Read(byte_buffer, binary.BigEndian, &(gf.Success))
	binary.Read(byte_buffer, binary.BigEndian, &(gf.ObjectNameLength))

	gf.ObjectName = make([]byte, gf.ObjectNameLength)
	byte_buffer.Read(gf.ObjectName)

	binary.Read(byte_buffer, binary.BigEndian, &(gf.ObjectNameLength2))
	gf.ObjectName2 = make([]byte, gf.ObjectNameLength2) 
	byte_buffer.Read(gf.ObjectName2)

	binary.Read(byte_buffer, binary.BigEndian, &(gf.FilePermission))

	binary.Read(byte_buffer, binary.BigEndian, &(gf.FileNameLength))
	gf.FileName = make([]byte, gf.FileNameLength)
	byte_buffer.Read(gf.FileName)

	binary.Read(byte_buffer, binary.BigEndian, &(gf.FileSize))

	gf.IsDirectory, err = byte_buffer.ReadByte()
	if (err != nil) {
		return err
	}

	binary.Read(byte_buffer, binary.BigEndian, &(gf.BlockReplicationFactor))
	binary.Read(byte_buffer, binary.BigEndian, &(gf.BlockSize))
	binary.Read(byte_buffer, binary.BigEndian, &(gf.ModifiedTime))
	binary.Read(byte_buffer, binary.BigEndian, &(gf.AccessTime))
	binary.Read(byte_buffer, binary.BigEndian, &(gf.FilePermission2))

	gf.OwnerNameLength, err = byte_buffer.ReadByte()
	if err != nil {
		return err
	}
	gf.OwnerName = make([]byte, gf.OwnerNameLength)
	byte_buffer.Read(gf.OwnerName)

	gf.GroupNameLength, err = byte_buffer.ReadByte()
	if err != nil {
		return err
	}
	gf.GroupName = make([]byte, gf.GroupNameLength)
	byte_buffer.Read(gf.GroupName)

	gf.Loaded = true
	gf.LoadedBytes = buf

	//TODO not error checking the binary.Read()s
	return nil
}



//request-response pair of the packet
//used by the processor package
type PacketPair struct {
	Request ReqPacket
	Response ResponsePacket
}

func NewPacketPair() *PacketPair {
	pp := PacketPair{}
	return &pp
}