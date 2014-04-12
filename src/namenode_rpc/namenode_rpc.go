package namenode_rpc

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

//this is the maximum size for an HDFS packet
var HDFS_PACKET_SIZE int = 640000

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

	//return the bytes that were used to load a field
	GetBuf() []byte
}

type ResponseObject struct {
	NameLength uint16
	ObjectName []byte
}

//this method figures out, given a request, what kind of new packet to build
func BuildResponsePacket(buf []byte, packetNumber uint32, currRequest *RequestPacket) ResponsePacket {
	if currRequest == nil {
		return NewGenericResponsePacket(buf, packetNumber)
	}

	return NewGenericResponsePacket(buf, packetNumber)
	/*
	switch string(currRequest.MethodName) {
	case "getFileInfo":
		gfir := NewGetFileInfoResponse()
		gfir.Load(buf)
		return gfir
	default:
		return NewGenericResponsePacket(buf, packetNumber)
	} */

}

//this is used when we aren't exactly sure (or don't care)
//what type of response a certain packet is
//so we can just stuff into some random data structure.
//implements the ResponsePacket interface
//used in processor.HandleHDFS
type GenericResponsePacket struct {
	Buf []byte
	PacketNumber uint32
	Success uint32

	ObjectNameLength1 uint16
	ObjectName1 []byte

	ObjectNameLength2 uint16
	ObjectName2 []byte

	ParameterLength uint16
	ParameterValue []byte
}

func (grp *GenericResponsePacket) GetBuf() []byte {
	return grp.Buf
}

func NewGenericResponsePacket(buf []byte, packNum uint32) *GenericResponsePacket {
	grp := GenericResponsePacket{Buf: buf, 
		PacketNumber: packNum}
	return &grp
}

func (grp *GenericResponsePacket) Load(buf []byte) error {
	byteBuffer := bytes.NewBuffer(buf)
	binary.Read(byteBuffer, binary.BigEndian, &(grp.PacketNumber))
	binary.Read(byteBuffer, binary.BigEndian, &(grp.Success))
	binary.Read(byteBuffer, binary.BigEndian, &(grp.ObjectNameLength1))
	
	grp.ObjectName1 = make([]byte, grp.ObjectNameLength1)
	byteBuffer.Read(grp.ObjectName1)
	
	binary.Read(byteBuffer, binary.BigEndian, &(grp.ObjectNameLength2))
	grp.ObjectName2 = make([]byte, grp.ObjectNameLength2)
	byteBuffer.Read(grp.ObjectName2)

	binary.Read(byteBuffer, binary.BigEndian, &(grp.ParameterLength))
	grp.ParameterValue = make([]byte, grp.ParameterLength)
	byteBuffer.Read(grp.ParameterValue)
	return nil
}

//just return the stuff stored in Buf
func (grp *GenericResponsePacket) LoadedBytes() []byte {
	return grp.Buf
}

//turn the Load()-ed structure into a byte array
func (grp *GenericResponsePacket) Bytes() []byte {
	byteBuffer := new(bytes.Buffer)
	binary.Write(byteBuffer, binary.BigEndian, grp.PacketNumber)
	binary.Write(byteBuffer, binary.BigEndian, grp.Success)

	binary.Write(byteBuffer, binary.BigEndian, grp.ObjectNameLength1)
	byteBuffer.Write(grp.ObjectName1)

	binary.Write(byteBuffer, binary.BigEndian, grp.ObjectNameLength2)
	byteBuffer.Write(grp.ObjectName2)

	binary.Write(byteBuffer, binary.BigEndian, grp.ParameterLength)
	byteBuffer.Write(grp.ParameterValue)

	return byteBuffer.Bytes()
}

func (grp *GenericResponsePacket) GetPacketNumber() uint32 {
	return grp.PacketNumber
}


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
	GetMethodName() []byte
	GetParameter(i int) Parameter
}


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

	loadedBytes []byte
	loaded bool
}

/* getter methods to fullfil the ReqPacket interface */

func (rp *RequestPacket) GetMethodName() []byte {
	return rp.MethodName
}

func (rp *RequestPacket) GetParameter(i int) Parameter {
	return rp.Parameters[i]
}

func (rp *RequestPacket) GetPacketNumber() uint32 {
	return rp.PacketNumber
}

//constructor for RequestPacket
func NewRequestPacket () *RequestPacket {
	r := RequestPacket{loaded: false}

	//values just have to be zero'ed 
	//so we can use the default constructor
	return &r
}

//load fields from the byte array buf into
//the object
//unit tested with data read from Wireshark
//there doesn't seem to be a formal description of the 
//protocol that Hadoop is currently using, since they seem
//to have dropped the custom "Writeables" altogether in favor of 
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
	rp.loaded = true
	rp.loadedBytes = buf
	return nil
}

//returns the bytes that were used for Load()
func (rp *RequestPacket) LoadedBytes() []byte {
	return rp.loadedBytes
}

//get the bytes array without padding it to match the expected length
func (rp *RequestPacket) BytesNoPad() []byte {
	byteBuf := new(bytes.Buffer)
	binary.Write(byteBuf, binary.BigEndian, rp.Length)
	binary.Write(byteBuf, binary.BigEndian, rp.PacketNumber)
	binary.Write(byteBuf, binary.BigEndian, rp.NameLength)
	byteBuf.Write(rp.MethodName)
	binary.Write(byteBuf, binary.BigEndian, rp.ParameterNumber)
	for i := 0; i<int(rp.ParameterNumber); i++ {
		param := rp.Parameters[i]
		binary.Write(byteBuf, binary.BigEndian, param.TypeLength)
		byteBuf.Write(param.Type)
		binary.Write(byteBuf, binary.BigEndian, param.ValueLength)
		byteBuf.Write(param.Value)
	}

	return byteBuf.Bytes()
}

//this method returns a byte representation of the packet
func (rp *RequestPacket) Bytes() []byte {
	var byteBuf bytes.Buffer
	
	binary.Write(&byteBuf, binary.BigEndian, rp.Length)

	binary.Write(&byteBuf, binary.BigEndian, rp.PacketNumber)

	binary.Write(&byteBuf, binary.BigEndian, rp.NameLength)

	byteBuf.Write(rp.MethodName)

	binary.Write(&byteBuf, binary.BigEndian, rp.ParameterNumber)

	fmt.Println("Parameter number: ", rp.ParameterNumber)

	for i := 0; i<int(rp.ParameterNumber); i++ {
		param := rp.Parameters[i]
		//fmt.Println("i = ", i);
		//fmt.Println("param.TypeLength: ", param.TypeLength)
		binary.Write(&byteBuf, binary.BigEndian, param.TypeLength)
		//fmt.Println("param.Type: ", string(param.Type), param.Type)
		byteBuf.Write(param.Type)

		//fmt.Println("param.ValueLength: ", param.ValueLength)
		binary.Write(&byteBuf, binary.BigEndian, param.ValueLength)
		//fmt.Println("param.Value: ", string(param.Value), param.Value)
		byteBuf.Write(param.Value)
		//fmt.Println("bytes: ", byteBuf.Bytes())
		//fmt.Println("-----------------------")
	}
	/*
	if byteBuf.Len() < int(rp.Length) {
		return byteBuf.Bytes()
	} else {
		return (byteBuf.Bytes()[0:rp.Length])
	}
	
	//statement never reached...
	return []byte{} */
	res := byteBuf.Bytes()

	//fmt.Println("Length of res: ", len(res))
	//fmt.Println("how much to pad: ", int(rp.Length) - len(res))

	//pad the rest of the packet with zeroes
	// we have to check the methodname because, for some odd reason,
	//some HDFS packets that do not have a methodName field
	//have an incorrect length
	if string(rp.MethodName) != "" {
		limit := int(rp.Length) - len(res)
		if limit > 0 {
			for j := 0; j < limit; j++ {
				res = append(res, 0)
				//fmt.Println("Lenght of res in loop: ", len(res), "j: ", j, "limit: ", limit)
			}
		}
	}

	//res = bytes.TrimRight(res, string([]byte{0}))
	return res
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

func (gfir *GetFileInfoResponse) GetBuf() []byte {
	return gfir.LoadedBytes
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

//a getListing response packet (typically returned on a dfs -ls)
//TODO POTENTIAL BUG this structure has not
//been unit tested, primarily because only the 
//packet number is currently being used
type GetListingResponse struct {
	PacketNumber uint32
	Success uint32
	
	ObjectNameLength uint16
	ObjectName []byte

	ObjectNameLength2 uint16
	ObjectName2 []byte

	ResLength uint16
	Listing []byte

	//[]buf in the call to Load()
	LoadedBytes []byte
}

func NewGetListingResponse() *GetListingResponse {
	glr := GetListingResponse{}
	return &glr
}

func (glr *GetListingResponse) Bytes() []byte {
	return glr.LoadedBytes		
}

func (glr *GetListingResponse) Load(buf []byte) error {
	glr.LoadedBytes = buf
	byteBuffer := bytes.NewBuffer(buf)

	binary.Read(byteBuffer, binary.BigEndian, &(glr.PacketNumber))
	binary.Read(byteBuffer, binary.BigEndian, &(glr.Success))

	binary.Read(byteBuffer, binary.BigEndian, &(glr.ObjectNameLength))
	glr.ObjectName = make([]byte, glr.ObjectNameLength)
	byteBuffer.Read(glr.ObjectName)
	
	binary.Read(byteBuffer, binary.BigEndian, &(glr.ObjectNameLength2))
	glr.ObjectName2 = make([]byte, glr.ObjectNameLength2)
	byteBuffer.Read(glr.ObjectName2)

	binary.Read(byteBuffer, binary.BigEndian, &(glr.ResLength))

	//the rest of the buffer should be listing message
	glr.Listing = byteBuffer.Bytes()

	//TODO POTENTIAL BUG do some error checking before we
	//just return nil
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

/*
    HDFS authentication length: 
    HDFS authorization bits: .org.apache.hadoop.hdfs.protocol.ClientProtocol\001
    HDFS length: 108
    HDFS packet number: 0
    HDFS name length: 18
    HDFS method name: getProtocolVersion
    HDFS number of parameters: 2
    HDFS name length: 16
    HDFS parameter type: java.lang.String
    HDFS name length: 46
    HDFS parameter value: org.apache.hadoop.hdfs.protocol.ClientProtocol
    HDFS name length: 4
    HDFS parameter type: long
    HDFS parameter value: 61
*/

//authentication packet structure; essentially, it is a request
//packet but has some extra details that we have to deal with
type AuthPacket struct {
	AuthenticationLength uint32
	AuthenticationBits []byte
	Length uint32
	PacketNumber uint32
	NameLength uint16
	MethodName []byte
	ParameterNumber uint32
	Parameters []Parameter

	LoadedBytes []byte
}

func NewAuthPacket() *AuthPacket {
	ap := AuthPacket{}
	return &ap
}

/*
func (ap *AuthPacket) LiveLoad(byteBuffer io.Reader) {
	rp := NewRequestPacket()
	binary.Read(byteBuffer, binary.BigEndian, &(ap.AuthenticationLength))
	ap.AuthenticationBits = make([]byte, ap.AuthenticationLength)
	byteBuffer.Read(ap.AuthenticationBits)

	restOfPacket := byteBuffer.Bytes()
	rp.Load(restOfPacket)

	//copy parts of the loaded RequestPacket into the 
	//authentication packet; we are essentially using
	//the good portions of the RequestPacket.Load() here
	ap.Length = rp.Length
	ap.PacketNumber = rp.PacketNumber
	ap.NameLength = rp.NameLength
	ap.MethodName = rp.MethodName
	ap.ParameterNumber = rp.ParameterNumber
	ap.Parameters = rp.Parameters	
} */

func (ap *AuthPacket) Bytes() []byte {
	res := new(bytes.Buffer)
	binary.Write(res, binary.BigEndian, ap.AuthenticationLength)
	res.Write(ap.AuthenticationBits)
	binary.Write(res, binary.BigEndian, ap.Length)
	binary.Write(res, binary.BigEndian, ap.PacketNumber)
	binary.Write(res, binary.BigEndian, ap.NameLength)
	res.Write(ap.MethodName)
	binary.Write(res, binary.BigEndian, ap.ParameterNumber)
	for i := 0; i<int(ap.ParameterNumber); i++ {
		binary.Write(res, binary.BigEndian, ap.Parameters[i].TypeLength)
		res.Write(ap.Parameters[i].Type)
		binary.Write(res, binary.BigEndian, ap.Parameters[i].ValueLength)
		res.Write(ap.Parameters[i].Value)
	}

	return res.Bytes()
}

func (ap *AuthPacket) Load(buf []byte) {
	ap.LoadedBytes = buf
	
	//the authentication packet is essentially
	//a request packet with some authentication stuff
	//wrapped around it, so we are going to use a 
	//request packet load method in order to load a majority
	//of the data
	byteBuffer := bytes.NewBuffer(buf)
	rp := NewRequestPacket()
	binary.Read(byteBuffer, binary.BigEndian, &(ap.AuthenticationLength))
	ap.AuthenticationBits = make([]byte, ap.AuthenticationLength)
	byteBuffer.Read(ap.AuthenticationBits)

	restOfPacket := byteBuffer.Bytes()
	rp.Load(restOfPacket)

	//copy parts of the loaded RequestPacket into the 
	//authentication packet; we are essentially using
	//the good portions of the RequestPacket.Load() here
	ap.Length = rp.Length
	ap.PacketNumber = rp.PacketNumber
	ap.NameLength = rp.NameLength
	ap.MethodName = rp.MethodName
	ap.ParameterNumber = rp.ParameterNumber
	ap.Parameters = rp.Parameters	
}

