package writables

import (
	//go packages
	"reflect"

	//local packages

)

/***
** Status codes
**/
var OP_WRITE_BLOCK int8 = 80
var OP_READ_BLOCK int8 = 81

var OP_READ_METADATA int8 = 82
var OP_REPLACE_BLOCK int8 = 83
var OP_COPY_BLOCK int8 = 84

var OP_BLOCK_CHECKSUM int8 = 85

var OP_STATUS_SUCCESS int8 = 0
var OP_STATUS_ERROR int8 = 1
var OP_STATUS_ERROR_CHECKSUM int8 = 2
var OP_STATUS_ERROR_INVALID int8 = 3
var OP_STATUS_ERROR_EXISTS int8 = 4
var OP_STATUS_ERROR_ACCESS_TOKEN int8 = 5
var OP_STATUS_CHECKSUM_OK int8 = 6

/***
** Checksum types
**/
var CHECKSUM_NULL int8 = 0;
var CHECKSUM_CRC32 int8 = 1;

var CHECKSUM_NULL_SIZE int8 = 0
var CHECKSUM_CRC32_SIZE int8 = 4
  
/**
* PipelineStatus
**/

//writable
type PipelineStatus struct {
	//read/written as a byte
	Status int8
}

func NewPipelineStatus() *PipelineStatus {
	//set to success by default
	p := PipelineStatus{Status: OP_STATUS_SUCCESS}
	return &p
}

func (p *PipelineStatus) Read(reader Reader) error {
	var err error
	p.Status, err = ReadByte(reader)
	if err != nil {
		return err
	}

	return nil
}

func (p *PipelineStatus) Write(writer Writer) error {
	var err error
	err = WriteByte(p.Status, writer)
	if err != nil {
		return err
	}

	return nil
}

/*
* PipelineAck
*/

//writable
type PipelineAck struct {
	//read/written as a "long"
	SeqNo uint64

	//read/written as a "short"
	NumOfReplies uint16

	//read/written as a bunch of "short"s
	Replies []uint16
}

func NewPipelineAck() *PipelineAck {
	pa := PipelineAck{}
	return &pa
}

func (p *PipelineAck) Read(reader Reader) error {
	var err error

	p.SeqNo, err = ReadLongInt(reader)
	if err != nil {
		return err
	}

	p.NumOfReplies, err = ReadShortInt(reader)
	if err != nil {
		return err
	}

	//allocate space for the replies then read them all in
	p.Replies = make([]uint16, p.NumOfReplies)
	for i := 0; i<int(p.NumOfReplies); i++ {
		p.Replies[i], err = ReadShortInt(reader)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *PipelineAck) Write(writer Writer) error {
	var err error

	err = WriteLongInt(p.SeqNo, writer)
	if err != nil {
		return err
	}

	err = WriteShortInt(p.NumOfReplies, writer)
	if err != nil {
		return err
	}

	for i := 0; i<int(p.NumOfReplies); i++ {
		err = WriteShortInt(p.Replies[i], writer)
		if err != nil {
			return err
		}
	}
	return nil
}

//writable
type DataRequestHeader struct {
	//short
	Version uint16

	//this is the type of command
	//byte
	Op int8
}

func NewDataRequestHeader() *DataRequestHeader {
	d := DataRequestHeader{}
	return &d
}

func (d *DataRequestHeader) Read(reader Reader) error {
	var err error

	d.Version, err = ReadShortInt(reader)
	if err != nil {
		return err
	}

	d.Op, err = ReadByte(reader)
	if err != nil {
		return err
	}
	
	return nil
}

func (d *DataRequestHeader) Write(writer Writer) error {
	var err error
	err = WriteShortInt(d.Version, writer)
	if err != nil {
		return err
	}

	err = WriteByte(d.Op, writer)
	return err
}

/**
** Text 
*/
type Text struct {
	//VInt
	Length int64

	//byte array of length Text.Length
	Bytes []byte
}

func (t *Text) Equals(e *Text) bool {
	if t.Length != e.Length {
		return false
	}

	if !reflect.DeepEqual(t.Bytes, e.Bytes) {
		return false
	}

	return true
}

func NewText() *Text {
	t := Text{Length: 0, 
		Bytes: []byte{}}
	return &t
}

func (t *Text) Read(reader Reader) error {
	err := GenericRead(t, reader)
	return err
}

func (t *Text) Write(writer Writer) error {
	err := GenericWrite(t, writer)
	return err
}

/**
** Token
*/
type Token struct {
	//read in as a VInt
	IdentifierLength int64

	//read as series of bytes,
	//has a length of Token.IdentifierLength
	Identifier []int8

	//VInt
	PasswordLength int64

	//read as series of bytes
	//has a length of Token.PasswordLength
	Password []int8

	//GenericRead will take care of reading these
	Kind *Text
	Service *Text
	
}

//comparator function
func (t *Token) Equals(e *Token) bool {
	if t.IdentifierLength != e.IdentifierLength {
		return false
	}

	if !reflect.DeepEqual(t.Identifier, e.Identifier) {
		return false
	}

	if t.PasswordLength != e.PasswordLength {
		return false
	}

	if !reflect.DeepEqual(t.Password, e.Password) {
		return false
	}

	if !t.Kind.Equals(e.Kind) {
		return false
	}

	if !t.Service.Equals(e.Service) {
		return false
	}

	return true
}

func (t *Token) Read(reader Reader) error {
	return GenericRead(t, reader)
}

func (t *Token) Write(writer Writer) error {
	return GenericWrite(t, writer)
}

func NewToken() *Token {
	t := Token{}
	t.Kind = NewText()
	t.Service = NewText()
	return &t
}

//writable
//header when the DataRequest has Op of OP_READ_BLOCK
type ReadBlockHeader struct {
	//long
	BlockId uint64

	//long
	Timestamp uint64

	//long
	StartOffset uint64

	//long
	Length uint64

	ClientName *Text

	AccessToken *Token
}

func NewReadBlockHeader() *ReadBlockHeader {
	r := ReadBlockHeader{}
	r.ClientName = NewText()
	r.AccessToken = NewToken()
	return &r
}

//comparing two ReadBlockHeaders (used for caching)
func (r *ReadBlockHeader) Equals(e *ReadBlockHeader) bool {
	if r.BlockId != e.BlockId {
		return false
	}

	//note: the timestamp does not 
	//need to be compared because it is not
	//relevant to the equivalency of two request
	//packets

	if r.StartOffset != e.StartOffset {
		return false
	}

	if r.Length != e.Length {
		return false
	}

	if !r.ClientName.Equals(e.ClientName) {
		return false
	}

	if !r.AccessToken.Equals(e.AccessToken) {
		return false
	}

	return true
}

//we can't use a genericRead() call for this 
//Read method implementation
func (r *ReadBlockHeader) Read(reader Reader) error {
	var err error

	r.BlockId, err = ReadLongInt(reader)
	if err != nil {
		return err
	}

	r.Timestamp, err = ReadLongInt(reader)
	if err != nil {
		return err
	}

	r.StartOffset, err = ReadLongInt(reader)
	if err != nil {
		return err
	}

	r.Length, err = ReadLongInt(reader)
	if err != nil {
		return err
	}

	err = r.ClientName.Read(reader)
	if err != nil {
		return err
	}

	err = r.AccessToken.Read(reader)
	if err != nil {
		return err
	}

	return nil
}

func (r *ReadBlockHeader) Write(writer Writer) error {
	return GenericWrite(r, writer)
}

type ChecksumHeader struct {
	//byte
	Type int8

	//int
	BytesPerChecksum uint32
}

func NewChecksumHeader() *ChecksumHeader {
	c := ChecksumHeader{}
	return &c
}

func (c *ChecksumHeader) Read(reader Reader) error {
	return GenericRead(c, reader)
}

func (c *ChecksumHeader) Write(writer Writer) error {
	return GenericWrite(c, writer)
}

//get the size of the ChecksumHeader (depends on type)
func (c *ChecksumHeader) Size() int8 {
	switch c.Type {
	case CHECKSUM_NULL:
		return CHECKSUM_NULL_SIZE
	case CHECKSUM_CRC32:
		return CHECKSUM_CRC32_SIZE
	}

	//this should never occur
	return 0
}

/**
* BlockPacket
* Has the contents of a block
*/
type BlockPacket struct {
	//int
	PacketLength uint32 //4

	//long
	Offset uint64 //8

	//long
	SeqNo uint64 //8

	//byte
	LastPacket int8 //1

	//Length
	Length uint32 //4

	//byte array
	ChecksumData []byte

	//byte array []byte
	Data []byte

	//have to have read the header before
	//reading the data
	header *BlockResponseHeader
}

func NewBlockPacket(header *BlockResponseHeader) *BlockPacket {
	b := BlockPacket{
		header: header}
	return &b
}

func (r *BlockPacket) checksumLen() int {
	return int(r.numChunks() * int(r.header.Checksum.Size()))
}

func (r *BlockPacket) numChunks() int {
	return int((r.Length + r.header.Checksum.BytesPerChecksum - 1)/r.header.Checksum.BytesPerChecksum)
}

func (r *BlockPacket) Read(reader Reader) error {
	var err error

	r.PacketLength, err = ReadInt(reader)
	if err != nil {
		return err
	}

	r.Offset, err = ReadLongInt(reader)
	if err != nil {
		return err
	}

	r.SeqNo, err = ReadLongInt(reader)
	if err != nil {
		return err
	}

	r.LastPacket, err = ReadByte(reader)
	if err != nil {
		return err
	}

	r.Length, err = ReadInt(reader)
	if err != nil {
		return err
	}

	//read in the checksum bytes
	checksumLen := r.checksumLen()
	r.Data, _, err = ReadBytesIOInfo(int64(checksumLen), reader)
	if err != nil {
		return err
	}

	//read some more the actual data
	var interm []byte
	chunkLen := int64(r.Length)
	interm, _, err = ReadBytesIOInfo(int64(chunkLen), reader)
	r.Data = append(r.Data, interm...)

	if err != nil {
		return err
	}

	return nil
}

func (r *BlockPacket) Write(writer Writer) error {
	var err error
	err = WriteInt(r.PacketLength, writer)
	if err != nil {
		return err
	}

	err = WriteLongInt(r.Offset, writer)
	if err != nil {
		return err
	}

	err = WriteLongInt(r.SeqNo, writer)
	if err != nil {
		return err
	}

	err = WriteByte(r.LastPacket, writer)
	if err != nil {
		return err
	}

	err = WriteInt(r.Length, writer)
	if err != nil {
		return err
	}

	_, err = WriteBytesInfo(r.Data, int64(r.PacketLength), writer)
	if err != nil {
		return err
	}

	return nil
}

func (b *BlockPacket) GenericWrite(writer Writer) error {
	return GenericWrite(b, writer)
}

/**
* BlockResponseHeader
*/ 
type BlockResponseHeader struct {
	//short int
	Status uint16

	Checksum *ChecksumHeader

	//long
	ChunkOffset uint64
}


//figure out packet size
func (header *BlockResponseHeader) AdjustChecksumSize(dataLen int64) int64 {

	bytesPerChecksum := int64(header.Checksum.BytesPerChecksum)
	checksumSize := int64(header.Checksum.Size())

	requiredSize := ((dataLen + bytesPerChecksum - 1)/bytesPerChecksum)*checksumSize
	return requiredSize
}

func NewBlockResponseHeader() *BlockResponseHeader {
	b := BlockResponseHeader{}
	b.Checksum = NewChecksumHeader()
	return &b
}

func (r *BlockResponseHeader) Read(reader Reader)  error{
	var err error
	r.Status, err = ReadShortInt(reader)
	if err != nil {
		return err
	}

	err = r.Checksum.Read(reader)
	if err != nil {
		return err
	}

	r.ChunkOffset, err = ReadLongInt(reader)
	if err != nil {
		return err
	}

	return nil
}

func (b *BlockResponseHeader) Write(writer Writer) error {
	return GenericWrite(b, writer)
}

/**
** ReadBlockResponse
** The response to a OP_READ_BLOCK request.
*/
type ReadBlockResponse struct {
	//short int (not clear why - almost all the response
	//codes are int8's, but for some reason, Hadoop devs
	//decided that this one should be a short int)
	Status uint16

	Checksum *ChecksumHeader

	//long
	ChunkOffset uint64

	//int
	PacketLength uint32

	//long
	Offset uint64

	//long
	SeqNo uint64

	//byte. Specifies if this is
	//the last packet in reading this
	//particular block - very important
	//since blocks in HDFS are very large
	//(usually 64MB)
	LastPacket int8

	//int
	Length uint32

	//byte array
	Data []byte
}

func NewReadBlockResponse() *ReadBlockResponse {
	r := ReadBlockResponse{}
	r.Checksum = NewChecksumHeader()
	return &r
}

//this has to be a manual read because GenericRead()
//is choking on the Checksum
func (r *ReadBlockResponse) Read(reader Reader) error {
	var err error
	r.Status, err = ReadShortInt(reader)
	if err != nil {
		return err
	}

	err = r.Checksum.Read(reader)
	if err != nil {
		return err
	}

	r.ChunkOffset, err = ReadLongInt(reader)
	if err != nil {
		return err
	}

	r.PacketLength, err = ReadInt(reader)
	if err != nil {
		return err
	}

	r.Offset, err = ReadLongInt(reader)
	if err != nil {
		return err
	}

	r.SeqNo, err = ReadLongInt(reader)
	if err != nil {
		return err
	}

	r.LastPacket, err = ReadByte(reader)
	if err != nil {
		return err
	}

	r.Length, err = ReadInt(reader)
	if err != nil {
		return err
	}

	r.Data, err = ReadBytesIO(int64(r.Length), reader)
	if err != nil {
		return err
	}

	return nil
}

func (r *ReadBlockResponse) GenericRead(reader Reader) error {
	return GenericRead(r, reader)
}

func (r *ReadBlockResponse) Write(writer Writer) error {
	return GenericWrite(r, writer)
}

