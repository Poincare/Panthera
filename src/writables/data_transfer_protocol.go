package writables

import (
	//local packages
	"util"
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
	util.TempLogger.Println("Reading DataRequestHeader (in Read())... ")
	var err error

	d.Version, err = ReadShortInt(reader)
	util.TempLogger.Println("Finished reading Version.")
	if err != nil {
		return err
	}

	d.Op, err = ReadByte(reader)
	if err != nil {
		return err
	}

	util.TempLogger.Println("Fnished reading DataRequestHeader")
	
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

func NewText() *Text {
	t := Text{}
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

func (t *Token) Read(reader Reader) error {
	return GenericRead(t, reader)
}

func (t *Token) Write(writer Writer) error {
	return GenericWrite(t, writer)
}

func NewToken() *Token {
	t := Token{}
	return &t
}

//writable
//header when the DataRequest has Op of OP_READ_BLOCK
type ReadBlockHeader struct {
	//long
	BlockId uint64

	//long
	StartOffset uint64

	//long
	Length uint64

	//varint (for absolutely no reason whatsoever, 
	//here, Hadoop's codebase decides to use varints
	//instead of longs to encode lengths)
	ClientNameLength int64

	ClientName []byte

	AccessToken *Token
}

func NewReadBlockHeader() *ReadBlockHeader {
	r := ReadBlockHeader{}
	r.AccessToken = NewToken()
	return &r
}

func (r *ReadBlockHeader) Read(reader Reader) error {
	return GenericRead(r, reader)
}

func (r *ReadBlockHeader) Write(writer Writer) error {
	return GenericWrite(r, writer)
}
