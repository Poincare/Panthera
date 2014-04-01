package writables

import (
	"encoding/binary"
	"bytes"
	"reflect"
	"errors"

	"fmt"
)

/*
* This package implements the Hadoop Writable serialization protocol
* to the extent that is required by this project, i.e. it does
not aim to be a fully replaceable implementation out of the Writable
algorithm. */

//the Writable interface, as specified in the Hadoop source code
type Writable interface {
	Read(Reader) error
	Write(Writer) error
}

//this is a mix of io.ByteReader
//and io.Reader
type Reader interface {
	ReadByte() (byte, error)
	Read(p []byte) (n int, err error)
}


//mix of io.ByteWriter and io.Writer
type Writer interface {
	Write(p []byte) (n int, err error)
	WriteByte(p byte) (err error)
}

//implements both Reader and Writer
type ReaderWriter interface {
	Write(p []byte) (n int, err error)
	WriteByte(p byte) (err error)
	ReadByte() (byte, error)
	Read(p []byte) (n int, err error)
}

/***
*** Writable Reader methods
***/

func GenericWrite(packet interface{}, writer Writer) error {

	packetValue := reflect.ValueOf(packet).Elem()
	//if the value of the packet is not (i.e. is a zero value)
	//then there is no point trying to read or write it
	if !packetValue.IsValid() {
		return errors.New("Packet is a zero value.")
	}

	fmt.Println("Packet value: ", packetValue)

	elementCount := reflect.ValueOf(packet).Elem().NumField()
	for i := 0; i < elementCount; i++ {
		element := reflect.ValueOf(packet).Elem().Field(i)
		elementVal := element.Interface()

		//we have to read in the element according to what
		//type it is in the packet structure
		switch elementVal.(type) {
		//byte
		case int8:
			val := elementVal.(int8)
			err := WriteByte(val, writer)
			if err != nil {
				return err
			}
		
		//short
		case uint16:
			val := elementVal.(uint16)
			err := WriteShortInt(val, writer)
			if err != nil {
				return err
			}

		//int
		case uint32:
			val := elementVal.(uint32)
			err := WriteInt(val, writer)
			if err != nil {
				return err
			}

		//long
		case uint64:
			val := elementVal.(uint64)
			err := WriteLongInt(val, writer)
			if err != nil {
				return err
			}

		//varint
		case int64:
			val := elementVal.(int64)
			WriteVInt(val, writer)

		//string
		case string:
			val := elementVal.(string)
			err := WriteString(val, writer)
			if err != nil {
				return err
			}
		case []byte:
			val := elementVal.([]byte)

			if i <= 0 {
				return errors.New("There is a []byte as the first element of the packet structure; don't know the length, so cannot proceed.")
			}

			length := reflect.ValueOf(packet).Elem().Field(i-1).Interface()
			var finalLength int64

			switch length.(type) {
			case uint16:
				length := length.(uint16)
				finalLength = int64(length)
			case uint32:
				length := length.(uint32)
				finalLength = int64(length)
			case uint64:
				length := length.(uint64)
				finalLength = int64(length)
			case int64:
				finalLength = length.(int64)
			}

			err := WriteBytes(val, finalLength, writer)
			if err != nil {
				return err
			}

			element.Set(reflect.ValueOf(val))
		case Writable:
			writable := elementVal.(Writable)
			err := writable.Write(writer)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

//this is a generic read that can read (most) kind of writables
//without any configuration because it uses reflection and determines
//what type of methods to call for certain element types.
func GenericRead(packet interface{}, reader Reader) error {
	//number of elements in the packet

	//elementElem := reflect.ValueOf(packet).Elem()
	/*
	byteElem := reflect.ValueOf(packet).Elem().Field(0)
	fmt.Println("ByteElem: ", byteElem)
	reflect.ValueOf(packet).Elem().Field(0).Set(reflect.ValueOf(int8(17))) */

	packetValue := reflect.ValueOf(packet).Elem()
	//if the value of the packet is not (i.e. is a zero value)
	//then there is no point trying to read or write it
	if !packetValue.IsValid() {
		return errors.New("Packet is a zero value.")
	}

	elementCount := reflect.ValueOf(packet).Elem().NumField()
	for i := 0; i < elementCount; i++ {
		element := reflect.ValueOf(packet).Elem().Field(i)
		elementVal := element.Interface()

		//we have to read in the element according to what
		//type it is in the packet structure
		switch elementVal.(type) {
		//byte
		case int8:
			val, err := ReadByte(reader)
			if err != nil {
				return err
			}

			element.Set(reflect.ValueOf(val))
		
		//short
		case uint16:
			val, err := ReadShortInt(reader)
			if err != nil {
				return err
			}

			element.Set(reflect.ValueOf(val))

		//int
		case uint32:
			val, err := ReadInt(reader)
			if err != nil {
				return err
			}

			element.Set(reflect.ValueOf(val))

		//long
		case uint64:
			val, err := ReadLongInt(reader)
			if err != nil {
				return err
			}

			element.Set(reflect.ValueOf(val))

		//varint
		case int64:
			val := ReadVInt(reader)

			element.Set(reflect.ValueOf(val))

		//string
		case string:
			val, err := ReadString(reader)
			if err != nil {
				return err
			}

			element.Set(reflect.ValueOf(val))

		case Writable:
			writable := elementVal.(Writable)
			writable.Read(reader)

		//probably the most complex case. Basically, we are
		//assuming that if there is a byte array, the 
		//previous field in the struct specifies the length 
		//of that byte array. This is generally true for
		//Hadoop's protocol (if it isn't, don't use GenericRead)
		case []byte:
			if i <= 0 {
				return errors.New("There is a []byte as the first element of the packet structure; don't know the length, so cannot proceed.")
			}

			length := reflect.ValueOf(packet).Elem().Field(i-1).Interface()
			var finalLength int64

			switch length.(type) {
			case uint16:
				length := length.(uint16)
				finalLength = int64(length)
			case uint32:
				length := length.(uint32)
				finalLength = int64(length)
			case uint64:
				length := length.(uint64)
				finalLength = int64(length)
			case int64:
				finalLength = length.(int64)
			}

			val, err := ReadBytes(finalLength, reader)
			if err != nil {
				return err
			}

			element.Set(reflect.ValueOf(val))
		}


	}

	return nil
}

//package method - read a Writeable String
func ReadString(reader Reader) (string, error) {
	//first have to read in an unsigned short (2 bytes) which
	//denotes the length
	length, err := ReadShortInt(reader)
	if err != nil {
		return "", err
	}

	//now we read in the actual string
	nameBuf := make([]byte, length)
	reader.Read(nameBuf)

	return string(nameBuf), nil
}

//package method - read a Writeable Short Int
func ReadShortInt(reader Reader) (uint16, error) {
	var res uint16
	err := binary.Read(reader, binary.BigEndian, &res)
	if err != nil {
		return 0, err
	}

	return res, nil
}

//package method - read a Writeable Int
func ReadInt(reader Reader) (uint32, error) {
	var res uint32
	err := binary.Read(reader, binary.BigEndian, &res)
	if err != nil {
		return 0, err
	}

	return res, nil
}

//package method - read a Writeable Long Int
func ReadLongInt(reader Reader) (uint64, error) {
	var res uint64
	err := binary.Read(reader, binary.BigEndian, &res)
	if err != nil {
		return 0, err
	}

	return res, nil
}

//package method - read a Writeable Boolean (1 byte)
func ReadBoolean(reader Reader) (bool, error) {
	var res byte
	err := binary.Read(reader, binary.BigEndian, &res)
	if err != nil {
		return false, err
	}

	var resBool bool
	if res == 1 {
		resBool = true
	} else {
		resBool = false
	}

	return resBool, nil
}

//package method - actually reads an int8, but we 
//are saying that it reads a byte since that is 
//how it is done in the Hadoop codebase
func ReadByte(reader Reader) (int8, error) {
	var res int8
	err := binary.Read(reader, binary.BigEndian, &res)
	if err != nil {
		return 0, err
	}

	return res, nil
}

//read a sequence of bytes given the length
//that are supposed to read.
func ReadBytes(length int64, reader Reader) ([]byte, error) {
	res := make([]byte, length)
 	_, err := reader.Read(res)
 	if err != nil {
 		return []byte{}, err
 	}

 	return res, nil
}

/**
** Writable Writing methods
**/
func WriteLongInt(val uint64, writer Writer) error {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, val)

	_, err := writer.Write(buf.Bytes())
	return err
}

func WriteBoolean(val bool, writer Writer) error {
	var res = []byte{0}
	if val == true {
		res[0] = 1
	} else {
		res[0] = 0
	}

	_, err := writer.Write(res)
	return err
}

func WriteInt(val uint32, writer Writer) error {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, val)
	_, err := writer.Write(buf.Bytes())

	return err
}

func WriteShortInt(val uint16, writer Writer) error {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, val)
	_, err := writer.Write(buf.Bytes())

	return err
}

func WriteByte(val int8, writer Writer) error {
	buf := []byte{byte(val)}
	_, err := writer.Write(buf)
	return err
}

func WriteBytes(val []byte, length int64, writer Writer) error {
	_, err := writer.Write(val)
	return err
}

func WriteString(val string, writer Writer) error {
	//write out the length first
	err := WriteShortInt(uint16(len(val)), writer)
	if err != nil {
		return err
	}

	//then we write out the contents
	for i := 0; i<len(val); i++ {
		err = WriteByte(int8(val[i]), writer)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteUvarint(val uint64, writer Writer) error {
	buf := make([]byte, 30)
	bytesWritten := binary.PutUvarint(buf, val)

	res := buf[0:bytesWritten]

	_, err := writer.Write(res)
	return err
}

func WriteVarint(val int64, writer Writer) error {
	//30 bytes should be plenty
	buf := make([]byte, 30)
	bytesWritten := binary.PutVarint(buf, val)

	//the PutVarint function only writes to a portion of the 
	//buffer, so we are getting rid of the unwritten portions
	res := buf[0:bytesWritten]

	_, err := writer.Write(res)
	return err
}

//this seems to be the "Writable" version of Google's protocol
//buffers (does not seem to be the same algorithm)
//(code adapted from the Hadoop codebase, which has been
//adpated from the Protocol Buffers codebase)
func WriteVInt(val int64, writer Writer) {

  if val >= -112 && val <= 127 {
		binary.Write(writer, binary.BigEndian, int8(val))
    return
  }
    
  length := -112
  if val < 0 {
    val ^= int64(-1) // take one's complement'
    length = -120;
  }
    
  tmp := val
  for tmp != 0 {
    tmp = tmp >> 8
    length--
  }
    
  binary.Write(writer, binary.BigEndian, int8(length));
  
  if length < -120 {
  	length = -(length+120)
  } else {
  	length = -(length+112)
  }

  for idx := length; idx != 0; idx-- {
  	shiftbits := (idx - 1) * 8
  	mask := 0xFF << uint(shiftbits)
  	binary.Write(writer, binary.BigEndian, int8((val & int64(mask)) >> uint(shiftbits)))
  }
}

//Internal function used ReadVInt
func decodeVIntSize(value int8) int8 {
  if value >= -112 {
    return 1
  } else if value < -120 {
    return -119 - value
  }
  return -111 - value
}

//Internal function used by ReadVInt
func isNegativeVInt(value int8) bool {
	return value < -120 || (value >= -112 && value < 0);
}

//see comments on WriteVInt()
func ReadVInt(reader Reader) int64 {
	var firstByte int8
	binary.Read(reader, binary.BigEndian, &firstByte)
  
  length := decodeVIntSize(firstByte);
  if length == 1 {
    return int64(firstByte)
  }

  var i int64 = 0;
  for idx := int8(0); idx < length-1; idx++ {
    var b int8
    binary.Read(reader, binary.BigEndian, &b)

    i = i << 8
    i = i | (int64(b) & 0xFF)
  }

  if isNegativeVInt(firstByte) {
  	return (i ^ -1)
  } else {
  	return i
  }
}

/*** 
** Specific Writable structures
****/

type BlockKey struct {
	//read as varint
	KeyId int64

	//read as varlong (but treated as varint in Go)
	ExpiryDate int64

	//read as varlong
	Len int64

	//depends on the Len
	KeyBytes []byte
}

func NewBlockKey() *BlockKey {
	bk := BlockKey{}
	return &bk
}

func (b *BlockKey) Read(reader Reader) error {
	b.KeyId = ReadVInt(reader)
	b.ExpiryDate = ReadVInt(reader)
	b.Len = ReadVInt(reader)

	var err error

	if b.Len > 0 {
		b.KeyBytes = make([]byte, b.Len)
		for i := 0; i < int(b.Len); i++ {
			b.KeyBytes[i], err = reader.ReadByte()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *BlockKey) Write(writer Writer) error {
	WriteVInt(b.KeyId, writer)
	WriteVInt(b.ExpiryDate, writer)
	WriteVInt(b.Len, writer)
	
	err := WriteBytes(b.KeyBytes, int64(len(b.KeyBytes)), writer)

	return err
}

type ExportedBlockKeys struct {
	IsBlockTokenEnabled bool

	KeyUpdateInterval uint64
	
	TokenLifetime uint64
	
	CurrentKey *BlockKey

	KeyLength uint32
	AllKeys []*BlockKey
}

func NewExportedBlockKeys() *ExportedBlockKeys {
	ebk := ExportedBlockKeys{}
	ebk.CurrentKey = NewBlockKey()

	return &ebk
}

func (e *ExportedBlockKeys) Write(writer Writer) error {
	WriteBoolean(e.IsBlockTokenEnabled, writer)
	WriteLongInt(e.KeyUpdateInterval, writer)
	WriteLongInt(e.TokenLifetime, writer)
	e.CurrentKey.Write(writer)

	WriteInt(e.KeyLength, writer)
	for i := 0; i< int(e.KeyLength); i++ {
		e.AllKeys[i].Write(writer)
	}

	return nil
}

func (e *ExportedBlockKeys) Read(reader Reader) error {
	e.IsBlockTokenEnabled, _ = ReadBoolean(reader)
	e.KeyUpdateInterval, _ = ReadLongInt(reader)
	e.TokenLifetime, _ = ReadLongInt(reader)
	
	e.CurrentKey.Read(reader)

	e.KeyLength, _ = ReadInt(reader)

	for i := 0; i< int(e.KeyLength); i++ {
		e.AllKeys[i] = NewBlockKey()
		e.AllKeys[i].Read(reader)
	}

	return nil
}

type DataNodeRegistration struct {
	/* 
	* Reading scheme:
	* Strings as read as per the org.apache.hadoop.io.UTF8 class
	* Short ints are read as per the java.io.DataInput class
	* Ints and Long Ints are read as per the java.io.DataInput class */

	//hostname:port (datatransfer port)
	Name string

	//unique per cluster storageID
	StorageID string

	//port to exchange information other than data
	//(this value is written as a short int by Hadoop)
	InfoPort uint16

	//port to conduct IPC on written as short int)
	IpcPort uint16

	//written as int
	LayoutVersion uint32

	//written as int
	NamespaceID uint32

	//written as long
	CTime uint64

	Keys *ExportedBlockKeys
}


/* constructor */
func NewDataNodeRegistration() *DataNodeRegistration {
	dnr := DataNodeRegistration{}
	dnr.Keys = NewExportedBlockKeys()
	return &dnr
}

func (d *DataNodeRegistration) Write(writer Writer) error {

	WriteString(d.Name, writer)
	WriteString(d.StorageID, writer)
	WriteShortInt(d.InfoPort, writer)
	WriteShortInt(d.IpcPort, writer)
	WriteInt(d.LayoutVersion, writer)
	WriteInt(d.NamespaceID, writer)
	WriteLongInt(d.CTime, writer)
	err := d.Keys.Write(writer)

	return err
}

func (d *DataNodeRegistration) Read(reader Reader) error {
	err := d.ReadName(reader)
	err = d.ReadStorageID(reader)
	err = d.ReadInfoPort(reader)
	err = d.ReadIpcPort(reader)
	err = d.ReadLayoutVersion(reader)
	err = d.ReadNamespaceID(reader)
	err = d.ReadCTime(reader)
	err = d.ReadKeys(reader)

	return err
}

//reads the name value from a reader (this can be a connection,
//byte buffer, etc.)
func (d *DataNodeRegistration) ReadName(reader Reader) error {
	name, err := ReadString(reader)
	if err != nil {
		return err
	}

	d.Name = name
	return nil
}

func (d *DataNodeRegistration) ReadStorageID(reader Reader) error {
	storageID, err := ReadString(reader)
	if err != nil {
		return err
	}

	d.StorageID = storageID
	return nil
}

func (d *DataNodeRegistration) ReadInfoPort(reader Reader) error {
	infoPortReg, err := ReadShortInt(reader)
	if err != nil {
		return err
	}

	d.InfoPort = infoPortReg & 0x0000ffff
	return nil
}

func (d *DataNodeRegistration) ReadIpcPort(reader Reader) error {
	ipcPortReg, err := ReadShortInt(reader)
	if err != nil {
		return err
	}

	d.IpcPort = ipcPortReg & 0x0000ffff
	return nil
}

func (d *DataNodeRegistration) ReadLayoutVersion(reader Reader) error {
	layoutVersion, err := ReadInt(reader)
	if err != nil {
		return err
	}

	d.LayoutVersion = layoutVersion
	return nil
}

func (d *DataNodeRegistration) ReadNamespaceID(reader Reader) error {
	namespaceID, err := ReadInt(reader)
	if err != nil {
		return err
	}

	d.NamespaceID = namespaceID
	return nil
}

func (d *DataNodeRegistration) ReadCTime(reader Reader) error {
	cTime, err := ReadLongInt(reader)
	if err != nil {
		return err
	}

	d.CTime = cTime
	return nil
}

/*
func (d *DataNodeRegistration) ReadIsBlockTokenEnabled(reader Reader) error {
	is, err := ReadBoolean(reader)
	if err != nil {
		return err
	}

	d.IsBlockTokenEnabled = is
	return nil
}

func (d *DataNodeRegistration) ReadKeyUpdateInterval(reader Reader) error {
	keyUpdateInterval, err := ReadLongInt(reader)
	if err != nil {
		return err
	}

	d.KeyUpdateInterval = keyUpdateInterval
	return nil
}

func (d *DataNodeRegistration) ReadTokenLifetime(reader Reader) error {
	tokenLifetime, err := ReadLongInt(reader)
	if err != nil {
		return err
	}

	d.TokenLifeTime = tokenLifetime
	return nil
} */

func (d *DataNodeRegistration) ReadKeys(reader Reader) error {
	keys := NewExportedBlockKeys()
	err := keys.Read(reader)
	if err != nil {
		return err
	}

	d.Keys = keys
	return nil
}

