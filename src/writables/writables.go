package writables

import (
	"encoding/binary"
	"io"
)

/*
* This package implements the Hadoop Writable serialization protocol
* to the extent that is required by this project, i.e. it does
not aim to be a fully replaceable implementation out of the Writable
algorithm. */

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

func (b *BlockKey) Read(reader io.Reader) {
	b.KeyId, _ := binary.ReadVarint(reader)
	b.ExpiryDate, _ := binary.ReadVarint(reader)
	b.Len, _ := binary.ReadVarint(reader)

	if b.Len > 0 {
		b.KeyBytes := make([]byte, b.Len)
		reader.Read(b.KeyBytes)
	}
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
	ebk.CurrentKey := NewBlockKey()

	return &ebk
}

func (e *ExportedBlockKeys) Read(reader io.Reader) {
	e.IsBlockTokenEnabled, _ := ReadBoolean()
	e.KeyUpdateInterval, _ := ReadLongInt()
	e.TokenLifetime, _ := ReadLongInt()
	
	e.CurrentKey.Read(reader)

	e.KeyLength := ReadInt()
	for i := 0; i<e.KeyLength; i++ {
		e.AllKeys[i] = NewBlockKey()
		e.AllKeys[i].Read(reader)
	}

	e.KindaDoneHereButNeedASyntaxError
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

	//port to conduct IPC on 9written as short int)
	IpcPort uint16

	//written as int
	LayoutVersion uint32

	//written as int
	NamespaceID uint32

	//written as long
	CTime uint64

	//boolean
	IsBlockTokenEnabled bool

	KeyUpdateInterval uint64

}


/* constructor */
func NewDataNodeRegistration() *DataNodeRegistration {
	dnr := DataNodeRegistration{}
	return &dnr
}

//package method - read a Writeable String
func ReadString(reader io.Reader) (string, error) {
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
func ReadShortInt(reader io.Reader) (uint16, error) {
	var res uint16
	err := binary.Read(reader, binary.BigEndian, &res)
	if err != nil {
		return 0, err
	}

	return res, nil
}

//package method - read a Writeable Int
func ReadInt(reader io.Reader) (uint32, error) {
	var res uint32
	err := binary.Read(reader, binary.BigEndian, &res)
	if err != nil {
		return 0, err
	}

	return res, nil
}

//package method - read a Writeable Long Int
func ReadLongInt(reader io.Reader) (uint64, error) {
	var res uint64
	err := binary.Read(reader, binary.BigEndian, &res)
	if err != nil {
		return 0, err
	}

	return res, nil
}

//package method - read a Writeable Boolean (1 byte)
func ReadBoolean(reader io.Reader) (bool, error) {
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


//reads the name value from a reader (this can be a connection,
//byte buffer, etc.)
func (d *DataNodeRegistration) ReadName(reader io.Reader) error {
	name, err := ReadString(reader)
	if err != nil {
		return err
	}

	d.Name = name
	return nil
}

func (d *DataNodeRegistration) ReadStorageID(reader io.Reader) error {
	storageID, err := ReadString(reader)
	if err != nil {
		return err
	}

	d.StorageID = storageID
	return nil
}

func (d *DataNodeRegistration) ReadInfoPort(reader io.Reader) error {
	infoPortReg, err := ReadShortInt(reader)
	if err != nil {
		return err
	}

	d.InfoPort = infoPortReg & 0x0000ffff
	return nil
}

func (d *DataNodeRegistration) ReadIpcPort(reader io.Reader) error {
	ipcPortReg, err := ReadShortInt(reader)
	if err != nil {
		return err
	}

	d.IpcPort = ipcPortReg & 0x0000ffff
	return nil
}

func (d *DataNodeRegistration) ReadLayoutVersion(reader io.Reader) error {
	layoutVersion, err := ReadInt(reader)
	if err != nil {
		return err
	}

	d.LayoutVersion = layoutVersion
	return nil
}

func (d *DataNodeRegistration) ReadNamespaceID(reader io.Reader) error {
	namespaceID, err := ReadInt(reader)
	if err != nil {
		return err
	}

	d.NamespaceID = namespaceID
	return nil
}

func (d *DataNodeRegistration) ReadCTime(reader io.Reader) error {
	cTime, err := ReadLongInt(reader)
	if err != nil {
		return err
	}

	d.CTime = cTime
	return nil
}

func (d *DataNodeRegistration) ReadIsBlockTokenEnabled(reader io.Reader) error {
	is, err := ReadBoolean(reader)
	if err != nil {
		return err
	}

	d.IsBlockTokenEnabled = is
	return nil
}

func (d *DataNodeRegistration) ReadKeyUpdateInterval(reader io.Reader) error {
	keyUpdateInterval, err := ReadLongInt(reader)
	if err != nil {
		return err
	}

	d.KeyUpdateInterval = keyUpdateInterval
	return nil
}

func (d *DataNodeRegistration) ReadTokenLifetime(reader io.Reader) error {
	tokenLifetime, err := ReadLongInt(reader)
	if err != nil {
		return err
	}

	d.TokenLifeTime = tokenLifetime
	return nil
}

