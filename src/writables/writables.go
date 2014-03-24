package writables

import (
	"encoding/binary"
)

//this is a mix of io.ByteReader
//and io.Reader
type Reader interface {
	ReadByte() (byte, error)
	Read(p []byte) (n int, err error)
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

func (b *BlockKey) Read(reader Reader) error {
	b.KeyId, _ = binary.ReadVarint(reader)
	b.ExpiryDate, _ = binary.ReadVarint(reader)
	b.Len, _ = binary.ReadVarint(reader)

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

	//port to conduct IPC on 9written as short int)
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
	return &dnr
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

