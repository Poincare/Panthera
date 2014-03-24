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
	IpcPort int

	//written as int
	LayoutVersion int

	//written as int
	NameSpaceID int

	//written as long
	CTime int
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