package writables

import (
	"testing"
	"bytes"
	"fmt"
	"reflect"
	"encoding/binary"
)

//test case
var DataNodeRegistrationTest []byte = []byte{0,24,100,104,97,105,118,97,116,45,71,65,45,56,55,
	48,65,45,85,68,51,58,50,48,49,48,0,42,68,83,45,50,48,57,54,56,50,54,49,51,54,45,49,50,55,46,
	48,46,49,46,49,45,50,48,49,48,45,49,51,57,53,50,48,53,55,51,57,56,51,56,195,155,195,100,255,
	255,255,215,108,110,46,95,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,255,0,0,0,0}

var dnrBuffer *bytes.Buffer

func setup() {
	dnrBuffer = bytes.NewBuffer(DataNodeRegistrationTest)
}

/* Test the DataNodeRegistration constructor */
func TestNewDNR (t *testing.T) {
	dnr := NewDataNodeRegistration();
	if dnr == nil {
		t.Fail()
	}	
}

func TestDNRWrite(t *testing.T) {
	dnr := NewDataNodeRegistration()
	setup()
	var dnrWriteBuffer bytes.Buffer

	err := dnr.Read(dnrBuffer)
	fmt.Println("Dnr.keys.currentkey: ", dnr.Keys.CurrentKey)
	fmt.Println("keys of dnr: ", dnr.Keys.KeyLength)

 	err = dnr.Write(&dnrWriteBuffer)
	if err != nil {
		t.Fail()
	}

	returnedBytes := dnrWriteBuffer.Bytes()
	expectedBytes := DataNodeRegistrationTest
	
	if len(returnedBytes) != len(expectedBytes) {
		t.Fail()
	}

	if !reflect.DeepEqual(returnedBytes, expectedBytes) {
		t.Fail()
	}

	fmt.Println("DataNodeRegistration Write() bytes: ", dnrWriteBuffer.Bytes())
	fmt.Println("DataNodeRegistration Expected bytes: ", DataNodeRegistrationTest)
}

func TestDNRRead(t *testing.T) {
	dnr := NewDataNodeRegistration()
	setup()

	dnr.Read(dnrBuffer)
	fmt.Println("DataNodeRegistration after Read(): ", dnr)
}

func TestReadString(t *testing.T) {
	setup()

	name, err := ReadString(dnrBuffer)
	if err != nil {
		t.Fail()
	}

	if name != "dhaivat-GA-870A-UD3:2010" {
		t.Fail()
	}
}

func TestReadShortInt(t *testing.T) {
	setup()

	res, err := ReadShortInt(dnrBuffer)
	if err != nil {
		t.Fail()
	}

	if res != 24 {
		t.Fail()
	}
}

func TestDNRReadName(t *testing.T) {
	dnr := NewDataNodeRegistration()
	setup()

	err := dnr.ReadName(dnrBuffer)
	if err != nil {
		t.Fail()
		fmt.Println("Error occurred in TestDNRReadName: ", err)
	}

	if dnr.Name != "dhaivat-GA-870A-UD3:2010" {
		t.Fail()
	}
}

func TestDNRReadStorageID(t *testing.T) {
	dnr := NewDataNodeRegistration()
	setup()

	dnr.ReadName(dnrBuffer)
	err := dnr.ReadStorageID(dnrBuffer)
	if err != nil {
		t.Fail()
		fmt.Println("Error occurred in TestDNRReadStorageID: ", err)
	}

	if dnr.StorageID != "DS-2096826136-127.0.1.1-2010-1395205739838" {
		t.Fail()
	}
}

func TestDNRReadInfoPort(t *testing.T) {
	dnr := NewDataNodeRegistration()
	setup()

	dnr.ReadName(dnrBuffer)
	dnr.ReadStorageID(dnrBuffer)
	err := dnr.ReadInfoPort(dnrBuffer)
	if err != nil {
		t.Fail()
		fmt.Println("Error occurred in TestDNRReadInfoPort: ", err)
	}

	if dnr.InfoPort != 50075 {
		t.Fail()
	}
}

func TestDNRReadIpcPort(t *testing.T) {
	dnr := NewDataNodeRegistration()
	setup()

	dnr.ReadName(dnrBuffer)
	dnr.ReadStorageID(dnrBuffer)
	dnr.ReadInfoPort(dnrBuffer)
	err := dnr.ReadIpcPort(dnrBuffer)
	if err != nil {
		t.Fail()
		fmt.Println("Error occurred in TestDNRReadIpcPort: ", err)
	}

	if dnr.IpcPort != 50020 {
		t.Fail()
	}
}

func TestDNRReadLayoutVersion(t *testing.T) {
	dnr := NewDataNodeRegistration()
	setup()

	dnr.ReadName(dnrBuffer)
	dnr.ReadStorageID(dnrBuffer)
	dnr.ReadInfoPort(dnrBuffer)
	dnr.ReadIpcPort(dnrBuffer)

	err := dnr.ReadLayoutVersion(dnrBuffer)
	if err != nil {
		t.Fail()
		fmt.Println("Error occurred in TestDNRReadLayoutVersion: ", err)
	}

	if dnr.LayoutVersion != 4294967255 {
		t.Fail()
	}
}

func TestDNRReadNamespaceID(t *testing.T) {
	dnr := NewDataNodeRegistration()
	setup()

	dnr.ReadName(dnrBuffer)
	dnr.ReadStorageID(dnrBuffer)
	dnr.ReadInfoPort(dnrBuffer)
	dnr.ReadIpcPort(dnrBuffer)
	dnr.ReadLayoutVersion(dnrBuffer)

	err := dnr.ReadNamespaceID(dnrBuffer)

	if err != nil {
		t.Fail()
		fmt.Println("Error occurred in TestDNRReadNamespaceID: ", err)
	}

	if dnr.NamespaceID != 1819160159 {
		t.Fail()
	}
}

/*
func TestDNRReadCTime(t *testing.T) {
	dnr := NewDataNodeRegistration()
	setup()

	dnr.ReadName(dnrBuffer)
	dnr.ReadStorageID(dnrBuffer)
	dnr.ReadInfoPort(dnrBuffer)
	dnr.ReadIpcPort(dnrBuffer)
	dnr.ReadLayoutVersion(dnrBuffer)
	dnr.ReadNamespaceID(dnrBuffer)

	err := dnr.ReadCTime(dnrBuffer)
	if err != nil {
		t.Fail()
		fmt.Println("Error occurred in TestDNRReadCTime: ", err)
	}

	if dnr.CTime != 0 {
		t.Fail()
	}
}

func TestDNRReadIsBlockTokenEnabled(t *testing.T) {
	dnr := NewDataNodeRegistration()
	setup()

	dnr.ReadName(dnrBuffer)
	dnr.ReadStorageID(dnrBuffer)
	dnr.ReadInfoPort(dnrBuffer)
	dnr.ReadIpcPort(dnrBuffer)
	dnr.ReadLayoutVersion(dnrBuffer)
	dnr.ReadNamespaceID(dnrBuffer)
	dnr.ReadCTime(dnrBuffer)

	err := dnr.ReadIsBlockTokenEnabled(dnrBuffer)
	if err != nil {
		t.Fail()
		fmt.Println("Error occurred in TestDNRReadIsBlockTokenEnabled: ", err)
	}

	//should be false
	if dnr.IsBlockTokenEnabled {
		t.Fail()
	}
} */

/***
** Testing package Writable Write methods 
**/

func TestWriteBoolean(t *testing.T) {
	val := true
	var buf bytes.Buffer

	WriteBoolean(val, &buf)

	if len(buf.Bytes()) != 1 {
		t.Fail()
	}

	if buf.Bytes()[0] != 1 {
		t.Fail()
	}
}

func TestWriteLongInt(t *testing.T) {
	var val uint64
	val = 1
	var buf bytes.Buffer

	WriteLongInt(val, &buf)

	if len(buf.Bytes()) != 8 {
		t.Fail()
	}

	if buf.Bytes()[0] != 0 {
		t.Fail()
	}

	if buf.Bytes()[7] != 1 {
		t.Fail()
	}
}

func TestWriteInt(t *testing.T) {
	var val uint32
	val = 17

	var buf bytes.Buffer
	WriteInt(val, &buf)

	if len(buf.Bytes()) != 4 {
		t.Fail()
	}

	var expected = []byte{0, 0, 0, 17}
	if !reflect.DeepEqual(expected, buf.Bytes()) {
		fmt.Println("Expected: ", expected)
		fmt.Println("Got: ", buf.Bytes())
		t.Fail()
	}
}

func TestWriteByte(t *testing.T) {
	var val byte
	val = byte('a')

	var buf bytes.Buffer
	WriteByte(val, &buf)

	if len(buf.Bytes()) != 1 {
		t.Fail()
	}

	if string(buf.Bytes()) != "a" {
		t.Fail()
	}
}

func TestWriteString(t *testing.T) {
	val := "dhaivat"
	var buf bytes.Buffer
	WriteString(val, &buf)

	bytes := buf.Bytes()
	if len(bytes) != len(val) + 2 {
		t.Fail()
	}

	if string(bytes[2:]) != val {
		t.Fail()
	}

	var length uint16
	binary.Read(&buf, binary.BigEndian, &length)

	if int(length) != len(val) {
		t.Fail()
	}
}

func TestWriteVarint(t *testing.T) {
	var val int64 = 15
	var buf bytes.Buffer
	WriteVarint(val, &buf)

	bytes := buf.Bytes()
	resVal, _ := binary.Varint(bytes)

	if resVal != val {
		fmt.Println("varint mismatch: ", resVal, val)
		t.Fail()
	}
}
