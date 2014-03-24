package writables

import (
	"testing"
	"bytes"
	"fmt"
)

//test case
var DataNodeRegistrationTest []byte = []byte{0,24,100,104,97,105,118,97,116,45,71,65,45,56,55,
	48,65,45,85,68,51,58,50,48,49,48,0,42,68,83,45,50,48,57,54,56,50,54,49,51,54,45,49,50,55,46,
	48,46,49,46,49,45,50,48,49,48,45,49,51,57,53,50,48,53,55,51,57,56,51,56,195,155,195,100,255,
	255,255,215,108,110,46,95,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,255,0,0,0,0}

var dnrBuffer *bytes.Buffer

/* Test the DataNodeRegistration constructor */
func TestNewDNR (t *testing.T) {
	dnr := NewDataNodeRegistration();
	if dnr == nil {
		t.Fail()
	}	
}

func setup() {
	dnrBuffer = bytes.NewBuffer(DataNodeRegistrationTest)
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

func TestDNRReadStorageID (t *testing.T) {
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
