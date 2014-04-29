package writables

import (
	"testing"
	"bytes"
	"reflect"
	"fmt"
)

func TestPiplineAckNew (t *testing.T) {
	pa := NewPipelineAck()
	if pa == nil {
		t.Fail()
	}
}

func TestPiplineAckWrite (t *testing.T) {
	pa := NewPipelineAck()
	pa.SeqNo = 0
	pa.NumOfReplies = 0

	buffer := new(bytes.Buffer)
	pa.Write(buffer)

	expected := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if !reflect.DeepEqual(expected, buffer.Bytes()) {
		t.Fail()
	}
}

/*****
** DataRequestHeader tests
**/

var DataRequestHeaderTestCase = []byte{0x00, 0x11, 0x51}
var testBuffer *bytes.Buffer

func setupDataRequestHeader() {
	testBuffer = bytes.NewBuffer(DataRequestHeaderTestCase)
}

func TestDataRequestHeaderNew(t *testing.T) {
	d := NewDataRequestHeader()
	if d == nil {
		t.Fail()
	}
}

func TestDataRequestHeaderRead(t *testing.T) {
	d := NewDataRequestHeader()
	setupDataRequestHeader()

	d.Read(testBuffer)

	if d.Version != 17 {
		fmt.Println("Version mismatch: ", d.Version)
		t.Fail()
	}

	if d.Op != 81 {
		t.Fail()
	}
}

/**
* Cache query related testing
*/
func TestReadRequestHeaderEquals(t *testing.T) {
	r1 := NewReadBlockHeader()
	r1.BlockId = 1
	r1.Timestamp = 2
	r1.StartOffset = 3
	r1.Length = 4
	r1.ClientName = NewText()
	r1.AccessToken = NewToken()

	r2 := NewReadBlockHeader()
	r2.BlockId = 1

	//note that the timestamp has
	//no relevance to the equivalence
	//of two request packets in this
	//case
	r2.Timestamp = 500

	r2.StartOffset = 3
	r2.Length = 4
	r2.ClientName = NewText()
	r2.AccessToken = NewToken()

	if !r1.Equals(r2) {
		fmt.Println("R1 wasn't equal to R2 when it was supposed to.")
		fmt.Println("Client names equal? : ", r1.ClientName.Equals(r2.ClientName))
		t.Fail()
	}

	r2.Length = 45
	if r1.Equals(r2) {
		t.Fail()
	}

	r2.Length = r1.Length
	r2.ClientName.Length = 15
	r2.ClientName.Bytes = make([]byte, 15)

	if r1.Equals(r2) {
		t.Fail()
	}
}
