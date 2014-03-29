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
