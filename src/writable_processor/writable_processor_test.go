package writable_processor

import (
	"testing"
	"bytes"

)

func TestWritableProcessorNew (t *testing.T) {
	w := New()
	if w == nil {
		t.Fail()
	}
}


var DataRequestHeaderTestCase = []byte{0x00, 0x11, 0x51}
var dataRequestBuffer *bytes.Buffer

func setup() {
	dataRequestBuffer = bytes.NewBuffer(DataRequestHeaderTestCase) 
}

func TestReadRequestHeader(t *testing.T) {
	setup()
	w := New()
	d := w.ReadRequestHeader(dataRequestBuffer)

	if d.Version != 17 {
		t.Fail()
	}

	if d.Op != 81 {
		t.Fail()
	}
}

