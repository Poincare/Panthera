package writable_processor

import (
	"testing"
)

func TestWritableProcessorNew (t *testing.T) {
	w := NewWritableProcessor()
	if w == nil {
		t.Fail()
	}
}

func TestReadRequestHeader(t *testing.T) {
	
}

