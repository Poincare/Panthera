package data_requests

import (
	"testing"
)

func TestNewProcessor (t *testing.T) {
	p := NewProcessor()
	if p == nil {
		t.Fail()
	}
}