package data_requests

import (
	"testing"
)

func TestNewPRP (t *testing.T) {
	prp := NewPutRequestProcessor(int64(15))
	if prp == nil {
		t.Fail()
	}

	if prp.id != id {
		t.Fail()
	}

}