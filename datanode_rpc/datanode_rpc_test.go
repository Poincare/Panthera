package datanode_rpc

import (
	"testing"
)

func TestDataRequestConstructor(t *testing.T) {
	dnr := NewDataRequest()
	if dnr == nil {
		t.Fail()
	}
}