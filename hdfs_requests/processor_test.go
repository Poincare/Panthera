package hdfs_requests

import (
	"testing"
)

func TestNewProcess(t *testing.T) {
	p := NewProcessor()
	if p == nil {
		t.FailNow()
	}
}