package hdfs_requests

import (
	"testing"
)

//obligatory constructor test
func TestNewObjectCreatedEvent (t *testing.T) {
	oce := NewObjectCreatedEvent("/usr/local/hadoop")
	if oce == nil {
		t.Fail()
	}

	if oce.Filepath != "/usr/local/hadoop" {
		t.Fail()
	}
}



