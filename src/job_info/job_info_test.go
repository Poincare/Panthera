package job_info

import (
	//golang imports
	"testing"
	"reflect"

	//local imports
)

func TestNewJobInfo(t *testing.T) {
	jobInfo := NewJobInfo()
	if jobInfo == nil {
		t.Fail()
	}
}

func TestJobInfoWriteRead(t *testing.T) {
	jobInfo := NewJobInfo()
	jobInfo.Name = "wordcount"
	jobInfo.FilesAccessed = []string{"gutenberg/sherlock-holmes-repeated.txt"}

	buf, err := jobInfo.Write()
	if err != nil {
		t.Fail()
	}

	if string(buf) != "{\"Name\":\"wordcount\",\"FilesAccessed\":[\"gutenberg/sherlock-holmes-repeated.txt\"]}" {
		t.Fail()
	}

	jobReadInfo := NewJobInfo()
	jobReadInfo.Read(buf)

	if !reflect.DeepEqual(*jobReadInfo, *jobInfo) {
		t.Fail()
	}
}