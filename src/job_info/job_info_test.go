package job_info

import (
	//golang imports
	"testing"
	"reflect"
	"fmt"

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
	jobInfo.BlocksAccessed = []uint64{17547979945164609488}
	jobInfo.SetBlockDescriptions()

	buf, err := jobInfo.Write()
	if err != nil {
		fmt.Println("Failed write test.")
		t.Fail()
	}

	if string(buf) != 
	"{\"Name\":\"wordcount\",\"BlocksAccessed\":[17547979945164609488]}" {
		fmt.Println("Failed string(buf)")
		t.Fail()
	}

	jobReadInfo := NewJobInfo()
	jobReadInfo.Read(buf)

	if !reflect.DeepEqual(*jobReadInfo, *jobInfo) {
		fmt.Println("Failed reflect comparison")
		t.Fail()
	}
}
