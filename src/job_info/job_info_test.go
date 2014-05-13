package job_info

import (
	//golang imports
	"testing"
	"reflect"
	"fmt"

	//local imports
	"cache_protocol"
)

var epsilon float64 = 0.000001

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

func TestJobInfoScore(t *testing.T) {
	jobInfo := NewJobInfo()
	jobInfo.Name = "wordcount"
	jobInfo.BlocksAccessed = []uint64{18, 20, 23}

	jobInfo2 := NewJobInfo()
	jobInfo2.Name = "wordcount"
	jobInfo2.BlocksAccessed = []uint64{2, 3, 4}

	descr := cache_protocol.NewCacheDescription()
	
	cachedBlocks := cache_protocol.NewCachedBlocks()
	cachedBlocks.NumBlocks = 3
	cachedBlocks.Blocks = make([]*cache_protocol.BlockDescription, 3)
	cachedBlocks.Blocks[0] = cache_protocol.NewBlockDescription()
	cachedBlocks.Blocks[0].BlockId = 2
	cachedBlocks.Blocks[1] = cache_protocol.NewBlockDescription()
	cachedBlocks.Blocks[1].BlockId = 3
	cachedBlocks.Blocks[2] = cache_protocol.NewBlockDescription()
	cachedBlocks.Blocks[2].BlockId = 4

	score := jobInfo.ScoreCache(descr, cachedBlocks)
	if (score - 0) > epsilon {
		t.Fail()
	}

	score = jobInfo2.ScoreCache(descr, cachedBlocks)
	if (score - 1) > epsilon {
		t.Fail()
	}
}
