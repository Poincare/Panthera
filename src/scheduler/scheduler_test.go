package main

import (
	//go packages
	"testing"
	"fmt"

	//local pacakges
	"cache_protocol"
	"job_info"
)

func TestSchedulerSort(t *testing.T) {
	jobs := make([]*job_info.JobInfo, 2)
	jobs[1] = job_info.NewJobInfo()
	jobs[1].BlocksAccessed = []uint64{1, 2, 3}
	jobs[1].SetBlockDescriptions()

	jobs[0] = job_info.NewJobInfo()
	jobs[0].BlocksAccessed = []uint64{4, 5, 6}
	jobs[0].SetBlockDescriptions()

	blocks := cache_protocol.NewCachedBlocks()
	blocks.NumBlocks = 3
	blocks.Blocks = make([]*cache_protocol.BlockDescription, 3)
	blocks.Blocks[0] = cache_protocol.NewBlockDescription()
	blocks.Blocks[0].BlockId = 1
	blocks.Blocks[1] = cache_protocol.NewBlockDescription()
	blocks.Blocks[1].BlockId = 2
	blocks.Blocks[2] = cache_protocol.NewBlockDescription()
	blocks.Blocks[2].BlockId = 3

	descr := cache_protocol.NewCacheDescription()
	descr.ReplaceAlgorithm = cache_protocol.LRU
	descr.CacheSize = 3
	descr.CurrSize = 3

	cache := cache_protocol.NewCacheInfo(descr, blocks)

	oldJob0 := jobs[0]
	oldJob1 := jobs[1]

	PrintJobs(jobs)	
	SortJobs(cache, jobs)
	fmt.Println("\nSorted jobs: ")
	PrintJobs(jobs)
	fmt.Print("\n")

	if(jobs[0] != oldJob1) {
		t.Fail()
	}

	if(jobs[1] != oldJob0) {
		t.Fail()
	}
}