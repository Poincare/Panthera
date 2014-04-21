
/*
* Implements the Panthera scheduler.
* Uses information the client caches 
* to decide what jobs to run next
*/
package main

import (
	//go packages
	"fmt"
	"sort"

	//local packages
	"cache_protocol"
	"job_info"
	"scheduler/configuration"
	"scheduler/cache_comm"
)

type Scheduler struct {
	Caches []*cache_protocol.CacheInfo
	Jobs []*job_info.JobInfo
}

func NewScheduler() *Scheduler {
	scheduler := Scheduler{}
	scheduler.Caches = make([]*cache_protocol.CacheInfo, 0)
	scheduler.Jobs = make([]*job_info.JobInfo, 0)
	return &scheduler
}

type JobSorter struct {
	jobs []*job_info.JobInfo
	cache *cache_protocol.CacheInfo
}

func NewJobSorter(jobs []*job_info.JobInfo, 
	cache *cache_protocol.CacheInfo) *JobSorter {
	j := JobSorter{jobs:jobs, cache:cache}
	return &j
}

func (js *JobSorter) GetJobs() []*job_info.JobInfo {
	return js.jobs
}

func (js *JobSorter) Less(i, j int) bool {
	jobKey := func(j1, j2 *job_info.JobInfo) bool {
		score1 := j1.ScoreCacheInfo(js.cache)
		score2 := j2.ScoreCacheInfo(js.cache)
		return score1 < score2
	}

	return jobKey(js.jobs[i], js.jobs[j])
}

func (j *JobSorter) Len() int {
	return len(j.jobs)
}

func (js *JobSorter) Swap(i int, j int) {
	js.jobs[i], js.jobs[j] = js.jobs[j], js.jobs[i]
}

func SortJobs(cache *cache_protocol.CacheInfo, 
	jobs []*job_info.JobInfo) ([]*job_info.JobInfo) {

	js := NewJobSorter(jobs, cache)
	sort.Sort(js)

	return js.GetJobs()
}

func PrintJobs(jobs []*job_info.JobInfo) {
	fmt.Print("[")
	for i:=0; i<len(jobs); i++ {
		fmt.Print(*jobs[i], ",")
	}
	fmt.Print("]")
}

func main() {
	scheduler := NewScheduler()

	//load the configuration
	conf := configuration.NewConfiguration()
	confFile := "scheduler_conf.json"
	err := conf.ReadFromFile(confFile)
	if err != nil {
		fmt.Println("Could not read configuration. Quitting!")
		fmt.Println("Error: ", err)
		return
	}

	//load the job info files
	jobInfoFiles, err := conf.JobInfoFiles()
	if err != nil {
		fmt.Println("Could not list JobInfoDir. Quitting!")
		return
	}

	//get the JobInfo instances
	scheduler.Jobs, err = conf.JobInfoList(jobInfoFiles)
	if err != nil {
		fmt.Println("Could not parse files in JobInfoDir. Quitting!")
		fmt.Println("Error: ", err)
		return
	}

	//get a cache description
	for i := 0; i < len(conf.CacheLocations); i++ { 
		cacheLocation := conf.CacheLocations[i]
		client, err := cache_comm.NewClient(cacheLocation)
		if err != nil {
			fmt.Println("Could not initialize client (quitting), err: ", err)
			return
		}

		descr, err := client.GetCacheDescription()
		if err != nil {
			fmt.Println("Could not get cache description, err: ", err)
			return
		}

		fmt.Println("Cache description: ", descr)

		cachedBlocks, err := client.GetCachedBlocks()
		if err != nil {
			fmt.Println("Could not get cachedBlocks, err: ", err)
			return
		}
		fmt.Println("Cached bocks: ", cachedBlocks)

		cacheInfo := cache_protocol.NewCacheInfo(descr, cachedBlocks)
		scheduler.Caches = append(scheduler.Caches, cacheInfo)
	}


}