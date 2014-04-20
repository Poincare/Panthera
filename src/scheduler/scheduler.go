
/*
* Implements the Panthera scheduler.
* Uses information the client caches 
* to decide what jobs to run next
*/
package main

import (
	//go packages
	"fmt"

	//local packages
	"cache_protocol"
	"scheduler/configuration"
	"scheduler/cache_comm"
)

type Scheduler struct {
	Caches []*cache_protocol.CacheInfo
}

func NewScheduler() *Scheduler {
	scheduler := Scheduler{}
	scheduler.Caches = make([]*cache_protocol.CacheInfo, 0)

	return &scheduler
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
	_, err = conf.JobInfoList(jobInfoFiles)
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
	
	fmt.Println("Scheduler caches: ")
	fmt.Println(scheduler.Caches)
}