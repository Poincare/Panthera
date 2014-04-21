/*
* Implements a way to describe Hadoop jobs
* in terms of the files they access from HDFS, 
* etc.
*/
package job_info

import (
	//golang imports
	"encoding/json"
	"io/ioutil"
	"reflect"
	"os/exec"

	//local imports
	"cache_protocol"
	"namenode_rpc"
)

/**
* Example job_info file:
* 
*{
*	JobName: "wordcount"
* FilesAccessed: "gutenberg/*"
*}
*/

type JobInfo struct {
	Name string
	BlocksAccessed []uint64

	//shell command used to execute
	//this job
	CommandPath string
	CommandArgs []string

	//the directory in which to execute Command
	ExecutionDir string

	//not written or accessed from the JSON
	blocksAccessed []*cache_protocol.BlockDescription
}

func NewJobInfo() *JobInfo {
	j := JobInfo{}
	return &j
}

//run the job using Command and ExecutionDir
func (j *JobInfo) Run() (*exec.Cmd, error) {
	c := exec.Command(j.ExecutionDir + "/" + j.CommandPath, j.CommandArgs...)
	c.Dir = j.ExecutionDir

	err := c.Start()
	if err != nil {
		return nil, err
	}

	return c, nil
}

//writes as the JobInfo structure 
//as a JSON string
func (j *JobInfo) Write() ([]byte, error) {
	return json.Marshal(j)
}

func (j *JobInfo) SetBlockDescriptions() {
	numBlocks := len(j.BlocksAccessed)
	j.blocksAccessed = make([]*cache_protocol.BlockDescription, numBlocks)
	for i := 0; i<numBlocks; i++ {
		j.blocksAccessed[i] = cache_protocol.NewBlockDescription()
		j.blocksAccessed[i].BlockId = j.BlocksAccessed[i]
	}

}

func (j *JobInfo) Read(buf []byte) error {
	err := json.Unmarshal(buf, j)
	if err != nil {
		return err
	}

	j.SetBlockDescriptions()
	return nil
}

func (j *JobInfo) ReadFromFile(filepath string) error {
	buf, err := ioutil.ReadFile(filepath)
	if err != nil {
		return err
	}

	return j.Read(buf)
}

//create a getBlockLocations request
func (j *JobInfo) getBlockLocations() {
	r := namenode_rpc.NewRequestPacket()
	r.PacketNumber = 2

}

//This is probably the most complicated method in this package.
//Though the JSON file specifies what files will be needed, it
//does not specify what blocks will be needed, so we have
//to translate the files list into a block list
func (j *JobInfo) GetBlocksAccessed() {
	//don't need the implementation yet, because currently the configuration
	//with block ids, not filenames
}

func (j *JobInfo) ScoreCacheInfo(cacheInfo *cache_protocol.CacheInfo) float64 {
	return j.ScoreCache(cacheInfo.Descr, cacheInfo.Blocks)
}

//the algorithm that tells us how well a job corresponds to a given cache
//returns a number that tells the correlation (the higher it is, 
//the more the job corresponds to the cache contents)
func (j *JobInfo) ScoreCache(cacheDescr *cache_protocol.CacheDescription,
	cachedBlocks *cache_protocol.CachedBlocks) float64 {

	hits := 0
	total := len(j.blocksAccessed)

	/* outline of the algorithm:
	* Loop over the blocks cached.
	* If a block that is in the cache is required by j, 
	* increment hits
	* return hits/total as the score
	* Consequences of the algorithm:
	* If a job has all of its requirements fullfilled, we get score of 1
	* If a job has nothing filled, it has a score of 0
	*/

	blocks := cachedBlocks.Blocks
	for i := 0; i < int(cachedBlocks.NumBlocks); i++ {
		for k := 0; k < len(j.blocksAccessed); k++ {
			if !reflect.DeepEqual(*blocks[i], *j.blocksAccessed[k]) {
				hits++
			}
		}
	}

	return (float64(hits)/float64(total))
}