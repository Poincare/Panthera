/*
* Implements a way to describe Hadoop jobs
* in terms of the files they access from HDFS, 
* etc.
*/
package job_info

import (
	"encoding/json"
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
	FilesAccessed []string
}

func NewJobInfo() *JobInfo {
	j := JobInfo{}
	return &j
}

//writes as the JobInfo structure 
//as a JSON string
func (j *JobInfo) Write() ([]byte, error) {
	return json.Marshal(j)
}

func (j *JobInfo) Read(buf []byte) error {
	return json.Unmarshal(buf, j)
}
