package configuration

import (
	//go packages
	"io/ioutil"
	"encoding/json"
	"strings"

	//local pacakges
	"job_info"
)

type Configuration struct {
	//network locations of cache_info_server instances
	CacheLocations []CacheLocation

	//Folder which has all of the job_info.JobInfo 
	//description files
	JobInfoDir string
}

func NewConfiguration() *Configuration {
	c := Configuration{}
	return &c
}

func (c *Configuration) Read(buf []byte) error {
	return json.Unmarshal(buf, c)
}

func (c *Configuration) ReadFromFile(filepath string) error {
	contents, err:= ioutil.ReadFile(filepath)
	if err != nil {
		return err
	}

	return c.Read(contents)
}

//get the list of files in the JobInfoDir
func (c *Configuration) JobInfoFiles() ([]string, error) {
	fileInfos, err := ioutil.ReadDir(c.JobInfoDir)
	if err != nil {
		return nil, err
	}

	res := []string{}
	for i := 0; i<len(fileInfos); i++ {
		fileInfo := fileInfos[i]
		name := fileInfo.Name()
		if strings.HasSuffix(name, ".json") {
			res = append(res, name)
		}
	}

	return res, nil
}

//get a list of JobInfo instances by reading files
func (c *Configuration) JobInfoList(
	files []string) ([]*job_info.JobInfo, error) {

	res := make([]*job_info.JobInfo, len(files))
	for i := 0; i < len(files); i++ {
		res[i] = job_info.NewJobInfo()
		err := res[i].ReadFromFile(c.JobInfoDir + "/" + files[i])
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}