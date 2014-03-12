package configuration

import (
	"io/ioutil"
	"encoding/json"
)

/* Here, we define the configuration loading
mechanism (using JSON) */

//Get the contents of a file
func getContents(path string) ([]byte, error) {
	contents, err := ioutil.ReadFile(path)
	return contents, err
}

//load up the conf object
func LoadIntoObject(confBytes []byte, conf *Configuration) error {
	err := json.Unmarshal(confBytes, conf)
	if err != nil {
		return err
	}

	return nil
}

//Load a configuration file
func LoadFile(path string) (*Configuration, error) {
	conf := NewConfiguration()
	confBytes, err := getContents(path)
	if err != nil {
		return nil, err
	}

	err = LoadIntoObject(confBytes, conf)
	if err != nil {
		return nil, err
	}

	return conf, err
}