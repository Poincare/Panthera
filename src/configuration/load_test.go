package configuration

import (
	"testing"
	"reflect"
	"fmt"
)

var exampleConf *Configuration

func tarit() {
	exampleConf = NewConfiguration()
	exampleConf.HdfsHostname = "127.0.0.1"
	exampleConf.HdfsPort = "1337"
	exampleConf.ServerPort = "1337"
	exampleConf.ServerHost = "127.0.0.1"
	exampleConf.RetryHdfs = false
}

func TestLoadFile (t *testing.T) {
	tarit()

	conf, err := LoadFile("example_conf.json")
	if err != nil {
		fmt.Println("Encountered error in loading configuration file: ", 
		err)
		t.FailNow()
	}
	fmt.Println("Example conf: ", exampleConf)
	fmt.Println("Conf: ", conf, " err: ", err)

	if !reflect.DeepEqual(*conf, *exampleConf) {
		t.Fail()
	}
}