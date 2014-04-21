package configuration

/* this file defines the data structures
used in configuration. Mainly used by 
main.go */

//used to configure the proxy
type Configuration struct {
	//where the hdfs namenode is located
	HdfsHostname string
	HdfsPort string

	//where the cache layer is supposed to be run
	ServerPort string
	ServerHost string

	//sets whether to retry connection to HDFS if it fails
	//necessary because sometimes HDFS doesn't respond immediately
	RetryHdfs bool

	//states the port number on which to run the cache_info_server
	//instance
	CacheInfoPort string
}

//constructor for the configuration object
func NewConfiguration() *Configuration {
	conf := Configuration{}
	return &conf
}