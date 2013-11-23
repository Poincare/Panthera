package main
import (
	//"fmt"
	"net"
	"hdfs_requests"
	"util"
	"io/ioutil"
	"log"
	"caches"
	"configuration"
	"fmt"
)

/* 
* TASK
* The server seems to be reading the bytes, but the loading is not 
* getting the address correctly
*/

//used to configure the proxy
type Configuration struct {
	hdfsHostname string
	hdfsPort string
	serverPort string
	serverHost string

	//sets whether to retry connection to HDFS if it fails
	//necessary because sometimes HDFS doesn't respond immediately
	//TODO not implemented yet
	retryHdfs bool
}

var config Configuration;

//main reactor function called by main
func loop(server net.Listener, caches *caches.CacheSet) {
	fmt.Println("looping...")
	eventChannel := make(chan hdfs_requests.ProcessorEvent)

	for {
		fmt.Println("Waiting for a connection...")
		conn, err := server.Accept()

		if err != nil {
			util.LogError(err.Error())
			continue
		}

		util.Log("Client accepted.");

		//set up connection to HDFS
		hdfs, hdfs_err := net.Dial("tcp", config.hdfsHostname + ":" + config.hdfsPort)
		if hdfs_err != nil {
			util.LogError(err.Error())
			continue
		}
		//check if the socket connected
		if hdfs == nil {
			util.LogError("Connection to HDFS failed. Closing client socket");
			conn.Close()
			continue
		}

		util.Log("Connected to HDFS.")

		//create new process and process the connected client
		//pass it the caches that are currently initialized
		processor := hdfs_requests.NewProcessor(eventChannel, caches)
		go processor.HandleConnection(conn, hdfs);
		go processor.HandleHDFS(conn, hdfs);
	}
}

func main() {
	/* setup the metadata layer */
	util.LoggingEnabled = false
	util.Log("Starting...")

	config.hdfsHostname = "127.0.0.1"
	config.hdfsPort = "1101"
	config.serverHost = "0.0.0.0"
	config.serverPort = "1035"
	config.retryHdfs = false

	//initialize the cacheset and the caches
	//within it
	gfiCacheSize := 15
	getListingCacheSize := 15
	cacheSet := caches.NewCacheSet()
	cacheSet.GfiCache = caches.NewGetFileInfoCache(gfiCacheSize)
	cacheSet.GetListingCache = caches.NewGetListingCache(getListingCacheSize)

	server, err := net.Listen("tcp", config.serverHost + ":" + config.serverPort)
	log.SetOutput(ioutil.Discard)

	if err != nil {
		util.LogError(err.Error());
		return
	}

	/* setup the data layer */
	dnl := configuration.NewDataNodeLocation("127.0.0.1", "1337")

	dnLocations := make([]*configuration.DataNodeLocation, 0)
	dnLocations = append(dnLocations, dnl)
	dnLocationMap := make(configuration.DataNodeMap)

	//loop through the dnLocations and set up servers on ports 
	//accordingly

	//TODO should probably be a configuration option
	port_offset := 2000
	for i := 0; i < len(dnLocations); i++ {
		dnLocationMap[port_offset + i] = dnLocations[i]
	}

	/* start the server */
	loop(server, cacheSet)
}


