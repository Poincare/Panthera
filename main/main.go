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
	"data_requests"
	"time"
)

/* 
* TASK
* The server seems to be reading the bytes, but the loading is not 
* getting the address correctly
*/

//used to configure the proxy
type Configuration struct {
	//where the hdfs namenode is located
	hdfsHostname string
	hdfsPort string

	//where the cache layer is supposed to be run
	serverPort string
	serverHost string

	//sets whether to retry connection to HDFS if it fails
	//necessary because sometimes HDFS doesn't respond immediately
	//TODO not implemented yet
	retryHdfs bool
}

var config Configuration;

//main reactor function called by main
func loop(server net.Listener, caches *caches.CacheSet, dnMap *configuration.DataNodeMap) {
	fmt.Println("looping...")
	eventChannel := make(chan hdfs_requests.ProcessorEvent)
	
	for {
		fmt.Println("Waiting for a connection...")
		conn, err := server.Accept()
		fmt.Println("Accepted connection...");

		if err != nil {
			util.LogError(err.Error())
			continue
		}
		util.DebugLog("Client Accepted; no errors received...");

		//set up connection to HDFS
		util.DebugLog("Connecting to HDFS host: " + string(config.hdfsHostname) + ":" + string(config.hdfsPort))
		hdfs, hdfs_err := net.Dial("tcp", config.hdfsHostname + ":" + config.hdfsPort)
		if hdfs_err != nil {
			util.LogError(err.Error())
			continue
		}
		util.DebugLog("Dialed HDFS...")

		//check if the socket connected
		if hdfs == nil {
			util.LogError("Connection to HDFS failed. Closing client socket");
			conn.Close()
			continue
		}
		util.DebugLog("Connected to HDFS.")
	

		//create new process and process the connected client
		//pass it the caches that are currently initialized
		processor := hdfs_requests.NewProcessor(eventChannel, caches, dnMap)
		go processor.HandleConnectionReimp(conn, hdfs);
		go processor.HandleHDFS(conn, hdfs);
	}
}

//listen on a port connected to one of the datanodes
//will be run as a goroutine
func loopData(listener net.Listener, location *configuration.DataNodeLocation, cache *caches.DataCache) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			util.LogError("Could not accept connection from the DataNode: " + err.Error())
		}

		//connection object to the datanode location
		dataNode, err := net.Dial("tcp", location.Address())
		if err != nil || dataNode == nil {
			log.Println("Could not connet to DataNode: ", err)
			dataNode = nil
		}

		//create a new processor this set
		dataProcessor := data_requests.NewProcessor(cache, location)
		go dataProcessor.HandleConnection(conn, dataNode)
		go dataProcessor.HandleDataNode(conn, dataNode)
	}
}

//takes a data node map and runs a main loop for each of 
//the location and port combinations
func runDataNodeMap(dataNodeMap configuration.DataNodeMap, cache *caches.DataCache) {
	for port, location := range dataNodeMap {
		listener, err := net.Listen("tcp", ":" + string(port))
		if err != nil || listener == nil {
			log.Println("Could not listen on relay port:", port, " because: ", err.Error(), listener, location)
		}

		fmt.Println("Listener: ", listener)
		time.Sleep(100)

		//set up a main loop for this (port, location) tuple
		go loopData(listener, location, cache)
	}
}

func main() {
	/* setup the metadata layer */
	util.LoggingEnabled = false
	util.Log("Starting...")

	config.hdfsHostname = "127.0.0.1"
	config.hdfsPort = "1102"

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
	
	//disable the metadata cache for now
	cacheSet.Disable()

	server, err := net.Listen("tcp", config.serverHost + ":" + config.serverPort)
	log.SetOutput(ioutil.Discard)
	
	err = util.Init()
	if err != nil {
		fmt.Println("Error ocurred in initializing the utilities: ", err)
		return
	}

	if err != nil {
		util.LogError(err.Error());
		return
	}

	/* setup the data cache */
	dataCacheSize := 10
	dataCache := caches.NewDataCache(dataCacheSize)
	
	/* setup the data layer */
	//TODO should probably be a configuration option
	portOffset := 2010
	dataNode := configuration.NewDataNodeLocation("127.0.0.1", "1389")
	dataNodeList := make([]*configuration.DataNodeLocation, 0)
	dataNodeList = append(dataNodeList, dataNode)
	dataNodeMap := configuration.MakeDataNodeMap(dataNodeList, portOffset)
	
	//retrofitted modification
	//dataNodeMap[configuration.Port(1389)] = dataNodeMap[configuration.Port(2010)]
	//delete(dataNodeMap, configuration.Port(2010))

	
	//start the datanode servers
	runDataNodeMap(dataNodeMap, dataCache)

	/* start the server */
	loop(server, cacheSet, &dataNodeMap)
}


