package main
import (
	//"fmt"
	"net"
	"hdfs_requests"
	"util"
	"io/ioutil"
	"log"
	"caches"
	"fmt"
	//"data_requests"
	"writable_processor"

	"time"
	"configuration"
	"runtime/pprof"
	"flag"
	"os"
	"os/signal"
)


var config *configuration.Configuration;
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

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
		util.DebugLog("Connecting to HDFS host: " + string(config.HdfsHostname) + ":" + string(config.HdfsPort))
		hdfs, hdfs_err := net.Dial("tcp", config.HdfsHostname + ":" + config.HdfsPort)
		if hdfs_err != nil {
			util.LogError(hdfs_err.Error())
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
	dataCacheSize := 15
	dataCache := caches.NewWritableDataCache(dataCacheSize)

	for {
		util.DebugLogger.Println("Waiting to accept data connection...")
		conn, err := listener.Accept()
		util.DebugLogger.Println("Data client accepted.")
		if err != nil {
			util.LogError("Could not accept connection from the DataNode: " + err.Error())
		}

		//connection object to the datanode location
		dataNode, err := net.Dial("tcp", location.Address())
		if err != nil || dataNode == nil {
			log.Println("Could not connet to DataNode: ", err)
			dataNode = nil
		}

		dataProcessor := writable_processor.New(dataCache)
		//go dataProcessor.GeneralProcessing(conn, dataNode, true)

		go dataProcessor.HandleClient(conn, dataNode)
		//go dataProcessor.HandleDataNode(conn, dataNode)

		/*
		//create a new processor this set
		dataProcessor := data_requests.NewProcessor(cache, location)
		go dataProcessor.HandleConnection(conn, dataNode)
		
		//go dataProcessor.HandleDataNode(conn, dataNode)
		go dataProcessor.BruteForceHandleDataNode(conn, dataNode)

		util.DebugLogger.Println("-----------") */
	}
}

//takes a data node map and runs a main loop for each of 
//the location and port combinations
func runDataNodeMap(dataNodeMap configuration.DataNodeMap, cache *caches.DataCache) {
	for port, location := range dataNodeMap {
		listener, err := net.Listen("tcp", ":" + string(port))
		if err != nil || listener == nil {
			log.Println("Could not listen on relay port:", port, " because: ", err.Error(), listener, location)
			util.DebugLogger.Println("Could not listen on relay port: ", port, " because: ", err.Error(), listener, location)
		}

		fmt.Println("Listener: ", listener)
		time.Sleep(100)

		//set up a main loop for this (port, location) tuple
		go loopData(listener, location, cache)
	}
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
        	f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(){
		for _ = range c {
			if *cpuprofile != "" {
				pprof.StopCPUProfile()
			}
			return
		}
	}()

	/* setup the metadata layer */
	util.LoggingEnabled = false
	util.Log("Starting...")

	/*
	config.HdfsHostname = "162.243.105.47"
	config.HdfsPort = "54310"

	config.ServerHost = "0.0.0.0"
	config.ServerPort = "1035"
	config.RetryHdfs = false
	*/

	var err error
	config, err = configuration.LoadFile("configuration.json")
	if err != nil {
		fmt.Println("Could not read configuration file: ", err)
		fmt.Println("Exiting...")
		return
	}

	//initialize the cacheset and the caches
	//within it
	gfiCacheSize := 15
	getListingCacheSize := 15
	cacheSet := caches.NewCacheSet()
	cacheSet.GfiCache = caches.NewGetFileInfoCache(gfiCacheSize)
	cacheSet.GetListingCache = caches.NewGetListingCache(getListingCacheSize)
	
	//disable the metadata cache for now
	//cacheSet.Disable()

	server, err := net.Listen("tcp", config.ServerHost + ":" + config.ServerPort)
	log.SetOutput(ioutil.Discard)
	
	err = util.Init()
	if err != nil {
		fmt.Println("Error ocurred in initializing the utilities: ", err)
		return
	}
	util.TempLogger.Println("init()ed temporary logging")

	/* setup the data cache */
	dataCacheSize := 10
	dataCache := caches.NewDataCache(dataCacheSize)
	
	/* setup the data layer */
	//TODO should probably be a configuration option
	portOffset := 1389 
	dataNode := configuration.NewDataNodeLocation("162.243.65.142", "1389")
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


