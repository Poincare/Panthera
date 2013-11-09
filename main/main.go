package main
import (
	//"fmt"
	"net"
	"hdfs_requests"
	"util"
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
func loop(server net.Listener) {
	eventChannel := make(chan hdfs_requests.ProcessorEvent)

	for {
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

		util.Log("Connected to HDFS.");

		//create new process and process the connected client
		processor := hdfs_requests.NewProcessor(eventChannel)
		go processor.HandleConnection(conn, hdfs);
		go processor.HandleHDFS(conn, hdfs);
	}
}

func main() {
	util.Log("Starting...")

	config.hdfsHostname = "127.0.0.1"
	config.hdfsPort = "1101"
	config.serverHost = "0.0.0.0"
	config.serverPort = "1035"
	config.retryHdfs = false

	server, err := net.Listen("tcp", config.serverHost + ":" + config.serverPort)
	if err != nil {
		util.LogError(err.Error());
		return
	}

	loop(server);
}
