package main
import (
	"fmt"
	"net"
)

/* 
* TODO Make sure that the byte buffer's get the *full* message
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

//log general information
func log(x string) {
	fmt.Println(x);
}

//log an error message
func log_error(x string) {
	fmt.Println("ERROR: " + x);
}

//log some received data
func log_recvd(source string, x string) {
	fmt.Println("RECVD FROM: " + source + ", DATA: " + x)
}

//forever loop that checks if there are
//any responses from HDFS and then relays
//them
func handleHDFS(conn net.Conn, hdfs net.Conn) {
	for {
		byteBuffer := make([]byte, 1024)

		//Read() blocks
		bytesRead, readErr := hdfs.Read(byteBuffer)
		byteBuffer = byteBuffer[0:bytesRead]

		//detects EOF's etc.
		if readErr != nil {
			log_error(readErr.Error())
			conn.Close()
			hdfs.Close()
			return
		}
		if(bytesRead > 0) {
			conn.Write(byteBuffer)
			log_recvd("HDFS", string(byteBuffer[:]))
		}
	}
}

//called once the server accepts a client
func handleConnection(conn net.Conn, hdfs net.Conn) {
	for {
		byteBuffer := make([]byte, 1024)
		//blocks
		bytesRead, read_err := conn.Read(byteBuffer);
		byteBuffer = byteBuffer[0:bytesRead]

		if read_err != nil {
			log_error(read_err.Error())
			conn.Close()
			hdfs.Close()
			return
		}

		if bytesRead > 0 {
			hdfs.Write(byteBuffer);
			log_recvd("CLIENT", string(byteBuffer[:]))
		}
	}
}

//main reactor function called by main
func loop(server net.Listener) {
	for {
		conn, err := server.Accept()

		if err != nil {
			log_error(err.Error())
			continue
		}

		log("Client accepted.");

		//set up connection to HDFS
		hdfs, hdfs_err := net.Dial("tcp", config.hdfsHostname + ":" + config.hdfsPort)
		if hdfs_err != nil {
			log_error(err.Error())
			continue
		}
		//check if the socket connected
		if hdfs == nil {
			log_error("Connection to HDFS failed. Closing client socket");
			conn.Close()
			continue
		}

		log("Connected to HDFS.");

		go handleConnection(conn, hdfs);
		go handleHDFS(conn, hdfs);
	}
}

func main() {
	config.hdfsHostname = "127.0.0.1"
	config.hdfsPort = "1101"
	config.serverHost = "0.0.0.0"
	config.serverPort = "1035"
	config.retryHdfs = false

	server, err := net.Listen("tcp", config.serverHost + ":" + config.serverPort)
	if err != nil {
		log_error(err.Error());
		return
	}

	loop(server);
}
