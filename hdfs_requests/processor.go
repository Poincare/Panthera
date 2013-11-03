package hdfs_requests

import (
	"namenode_rpc"
	"net"
	"util"
	"fmt"
)

type Processor struct {
	//array of the past requests received by this processor
	//presumably by the same client (i.e. one client per processor)
	PastRequests []namenode_rpc.RequestPacket
}

func NewProcessor() *Processor { 
	p := Processor{}
	return &p
}

//called by the main function on a new instance of Processor
//when we get a new connection
func (p *Processor) HandleConnection(conn net.Conn, hdfs net.Conn) {
	for {
		byteBuffer := make([]byte, 1024)
		//blocks
		bytesRead, read_err := conn.Read(byteBuffer);
		byteBuffer = byteBuffer[0:bytesRead]

		if read_err != nil {
			util.LogError(read_err.Error())
			conn.Close()
			hdfs.Close()
			return
		}

		if bytesRead > 0 {
			rp := namenode_rpc.NewRequestPacket()
			err := rp.Load(byteBuffer)
			if err != nil {
				fmt.Println("Error in loading request packet: ", err.Error())
			} else {
				fmt.Println("Method received: ", string(rp.MethodName))
			}

			hdfs.Write(byteBuffer);
		}
	}
}

func (p *Processor) HandleHDFS(conn net.Conn, hdfs net.Conn) {
	for {
		byteBuffer := make([]byte, 1024)

		//Read() blocks
		bytesRead, readErr := hdfs.Read(byteBuffer)
		byteBuffer = byteBuffer[0:bytesRead]

		//detects EOF's etc.
		if readErr != nil {
			util.LogError(readErr.Error())
			conn.Close()
			hdfs.Close()
			return
		}
		if(bytesRead > 0) {
			//proxy the read data to the associated client socket
			conn.Write(byteBuffer)
		}
	}
}


//process a request packet
//with the cache, this should look inside the cache
//and either return a response to send to the client
//or, return a nil packet

//TODO at the moment, this doesn't actually look inside a 
//cache at all, it just sends back a nil packet; i.e.
//it is not doing any processing with results at all
func (p *Processor) Process(req *namenode_rpc.RequestPacket) *namenode_rpc.ResponsePacket {
	
	//see above TODO
	return nil
}

