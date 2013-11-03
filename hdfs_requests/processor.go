package hdfs_requests

import (
	"namenode_rpc"
	"net"
	"util"
	"fmt"
	"caches"
	"log"
)

type Processor struct {
	//array of the past requests received by this processor
	//presumably by the same client (i.e. one client per processor)
	PastRequests []namenode_rpc.RequestPacket

	//cache called when we get a "GetFileInfo" cache
	gfiCache *caches.GetFileInfoCache
}

func NewProcessor() *Processor { 
	p := Processor{}

	/* configuration settings here */
	gfiCacheSize := 10

	p.gfiCache = caches.NewGetFileInfoCache(gfiCacheSize)
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
				resp := p.Process(rp)
				log.Println("Resp: ", resp)
				//if a response was found in one the caches currently running
				//we can respond to the client immediately, we don't need to 
				//wait for HDFS do anything
				if resp != nil {
					util.Log("found in the cache")
				} else {
					//TODO this is an important consideration - do we still need
					//to send it to the server? For some methods, this might be
					//important! For example, analytics would be affected if we
					//kept swooping these up
					util.Log("not found in cache")
					hdfs.Write(byteBuffer);
				}
			}

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
func (p *Processor) Process(req *namenode_rpc.RequestPacket) namenode_rpc.ResponsePacket {
	methodName := string(req.MethodName)
	log.Println("Trying to do something here...")
	if methodName == "getFileInfo" {
		//this will return a correct response if we can find one
		//cached, or it will simply return nil so that we now
		//that a result was not found in the
		
		res := p.gfiCache.Query(req)
		return res
	} else {
		log.Println("Not getFileInfo call, trying to return nil now")
	}
	return nil
}

