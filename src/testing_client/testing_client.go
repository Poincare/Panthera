package main

import (
	"net"
	"fmt"
	"bufio"
	"encoding/binary"
	"bytes"
	"time"
)

/*
* A utility that allows us to test the latency involved in 
* processing certain packets, e.g. for metadata testing with
* getListing calls 
*/

//byte structures for the packets used for testing

//getListing call on the /user/hduser directory
var GetListingRequest []byte = []byte{0,0,0,60,0,0,0,2,
	0,10,103,101,116,76,105,115,116,105,110,103,0,0,0,2,
	0,16,106,97,118,97,46,108,97,110,103,46,83,116,114,
	105,110,103,0,12,47,117,115,101,114,47,104,100,117,
	115,101,114,0,2,91,66,0,0,0,0,0}

var ProtocolRequest []byte = []byte{0,0,0,57,46,111,114,103,46,97,112,
97,99,104,101,46,104,97,100,111,111,112,46,104,100,102,115,46,112,114,
111,116,111,99,111,108,46,67,108,105,101,110,116,80,114,111,116,111,99,
111,108,1,0,6,104,100,117,115,101,114,0,0,0,0,108,0,0,0,0,0,18,103,101,
116,80,114,111,116,111,99,111,108,86,101,114,115,105,111,110,0,0,0,2,0,
16,106,97,118,97,46,108,97,110,103,46,83,116,114,105,110,103,0,46,111,
114,103,46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,100,
102,115,46,112,114,111,116,111,99,111,108,46,67,108,105,101,110,116,
80,114,111,116,111,99,111,108,0,4,108,111,110,103,0,0,0,0,0,0,0,61,0}

var InitiateRequest []byte = []byte{104,114,112,99,4,80,0}

func main() {
	fmt.Println("Starting...")

	//describes where the cache layer is sitting
	SERVER := "127.0.0.1"
	PORT := "1035"

	conn, err := net.Dial("tcp", SERVER + ":" + PORT)
	if err != nil {
		fmt.Println("Error in connecting to cache layer: ", err)
		return
	}

	//write to the cache layer
	conn.Write(InitiateRequest)

	//hack
	time.Sleep(1000 * time.Millisecond)
	conn.Write(ProtocolRequest)
	time.Sleep(10000 * time.Millisecond)
	//conn.Write(GetListingRequest)

	//read the length
	lengthBuf := make([]byte, 4)
	connReader := bufio.NewReader(conn)
	bytesRead, err := connReader.Read(lengthBuf)

	fmt.Println("bytes read: ", bytesRead)
	lengthBuffer := bytes.NewBuffer(lengthBuf)
	var length uint32
	binary.Read(lengthBuffer, binary.BigEndian, &length)

	fmt.Println("Received length: ", length)
}