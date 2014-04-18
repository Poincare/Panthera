/*
* Implements a simple
* testing client for the CacheInfoServer.
* Used for debugging and unit tests */

package main

import (
	//go imports
	"net"
	"fmt"
	
	//local imports
	"cache_protocol"
	"writable_processor"
)

func sendDescriptionRequest(server net.Conn) error {
	r := cache_protocol.NewRequest(cache_protocol.REQ_CACHE_DESCRIPTION)
	serverObj := writable_processor.NewConnection(server)

	err := r.Write(serverObj)
	if err != nil {
		return err
	}

	return nil
}

func sendRequest(server net.Conn) {
	err := sendDescriptionRequest(server)
	if err != nil {
		fmt.Println("Could not send request: ", err)
	}
}

func readDescriptionResponse(
	server net.Conn) (*cache_protocol.CacheDescription, error) {

	serverObj := writable_processor.NewConnection(server)
	descr := cache_protocol.NewCacheDescription()
	err := descr.Read(serverObj)
	if err != nil {
		return nil, err
	}

	return descr, nil
}

func readResponse(server net.Conn) {
	descr, err := readDescriptionResponse(server)
	if err != nil {
		fmt.Println("Could not read response: ", err)
	}

	fmt.Println("Received response: ", descr)
}

func main() {
	hostname := "localhost"
	port := "1337"

	server, err := net.Dial("tcp", hostname + ":" + port)
	if err != nil {
		fmt.Println("Could not connection to cache_info_server: ", err)
	}

	sendRequest(server)
	readResponse(server)
}