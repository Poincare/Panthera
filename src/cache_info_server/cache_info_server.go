/*
* This package implements an information server for the caches.WritableDataCache 
* to be used by the scheduler. Relays information to the scheduler such as the 
* blocks currently cached, lets the scheduler request specific blocks, etc.
*/

package cache_info_server

import (
	//go packages
	"math/rand"
	"net"

	//local packages
	"caches"
)

type CacheInfoServer struct {
	//unique id number for this instance of the server.
	//used primarily for debugging/logging purposes.
	Id uint64

	//port number that the server runs on
	Port string

	//the caches.WritableDataCache associated with this server (each server
	//deals with one cache instance)
	DataCache *caches.WritableDataCache
}

func NewCacheInfoServer(port string, 
	dataCache *caches.WritableDataCache) *CacheInfoServer {

	c := CacheInfoServer{Port: port,
		Id: uint64(rand.Int63n(999999999)),
		DataCache: dataCache}

	return &c
}

//main loop of the server; dispatches goroutines to
//handle clients
func (c *CacheInfoServer) Start() error {
	ln, err := net.Listen("tcp", ":" + c.Port)
	if err != nil {
		return err
	}

	for {
		//blocking call
		client, err := ln.Accept()
		if err != nil {
			return nil
		}

		//set up and run the processor
		proc := NewProcessor(c.DataCache, client)
		go proc.HandleClient()
	}
}
