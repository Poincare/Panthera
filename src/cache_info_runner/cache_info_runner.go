/*
* This file is used to debug the CacheInfoServer 
* without attaching it to the rest of the Panthera
*/
package main

import (
	//go packages
	"fmt"

	//local packages
	"cache_info_server"
	"caches"
)


func main() {
	fmt.Println("Starting cache_info_runner...")
	port := "1337"
	cacheSize := 15

	dataCache := caches.NewWritableDataCache(cacheSize)
	server := cache_info_server.NewCacheInfoServer(port, dataCache)
	server.Start()
}