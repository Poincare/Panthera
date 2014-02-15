package main

import (
	"fmt"
	"io/ioutil"
	"strings"
)


func main() {
	noCacheLatency, _ := ioutil.ReadFile("no_cached_latency")
	cachedLatency, _ := ioutil.ReadFile("cached_latency")
	
	noCacheLatencyStr := string(noCacheLatency)
	cachedLatencyStr := string(cachedLatency)

	fmt.Println("no cache latency str: ", noCacheLatencyStr)
	fmt.Println("Cached latency str: ", noCacheLatencyStr)
	noCachePieces := strings.Split(noCacheLatencyStr, "\n")
	cachedPieces := strings.Split(cachedLatencyStr, "\n")
	
	for i := 0; i < len(noCachePieces); i++ {
		piece := noCachePieces[i] + ", " + cachedPieces[i]
		fmt.Println(piece)
	}
}










