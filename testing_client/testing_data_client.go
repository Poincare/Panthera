package main

import (
	//golang imports
	"net"
	"fmt"
)

var DataNodeRequest []byte = []byte{0,17,81,97,18,75,209,17,
	9,75,203,0,0,0,0,0,0,4,164,0,0,0,0,0,0,0,0,0,0,0,0,0,13,112,
	198,35,68,70,83,67,108,105,101,110,116,95,78,79,78,77,65,80,
	82,69,68,85,67,69,95,49,53,48,50,55,50,49,56,50,57,95,49,0,
	0,0,0}

func main() {
	fmt.Println("starting...")

	//where the cache layer is located
	LAYERADDR := "127.0.0.1:2000"

	conn, err := net.Dial("tcp", LAYERADDR)
	if err != nil {
		fmt.Println("Not able to connect to layer: ", err)
		return
	}

	//write the request acros sthe socket
	conn.Write(DataNodeRequest)

	//wait around
	for {}
}