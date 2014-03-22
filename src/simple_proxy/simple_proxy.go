package main

import (
	"fmt"
	"net"
	"flag"
	"encoding/hex"
)

func handleClientConnection(conn net.Conn, sv net.Conn) {
	for {
		//we are just making the buffer the size of a 
		//TCP packet mostly because for this testing 
		//proxy, memory doesn't matter
		buffer := make([]byte, 65535)
		bytesRead, err := conn.Read(buffer)
		fmt.Println("Read from client.")
		if err != nil {
			fmt.Println("COuld not read from client: ", err)
			return
		}

		buffer = buffer[0:bytesRead]
		fmt.Println("Buffer: \n")
		fmt.Println(hex.Dump(buffer))
		sv.Write(buffer)
	}
}

func handleServerConnection(conn net.Conn, sv net.Conn) {
	for {
		buffer := make([]byte, 65535)
		bytesRead, err := sv.Read(buffer)
		if err != nil {
			fmt.Println("Error in reading from server: ", err)
			return 
		}

		buffer = buffer[0:bytesRead]
		conn.Write(buffer)
		fmt.Println("Server buffer: \n")
		fmt.Println(hex.Dump(buffer))
	}
}

func handleClient(ln net.Listener, sv net.Conn) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error occurred in accepting client. Ignoring...")
			continue
		}
		
		go handleServerConnection(conn, sv)
		go handleClientConnection(conn, sv)
	}
}

func main() {
	var proxy_port string
	flag.StringVar(&proxy_port, "from", "2010", "The from port")
	var server_port string
	flag.StringVar(&server_port, "to", "1389", "The to port")

	flag.Parse()

	ln, err := net.Listen("tcp", ":"+ proxy_port)
	if err != nil {
		fmt.Println("Could not listen on port: ", proxy_port)
		fmt.Println("Quitting!")
		return
	}

	sv, err := net.Dial("tcp", "127.0.0.1:"+server_port)
	if err != nil {
		fmt.Println("Could not connect to server on port: ", server_port)
		fmt.Println("Quitting!")
		return
	}
	
	handleClient(ln, sv)
}
