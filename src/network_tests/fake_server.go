package main

import (
	"fmt"
	"net"
)

func main() {
	ln, _ := net.Listen("tcp", ":1035")
	conn, _ := ln.Accept()
	for {
		buf := make([]byte, 1024)
		conn.Read(buf)
		fmt.Println(buf)
	}
}
