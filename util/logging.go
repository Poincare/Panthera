package util

import (
	"fmt"
)

//log general information
func Log(x string) {
	fmt.Println(x);
}

//log an error message
func LogError(x string) {
	fmt.Println("ERROR: " + x);
}

//log some received data
func LogRecvd(source string, x string) {
	fmt.Println("RECVD FROM: " + source + ", DATA: " + x)
}