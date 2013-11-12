package util

import (
	"fmt"
)

var LoggingEnabled bool = true

//log general information
func Log(x string) {
	if LoggingEnabled {
		fmt.Println(x);
	}
}

//log an error message
func LogError(x string) {
	if LoggingEnabled {
		fmt.Println("ERROR: " + x);
	}
}

//log some received data
func LogRecvd(source string, x string) {
	if LoggingEnabled {
		fmt.Println("RECVD FROM: " + source + ", DATA: " + x)
	}
}