package util

import (
	"fmt"
	"log"
	"os"
)

//this logger is handled by the DataReqLogger
var DataReqLogger *log.Logger
var DataReqLogFile = "/home/dhaivat/dev/hadoopproxy/logs/data_req_log"

var LoggingEnabled bool = true
var DebugLoggingEnabled bool = true

func Init() error {
	dataLogFile, err := os.OpenFile(DataReqLogFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	DataReqLogger = log.New(dataLogFile, "", 0)
	return nil
}

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

//log debugging information
func DebugLog(x string) {
	if DebugLoggingEnabled {
		fmt.Println(x);
	}
}
