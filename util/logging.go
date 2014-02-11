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

var NoCacheLatencyLog *log.Logger
var NoCacheLatencyLogFile = "/home/dhaivat/dev/hadoopproxy/logs/no_cache_latency"

var CachedLatencyLog *log.Logger
var CachedLatencyLogFile = "/home/dhaivat/dev/hadoopproxy/logs/cached_latency"

var DebugLogger *log.Logger
var DebugLogFile = "/home/dhaivat/dev/hadoopproxy/logs/debug.log"

func InitDataReqLogger() error {
	dataLogFile, err := os.OpenFile(DataReqLogFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	DataReqLogger = log.New(dataLogFile, "", 0)
	return nil
}

func InitDebugLogger() error {
	debugLogFile, err := os.OpenFile(DebugLogFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	DebugLogger = log.New(debugLogFile, "", 0)
	return nil
}

func InitNoCacheLatencyLogger() error {
	noCacheLogFile, err := os.OpenFile(NoCacheLatencyLogFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	NoCacheLatencyLog = log.New(noCacheLogFile, "", 0)
	
	//write the units for the data
	NoCacheLatencyLog.Println("(latency times in nanoseconds)")
	return nil
}

func InitCachedLatencyLogger() error {
	cachedLogFile, err := os.OpenFile(CachedLatencyLogFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	CachedLatencyLog = log.New(cachedLogFile, "", 0)
	
	//write the units for the data
	CachedLatencyLog.Println("(latency times in nanoseconds)")
	return nil
}

func Init() error {

	err := InitDataReqLogger()
	if err != nil {
		return err
	}

	err = InitNoCacheLatencyLogger()
	if err != nil {
		return err
	}

	err = InitCachedLatencyLogger()
	if err != nil {
		return err
	}

	err = InitDebugLogger()
	if err != nil {
		return err
	}

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
