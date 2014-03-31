package util

import (
	"fmt"
	"log"
	"os"
	"io/ioutil"
)

//this logger is handled by the DataReqLogger
var DataReqLogger *log.Logger
var DataReqLogFile = "../../logs/data_req_log"

var LoggingEnabled bool = true
var DebugLoggingEnabled bool = true

var NoCacheLatencyLog *log.Logger
var NoCacheLatencyLogFile = "../../logs/no_cache_latency"

var CachedLatencyLog *log.Logger
var CachedLatencyLogFile = "../../logs/cached_latency"

var DebugLogger *log.Logger
var DebugLogFile = "../../logs/debug.log"

var MetaCachedLatencyLogger *log.Logger
var MetaCachedLatencyLogFile = "../../logs/meta_cache_latency"

var NonMetaCachedLatencyLogger *log.Logger
var NonMetaCachedLatencyLogFile = "../../logs/non_meta_cache_latency"


//this is type of log is supposed to be used ONLY in debugging 
//(i.e. should not be in th deployed code) because the only 
//purpose it serves is to aid in debugging Panthera itself
var TempLogger *log.Logger

//note that this file is *not* an append log - it is 
//cleared at every run
var TempLoggerLogFile = "../../logs/temp.log"

//NOTE this is a relative path; in deployment, the executable needs to be in the same directory
//this file otherwise there will be problems in loading the logging configuration (it will
//just default to the development values).
var LoggingConfFile = "logging.json"

//load, with a configuration file, the places where the logs are 
//supposed to be going
func InitLoggingConfiguration() error {
	_, err := ioutil.ReadFile(LoggingConfFile)
	if err != nil {
		return err
	}
	return nil
}

func InitTempLogger() error {
	//tempLogFile, err := os.OpenFile(TempLoggerLogFile, os.O_WRONLY | os.O_CREATE, 0666)
	tempLogFile, err := os.Create(TempLoggerLogFile)
	
	if err != nil {
		return err
	}

	TempLogger = log.New(tempLogFile, "", log.Ldate | log.Ltime)
	return nil
}

func InitDataReqLogger() error {
	dataLogFile, err := os.OpenFile(DataReqLogFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	DataReqLogger = log.New(dataLogFile, "", 0)
	return nil
}

func InitDebugLogger() error {
	debugLogFile, err := os.Create(DebugLogFile)
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
	
	return nil
}

func InitNonMetaCachedLatencyLogger() error {
	file, err := os.OpenFile(NonMetaCachedLatencyLogFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	NonMetaCachedLatencyLogger = log.New(file, "", 0)
	
	return nil
}

func InitMetaCachedLatencyLogger() error {
	metaCacheLogFile, err := os.OpenFile(MetaCachedLatencyLogFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	MetaCachedLatencyLogger = log.New(metaCacheLogFile, "", 0)

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

	err = InitMetaCachedLatencyLogger()
	if err != nil {
		return err
	}

	err = InitNonMetaCachedLatencyLogger()
	if err != nil {
		return err
	}

	err = InitTempLogger()
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
