package hdfs_requests

import (
	"namenode_rpc"
	"net"
	"util"
	"caches"
	"log"
	"fmt"
	"bytes"
	"encoding/binary"
	"time"
	"os"
	"strings"
	"io"
	"encoding/hex"
	"reflect"

	//used for the the DataNodeMap
	"configuration"

	//used for the DataNodeRegistration writable
	"writables"

	//"reflect"
)

type PacketNumber uint32

//used to measure latency in the processor
var TIMECOUNTER time.Time

type Processor struct {
	currentRequest *namenode_rpc.RequestPacket
	
	//set to true if the last request packet received by a
	//processor was an authentication packet. That implies
	//that the next packet will have to be modified since
	//it is a DataNodeRegistration
	lastPacketAuth bool

	//array of the past requests received by this processor
	//presumably by the same client (i.e. one client per processor)
	PastRequests []namenode_rpc.RequestPacket

	//the configured, ready-to-go caches (supplied by main.go)
	cacheSet *caches.CacheSet

	//this map links up packet numbers with
	//request, response pairs. So, a response packet
	//with a packet number of 1 is responding to the request 
	//with the packet number of 1.
	//The Map() method fills in response and request packets as needed
	RequestResponse map[PacketNumber]namenode_rpc.PacketPair

	//the channel processed by EventLoop()
	EventChannel chan ProcessorEvent

	//set to true after the first packet is handled from the client
	HandledFirstPacket bool

	//the second packet is the authentication/registration packet
	PacketsProcessed int

	//log the total time spent
	cachedTimeLogger *log.Logger
	hdfsTimeLogger *log.Logger
	//log useless, debugging things
	randomLogger *log.Logger

	//we need the datanode map to make replacements in block reports
	dataNodeMap *configuration.DataNodeMap

	//skip the response if it has already been filled by the cache
	skipResponse bool

	//start time for measuring latency
	cacheStartTime *time.Time
	nonCacheStartTime *time.Time

	//set by preprocessAuthRequest, 
	//used by preprocessRegistrationResponse
	dataNodeRegistration *writables.DataNodeRegistration
}

//Object constructor. It needs the event_chan to talk with other processors,
//the cacheSet is initialized and used to cache req/resp and the datanodeMap
//is used for ModifyBlockReport().
func NewProcessor(event_chan chan ProcessorEvent, cacheSet *caches.CacheSet,
	datanodeMap *configuration.DataNodeMap) *Processor { 
	p := Processor{}
	p.RequestResponse = make(map[PacketNumber]namenode_rpc.PacketPair)
	p.dataNodeMap = datanodeMap

	p.EventChannel = event_chan
	p.cacheSet = cacheSet
	p.HandledFirstPacket = false
	p.PacketsProcessed = 0
	p.skipResponse = false

	cacheLogFile, err := os.OpenFile("cache_times", os.O_RDWR | os.O_APPEND | os.O_CREATE, 0666)
	if err != nil {
		util.LogError("Failed to open cache log file")
	}
	p.cachedTimeLogger = log.New(cacheLogFile, "", 0)
	
	hdfsLogFile, err := os.OpenFile("hdfs_times", os.O_RDWR | os.O_APPEND | os.O_CREATE, 0666)
	if err != nil {
		util.LogError("Failed open to cache log file")
	}
	p.hdfsTimeLogger = log.New(hdfsLogFile, "", 0)
	randomLogFile, err := os.OpenFile("random_log", os.O_RDWR | os.O_APPEND | os.O_CREATE, 0666)
	if err != nil {
		util.LogError("Failed to open random log file")
	}

	p.randomLogger = log.New(randomLogFile, "", 0)
	go p.EventLoop()

	/* disable the caches or enable them here */
	//p.cacheSet.GetListingCache.Disable()

	return &p
}

//this gets called by both HandleConnection in order to put a 
//Request into one of the caches. Once a corresponding call is made
//to CacheResponse(), the cache spot is "filled"
//TODO need to implement
func (p *Processor) CacheRequest(req *namenode_rpc.RequestPacket) {
	//fmt.Println("Method name from CacheRequest(): ", string(req.MethodName))

	/*
	if(string(req.MethodName) == "getFileInfo") {
		//follow through with the GetFileInfoCache
		fmt.Println("Caching request..., cache size: ", len(p.cacheSet.GfiCache.Cache.RequestResponse))
		p.cacheSet.GfiCache.Cache.AddRequest(req)
		fmt.Println("Cached request. Cache size: ", len(p.cacheSet.GfiCache.Cache.RequestResponse))
		log.Println("Cached GFI Request: ")
	} else */
	if(string(req.MethodName) == "getListing") {
		p.cacheSet.GetListingCache.Cache.AddRequest(req)
	}
}

//this gets called both HandleHDFS in order to put a Response into 
//one of the caches. It looks up a request with the same packet number
//and if such a request exists in the cache, it places itself into that
//spot.
//TODO need to implement
func (p *Processor) CacheResponse(resp namenode_rpc.ResponsePacket) {
	packetNum := caches.PacketNumber(resp.GetPacketNumber())

	//we can hook up a response with a request
	//TODO figure out a better way to make a generic
	//caching mechanism
	if p.cacheSet.GfiCache.Cache.HasPacketNumber(packetNum) {
		p.cacheSet.GfiCache.Cache.AddResponse(resp)
	}
	if p.cacheSet.GetListingCache.Cache.HasPacketNumber(packetNum) {
		p.cacheSet.GetListingCache.Cache.AddResponse(resp)
	}

	log.Println("Cached response: ", resp)
}

//read the length from the client
func (p *Processor) readLength(conn net.Conn) (uint32, error) {
	buf := make([]byte, 40)

	util.DebugLog("readLength: beginning read...")
	bytesRead, read_err := conn.Read(buf)
	util.DebugLog("readLength: ended read...")

	buf = buf[0:bytesRead]
	if bytesRead != 0 {
		fmt.Println("bytes read: ", bytesRead, "buffer: ", buf);
	}

	byteBuffer := bytes.NewBuffer(buf)

	var res uint32
	binary.Read(byteBuffer, binary.BigEndian, &res)
	return res, read_err
}


//takes a request object, and if it is DatanodeRegistration related, it modifies the storageID
//and port number (all operations done in place on req)
//TODO this needs a fix to consider the case where the new port string length != old port string
//length
func (p *Processor) ModifyBlockReport(req *namenode_rpc.RequestPacket) []byte {
	fmt.Println("Modifying block report...")
	//we need to change the port numbers on the blockReport calls
	//so that it registers the cache layer instead of the DN port number

	//first we figure out how far Parameter[1] is offset in the byte array
	var offsetBuffer bytes.Buffer
	binary.Write(&offsetBuffer, binary.BigEndian, req.Length)
	binary.Write(&offsetBuffer, binary.BigEndian, req.PacketNumber)
	binary.Write(&offsetBuffer, binary.BigEndian, req.NameLength)
	offsetBuffer.Write(req.MethodName)
	binary.Write(&offsetBuffer, binary.BigEndian, req.ParameterNumber)
	binary.Write(&offsetBuffer, binary.BigEndian, req.Parameters[0].TypeLength)
	offsetBuffer.Write(req.Parameters[0].Type)
	binary.Write(&offsetBuffer, binary.BigEndian, req.Parameters[0].ValueLength)
	offsetBuffer.Write(req.Parameters[0].Value)
	
	storageParameter := req.Parameters[1]
	
	location := configuration.NewDataNodeLocationAddr(string(storageParameter.Type))
	correspondingPort := location.Port
	//find the corresponding port
	for port, ploc := range *p.dataNodeMap {
		if ploc.Ip == location.Ip && ploc.Port == location.Port {
			correspondingPort = string(port)
		}
	}
	fmt.Println("Corresponding port: ", correspondingPort)

	//something like "DS-678002061-127.0.1.1-1389-1387734822426"
	storageString := string(storageParameter.Value)
	//something like ["DS", "678002061", "127.0.1.1", "1389", "1387734822426"]
	storagePieces := strings.Split(storageString, "-")
	storagePieces[3] = correspondingPort
	storageString = strings.Join(storagePieces, "-")
	fmt.Println("New storageString: ", storageString)
	
	//TODO consider the case where the length of the value is now different
	binary.Write(&offsetBuffer, binary.BigEndian, req.Parameters[1].TypeLength)
	//offsetBuffer.Write([]byte(location.Ip + ":" + correspondingPort))
	offsetBuffer.Write([]byte("127.0.0.1:2010"))
	binary.Write(&offsetBuffer, binary.BigEndian, req.Parameters[1].ValueLength)
	offsetBuffer.Write([]byte("DS-2096826136-127.0.1.1-2010-1395205739838"))
	//offsetBuffer.Write([]byte(storageString))

	
	//now write the rest of the byte array to the end of offsetBuffer 
	loadedBytes := req.LoadedBytes()
	offsetBuffer.Write(loadedBytes[offsetBuffer.Len():])

	req.Load(offsetBuffer.Bytes())

	fmt.Println("Modified block report bytes: ")
	fmt.Println(hex.Dump(offsetBuffer.Bytes()))
	return offsetBuffer.Bytes()
	/*
	req.Parameters[1].Value = []byte(storageString)
	req.Parameters[1].Type = []byte(location.Ip + ":" + correspondingPort) */}
	

//this method takes a request object and operates on it to produce an altered request object
//the primary purpose of this method (currently) is to prevent the DataNode from registering
//with the wrong port number (with the current version of Hadoop, this cannot be managed from
//the configuration files).
//returns the new request packet and also a boolean value that is set to true if the initial
//request packet was modified
func (p *Processor) Preprocess(req *namenode_rpc.RequestPacket) (*namenode_rpc.RequestPacket, bool) {
	//here we check if the datanode is trying to register
	if string(req.MethodName) == "blockReport" || string(req.MethodName) == "blocksBeingWrittenReport" {
		fmt.Println("Modifying request...")
		p.ModifyBlockReport(req)
		fmt.Println("New request.Parameters[1].Type", string(req.Parameters[1].Type))
		fmt.Println("New request.Parameters[1].Value", string(req.Parameters[1].Value))
		
		return req, true
	}
	
	return req, false
}

//called by HandleConnectionReimp in order to process the first packet
//which sets up the protocol
func (p *Processor) HandleConnZeroPacket(conn net.Conn, hdfs net.Conn) error {
	byteBuffer := make([]byte, 6)
	bytesRead, read_err := conn.Read(byteBuffer)
	byteBuffer = byteBuffer[0:bytesRead]
	if read_err != nil {
		return read_err
	}

	_, writeError := hdfs.Write(byteBuffer)
	fmt.Println("Zero packet: ", byteBuffer)

	return writeError
}

func (p *Processor) readAuthPacketLength(conn net.Conn) (uint32, []byte, error) {
	var packetLength uint32
	buf := make([]byte, 4)
	bytesRead, readErr := conn.Read(buf)
	buf = buf[0:bytesRead]
	if readErr != nil || bytesRead != 4 {
		return 0, []byte{}, readErr
	}
	//readErr := binary.Read(conn, binary.BigEndian, &packetLength)
	/*
	if readErr != nil {
		return packetLength, readErr
	} */
	//return packetLength, readErr
	byteBuffer := bytes.NewBuffer(buf)
	fmt.Println("buf before: ", byteBuffer.Bytes())
	binary.Read(byteBuffer, binary.BigEndian, &packetLength)
	fmt.Println("buf: ", byteBuffer.Bytes(), "pack len: ", packetLength)
	return packetLength, buf, readErr
}

//called by preprocessAuthPacket() to modify the authentication packet to change the 
//datanode location. In essence, it tricks the namenode
//into believing the datanode is somewhere it (the DataNode) isn't
func (p *Processor) preprocessAuthRegister(authPacket *namenode_rpc.AuthPacket, loadedBytes []byte) []byte {
	//figure out the length of assembled vs. the length of the loadedBytes
	packetBytes := authPacket.Bytes()
	fmt.Println("Length of packet bytes: ", len(packetBytes))
	fmt.Println("Length of loaded bytes: ", len(loadedBytes))
	fmt.Println("Diff bytes: ", loadedBytes[len(packetBytes):])
	fmt.Println("Diff string: ", string(loadedBytes[len(packetBytes):]))

	//we load the "diff bytes" into the Writable DataNodeRegistration
	diffBuffer := bytes.NewBuffer(loadedBytes[len(packetBytes):])
	dataNodeRegistration := writables.NewDataNodeRegistration()
	err := dataNodeRegistration.Read(diffBuffer)
	if err != nil {
		util.DebugLogger.Println("Error occurred in preprocessing DataNodeRegistration: ", err)
		util.DebugLogger.Println("Not submitting registration.")
		return []byte{}
	}

	//we make changes to the DataNodeRegistration
	dataNodeRegistration.Name = "dhaivat-GA-870A-UD3:2010"
	dataNodeRegistration.StorageID = "DS-2096826136-127.0.1.1-2010-1395205739838"

	resDiffBuffer := new(bytes.Buffer)
	err = dataNodeRegistration.Write(resDiffBuffer)
	fmt.Println("ResDiffBuffer: ")
	fmt.Println(hex.Dump(resDiffBuffer.Bytes()))
	if err != nil {
		util.DebugLogger.Println("Error occurred in preprocessing DataNodeRegistration: ", err)
		util.DebugLogger.Println("Not submitting registration.")
		return []byte{}
	}

	resBytes := []byte(packetBytes)
	resBytes = append(resBytes, resDiffBuffer.Bytes()...)
	fmt.Println("Res bytes: ")
	fmt.Println(hex.Dump(resBytes))

	p.dataNodeRegistration = dataNodeRegistration

	return resBytes

	/*
	correctBytes := []byte{0,24,100,104,97,105,118,97,116,45,71,65,45,56,55,48,65,45,85,68,51,58}
	correctBytes = append(correctBytes, []byte("2010")...)
	//there might be an error right here
	correctBytes = append(correctBytes, []byte{0,42,68,83,45,50,48,57,54,56,50,54,49,51,54,45,49,50,55,46,48,46,49,46,49,45}...)
	correctBytes = append(correctBytes, []byte("2010")...)
	correctBytes = append(correctBytes, []byte{45,49,51,57,53,50,48,53,55,51,57,56,51,56,195,155,195,100,255,255,255,215,108,110,46,95,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,255,0,0,0,0}...)
	packetBytes = append(packetBytes, correctBytes...)
	fmt.Println("Packet bytes: ")
	fmt.Println(hex.Dump(packetBytes))
	fmt.Println("Loaded bytes: ")
	fmt.Println(hex.Dump(loadedBytes))
	return packetBytes */
	
	//TODO make this generic so that we can actually set a port #, id, etc.
	/*
	correctBytes := []byte{0, 24, 100, 104, 97, 105, 118, 97, 116, 45, 71, 65, 45, 56, 55, 48, 65, 45, 85, 68, 51, 58}
	correctBytes = append(correctBytes, []byte("2010")...)
	correctBytes = append(correctBytes, []byte{0, 41, 68, 83, 45, 54, 55, 56,
	48, 48, 50, 48, 54, 49, 45, 49, 50, 55, 46, 48, 46, 49, 46, 49, 45}...)
	correctBytes = append(correctBytes, []byte("2010")...)
	correctBytes = append(correctBytes, []byte{45, 49, 51, 56, 55, 55, 51, 52, 56, 50, 50, 52, 50, 54, 195, 155, 195, 
		100, 255, 255, 255, 215, 4, 220, 11, 33, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
		0, 0, 0, 255, 0, 0, 0, 0}...) 

	fmt.Println("Correct bytes: ", string(correctBytes))
	packetBytes = append(packetBytes, correctBytes...)
	fmt.Println("Packet bytes: ", packetBytes)
	fmt.Println("Loaded bytes: ", loadedBytes) */
}

//decides how to preprocess authentication packets
func (p *Processor) preprocessAuthPacket(authPacket *namenode_rpc.AuthPacket, loadedBytes []byte) []byte {
	fmt.Println("Preprocessing auth packet...	")
	if string(authPacket.MethodName) == "register" {
		return p.preprocessAuthRegister(authPacket, loadedBytes)
	}

	//if it is not to be modified, just return the loadedBytes
	return loadedBytes
}

//called to handle the authentication packet
func (p *Processor) HandleConnAuthPacket(conn net.Conn, hdfs net.Conn) error {
	util.DebugLog("Reading auth packet...")
	//first we have to read in the authorization bits length
	authLength, authLengthBuf, lengthErr := p.readAuthPacketLength(conn)
	if lengthErr != nil {
		return lengthErr
	}

	//then we read in the actual authorization signature
	authSignature := make([]byte, authLength)
	bytesRead, readErr := conn.Read(authSignature)
	if readErr != nil || bytesRead != int(authLength) {
		util.DebugLog("Error occurred while reading auth signature.")
		return readErr
	}
	authSignature = authSignature[0:authLength]

	//read another length field in
	lengthLeft, lengthBuf, lengthErr := p.readAuthPacketLength(conn)
	if lengthErr != nil {
		return lengthErr
	}

	restBuf := make([]byte, lengthLeft)
	bytesRead, readErr = conn.Read(restBuf)
	restBuf = restBuf[0:bytesRead]
	if readErr != nil {
		return readErr
	}

	finalBuf := []byte{}
	finalBuf = append(finalBuf, authLengthBuf...)
	finalBuf = append(finalBuf, authSignature...)
	finalBuf = append(finalBuf, lengthBuf...)
	finalBuf = append(finalBuf, restBuf...)
	
	//deal with the auth packet processing
	authPacket := namenode_rpc.NewAuthPacket()
	authPacket.Load(finalBuf)
	finalBuf = p.preprocessAuthPacket(authPacket, finalBuf)

	fmt.Println("Auth packet authentication method name: ", string(authPacket.MethodName))
	_, writeError := hdfs.Write(finalBuf)
	
	return writeError
}

//read the length of the request packet from the connection
func (p *Processor) readRequestPacketLength(conn net.Conn) (uint32, []byte, error) {
	var packetLength uint32
	buf := make([]byte, 4)
	bytesRead, readErr := conn.Read(buf)
	buf = buf[0:bytesRead]
	if readErr != nil {
		return 0, []byte{}, readErr
	}
	util.DebugLogger.Println("Read error value in readRequestPacketLength: ", readErr)

	//readErr := binary.Read(conn, binary.BigEndian, &packetLength)
	/*
	if readErr != nil {
		return packetLength, readErr
	} */
	//return packetLength, readErr
	byteBuffer := bytes.NewBuffer(buf)
	binary.Read(byteBuffer, binary.BigEndian, &packetLength)

	return packetLength, buf, readErr	
}
 
//read the request packet from the connection
func (p *Processor) readRequestPacket(conn net.Conn) (*namenode_rpc.RequestPacket, error) {
	util.DebugLogger.Println("Now calling readRequestPacketLength.")
	packetLength, lengthBuf, lengthError := p.readRequestPacketLength(conn)
	if lengthError != nil {
		return nil, lengthError
	}
	util.DebugLogger.Println("Done with readRequestPacketLength. Packet length: ", packetLength)

	restBuf := make([]byte, int(packetLength))
	bytesRead, readError := conn.Read(restBuf)
	fmt.Println("read request packet, bytesRead: ", bytesRead)
	if bytesRead == 0 || readError != nil {
		return nil, readError
	}

	finalBuf := []byte{}
	finalBuf = append(finalBuf, lengthBuf...)
	finalBuf = append(finalBuf, restBuf...)

	reqPacket := namenode_rpc.NewRequestPacket()
	reqPacket.Load(finalBuf)
	fmt.Println("Read request with method name: ", string(reqPacket.MethodName))

	p.currentRequest = reqPacket

	return reqPacket, nil
}

func (p *Processor) preprocessBeingWrittenReport(reqPacket *namenode_rpc.RequestPacket) (*namenode_rpc.RequestPacket, bool) {
	//TODO need to generalize this
	reqPacket.Parameters[1].Value = []byte("DS-2096826136-127.0.1.1-2010-1395205739838")

	return reqPacket, true
}

//look through the "register" request packet and save the stuff
//that will be used in modifying the response to the request
func (p *Processor) processRegisterRequest(reqPacket *namenode_rpc.RequestPacket) (*namenode_rpc.RequestPacket, bool) {
	fmt.Println("In processRegisterRequest(): ")
	
	//full byte structure
	loadedBytes := reqPacket.LoadedBytes()

	//the bytes that do not include the writable data
	packetBytes := reqPacket.BytesNoPad()

	//the difference between the two should be just the writable data
	//which is what we are interested in.
	dataBytes := loadedBytes[len(packetBytes):]

	fmt.Println("Loaded bytes: ")
	fmt.Println(hex.Dump(reqPacket.LoadedBytes()))

	fmt.Println("reqPacket.Bytes(): ")
	fmt.Println(hex.Dump(reqPacket.BytesNoPad()))

	fmt.Println("dataBytes: ")
	fmt.Println(hex.Dump(dataBytes))

	dataByteBuffer := bytes.NewBuffer(dataBytes)

	//we take the data and pack it into a DataNodeRegistration object
	p.dataNodeRegistration = writables.NewDataNodeRegistration()
	p.dataNodeRegistration.Read(dataByteBuffer)
	
	//modify the dataNodeRegistration to reflect the values we want
	p.dataNodeRegistration.Name = "127.0.0.1:2010"

	//get the new dataBytes
	dataResBuffer := new(bytes.Buffer)
	p.dataNodeRegistration.Write(dataResBuffer)

	//put together packetBytes and dataBytes to create the modified packet
	resPacket := namenode_rpc.NewRequestPacket()
	resBuf := append(packetBytes, dataResBuffer.Bytes()...)	
	resPacket.Load(resBuf)

	return resPacket, true
}

func (p *Processor) preprocessRequestPacket(reqPacket *namenode_rpc.RequestPacket) (*namenode_rpc.RequestPacket, bool) {
	fmt.Println("Called preprocessRequestPacket()")
	if string(reqPacket.MethodName) == "blocksBeingWrittenReport" {
		return p.preprocessBeingWrittenReport(reqPacket)
	}

	if string(reqPacket.MethodName) == "register" {
		return p.processRegisterRequest(reqPacket)
	}

	fmt.Println("Ended preprocessRequestPacket()")
	return reqPacket, false
}

func (p *Processor) recordCachedLatency() {
	diff := time.Now().Sub(*p.cacheStartTime).Nanoseconds()
	util.MetaCachedLatencyLogger.Println(diff)
}

func (p *Processor) recordNonCachedLatency() {
	diff := time.Now().Sub(*p.nonCacheStartTime).Nanoseconds()
	util.NonMetaCachedLatencyLogger.Println(diff)
}

//processes general request packets (e.g. checks against cache, responds,
//modifies, etc.)
func (p *Processor) HandleRequestPacket(conn net.Conn, hdfs net.Conn) error {

	//read the whole request packet (length first)
	util.DebugLogger.Println("Now calling readRequstPacket()")
	reqPacket, reqPacketError := p.readRequestPacket(conn)
	util.DebugLogger.Println("Done with readRequestPacket()")
	if reqPacketError != nil {
		return reqPacketError
	}

	//preprocess the request
	//reqPacket, _ = p.Preprocess(reqPacket)
	reqPacket, modified := p.preprocessRequestPacket(reqPacket)

	t := time.Now()
	p.cacheStartTime = &t

	//check the cache and write the corresponding request
	util.DebugLogger.Println("Calling process method...")
	respPacket := p.Process(reqPacket)
	util.DebugLogger.Println("Done with process method, respPacket.")

	//hit in the cache
	if respPacket != nil {
		p.skipResponse = true
		util.DebugLogger.Println("Writing resp packet, type: ", reflect.TypeOf(respPacket), " bytes: \n", hex.Dump(respPacket.GetBuf()))
		conn.Write(respPacket.GetBuf())
		
		p.recordCachedLatency()
		fmt.Println("Diff in cached time: ", time.Now().Sub(*p.cacheStartTime).Nanoseconds())

		util.DebugLogger.Println("Done writing resp packet, bytes") 
	} else {
		p.CacheRequest(reqPacket)
	}

	//write the request to HDFS (differently depending on
	//whether or not the request was modified)
	if !modified {
		fmt.Println("Unmodified request written: ")
		fmt.Println(hex.Dump(reqPacket.LoadedBytes()))
		hdfs.Write(reqPacket.LoadedBytes())
		t := time.Now()
		p.nonCacheStartTime = &t
	} else {
		fmt.Println("Modified request written: ")
		fmt.Println(hex.Dump(reqPacket.LoadedBytes()))

		hdfs.Write(reqPacket.LoadedBytes())
	}
	return nil
}


//this gets called by the main function on a new instance of Processor
//when we get a new connection
func (p *Processor) HandleConnectionReimp(conn net.Conn, hdfs net.Conn) {
	for {
		//initialize the buffer, etc.
		//if it is the first packet, pass it onto a different method
		switch(p.PacketsProcessed) {
		//process the first packet (which sets up the protocol)
		case 0:
			err := p.HandleConnZeroPacket(conn, hdfs)
			if err != nil {
				util.DebugLog("Error reading first packet of connection.")
				//if we have an EOF error we can close the lconnection.
				if err == io.EOF {
					util.DebugLog("Client connection closed. Closing local socket...")
					conn.Close()
				}
			}
			p.PacketsProcessed++
			util.DebugLog("Handled zero packet")

		case 1:
			p.lastPacketAuth = true
			p.HandleConnAuthPacket(conn, hdfs)
			p.PacketsProcessed++
			util.DebugLog("Handled auth packet")
			util.DebugLogger.Println("Handled Authentication Packet!")
			
		//if it is the second packet, it is the authentication packet
		//after that, process them as request packets
		default:
			util.DebugLogger.Println("Starting to handle request packet...")
			err := p.HandleRequestPacket(conn, hdfs)
			if err != nil {
				util.DebugLogger.Println("Error handling request packet.")
				if err == io.EOF {
					util.DebugLogger.Println("Client connection closed. Closing local socket...")
					return
				}
			}
			p.PacketsProcessed++
		}

	}
}

//this gets called by the main function on a new instance of Processor
//when we get a new connection
func (p *Processor) HandleConnection(conn net.Conn, hdfs net.Conn) {
	util.Log("Handling connection...")
	for {
		byteBuffer := make([]byte, 1024)
		bytesRead := 0
		var read_err error

		//if we're still on the first packet, we can't read in the length
		if(!p.HandledFirstPacket) {
			//fmt.Println("HAVE NOT HANDLED FIRST PACKET YET.")
			bytesRead, read_err = conn.Read(byteBuffer);
			byteBuffer = byteBuffer[0:bytesRead]
			p.HandledFirstPacket = true
		} else {
			//fmt.Println("Processing a full request.");
			len, err := p.readLength(conn)
			util.DebugLog("Read length: " + string(len));

			if err != nil || len == 0 {
				//if we get an error reading the length, that
				//means the socket was closed, so we will 
				//just return this socket
				return
			}

			byteBuffer = make([]byte, len-4)
			bytesRead, read_err = conn.Read(byteBuffer)
			byteBuffer = byteBuffer[0:bytesRead]

			var finalBuffer bytes.Buffer
			binary.Write(&finalBuffer, binary.BigEndian, len)
			finalBuffer.Write(byteBuffer)

			byteBuffer = finalBuffer.Bytes()
		}
		/* else if p.PacketsProcessed == 1 {
			authenticationLength, _ := p.readLength(conn)
			byteBuffer = make([]byte, authenticationLength)
		} */


		if read_err != nil {
			util.LogError(read_err.Error())
			//fmt.Println("error: ", read_err.Error())
			conn.Close()
			hdfs.Close()
			return
		}

		util.Log("Handling the packet...")

		TIMECOUNTER = time.Now()
		//fmt.Println("BYTES READ: ", bytesRead)
		
		if bytesRead > 0 {
			rp := namenode_rpc.NewRequestPacket()
			err := rp.Load(byteBuffer)
			if err != nil {
				util.LogError("Error in loading request packet: " + err.Error())
			} else {
				//first we process the request in case we want to 
				//weed out anything (e.g. port numbers for the DN)
				rp, modified := p.Preprocess(rp)
				//fmt.Println("Method name: ", string(rp.MethodName))
				//fmt.Println("Modified: ", modified)

				//deal with checking the cache
				resp := p.Process(rp)
				log.Println("Resp: ", resp)

				//if a response was found in one the caches currently running
				//we can respond to the client immediately, we don't need to 
				//wait for HDFS do anything
				if resp != nil {
					fmt.Println("Cache hit!")
					conn.Write(resp.Bytes())
					//hdfs.Write(byteBuffer)
					//fmt.Println("Sent to client, time elapsed (ns): ", time.Now().Sub(TIMECOUNTER).Nanoseconds())
					p.cachedTimeLogger.Println(time.Now().Sub(TIMECOUNTER).Nanoseconds())
				} else {
					//fmt.Println("Cache miss!, methodname: ", string(rp.MethodName))
					//if it wasn't found in any of the
					//caches, we should cache the request
					util.Log("Trying to cache a request...")
					//fmt.Println("Rp.length: ", rp.Length)
					p.CacheRequest(rp)

					//TODO this is an important consideration - do we still need
					//to send it to the server? For some methods, this might be
					//important! For example, analytics would be affected if we
					//kept swooping these up
					util.Log("not found in cache")

					/*
					if !reflect.DeepEqual(byteBuffer, rp.Bytes()) {
						fmt.Println("Byte comparison FAILED")
						if modified {
							p.randomLogger.Println("not equal: ")
							p.randomLogger.Println("correct: ", byteBuffer)
							p.randomLogger.Println("bytes(): ", rp.Bytes())
						}
					} else {
						fmt.Println("Byte comparison SUCCESSFUL")
					} */
					if !modified {
						hdfs.Write(byteBuffer)
					} else {
						//fmt.Println("modified true, byteBuffer: ", byteBuffer)
						//fmt.Println("loadedBytes: ", rp.LoadedBytes())
						hdfs.Write(rp.LoadedBytes())
				  }
				}
			}
			p.PacketsProcessed += 1
		}
	}
}

func (p *Processor) preprocessRegistrationResponse(genericResp *namenode_rpc.GenericResponsePacket) *namenode_rpc.GenericResponsePacket {
	//get the bytes from the structure
	packetBytes := genericResp.Bytes()

	//get the bytes actually received
	readBytes := genericResp.Buf
	
	//get the Writable portion
	writable := readBytes[len(packetBytes):]
	fmt.Println("Writable: ", writable)
	fmt.Println("StrWrita: ", string(writable))

	//TODO need to make this generic with an implementation of writables
	genericResp.ParameterValue = []byte("127.0.0.1:1389")
	genericResp.ParameterLength = uint16(len(genericResp.ParameterValue))
	
	//create a buffer where we can write confirming information
	resDiffBuffer := new(bytes.Buffer)

	//set the storageid to the local value
	p.dataNodeRegistration.StorageID = 
	"DS-2096826136-127.0.1.1-1389-1395205739838"
	err := p.dataNodeRegistration.WriteWithoutName(resDiffBuffer)
	if err != nil {
		util.DebugLogger.Println("Failed to preprocess registration response.")
		return nil
	}

	correctBytes := []byte(packetBytes)
	correctBytes = append(correctBytes, resDiffBuffer.Bytes()...)

	/*
	genericResp.ParameterValue = []byte("127.0.0.1:1389")
	genericResp.ParameterLength = uint16(len(genericResp.ParameterValue))
	correctBytes := packetBytes
	correctBytes = append(correctBytes, []byte{0,42,68,83,45,50,48,57,54,56,50,54,49,51,54,45,49,50,55,46,48,46,49,46,49,45}...)
	correctBytes = append(correctBytes, []byte("1389")...)
	correctBytes = append(correctBytes, []byte{45,49,51,57,53,50,48,53,55,51,57,56,51,56,195,155,195,100,255,255,255,215,108,110,
		46,95,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,255,0,0,0,0}...) */

	genericResp.Buf = correctBytes
	fmt.Println("In preprocessor, genericResp.Buf: ")
	fmt.Println(hex.Dump(genericResp.Buf))

	fmt.Println("In preprocessor, genericResp Bytes: ")
	fmt.Println(hex.Dump(genericResp.LoadedBytes())) 
	return genericResp
}

//preprocess HDFS responses. Calls other methods depending on the type
//of modification needed.
func (p *Processor) preprocessHDFS(genericResp *namenode_rpc.GenericResponsePacket) *namenode_rpc.GenericResponsePacket {
	if string(genericResp.ObjectName1) == "org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration" {
		fmt.Println("Now preprocessing registration response...")
		genericResp := p.preprocessRegistrationResponse(genericResp)
		fmt.Println("Preprocessed response bytes: ");
		fmt.Println(hex.Dump(genericResp.LoadedBytes()))
	} else {
		fmt.Println("Did not have to preprocess registration response.")
	}
	return genericResp
}

//handle the connection to the HDFS server.
//reads in the response, transforms it if needed (e.g. in the case of a 
//DataNodeRegistration) and then sends it to the client.
func (p *Processor) HandleHDFS(conn net.Conn, hdfs net.Conn) {
	for {
		buf := make([]byte, namenode_rpc.HDFS_PACKET_SIZE)

		//Read() blocks
		bytesRead, readErr := hdfs.Read(buf)
		if p.nonCacheStartTime != nil {
			p.recordNonCachedLatency()
			p.nonCacheStartTime = nil
		}

		if readErr != nil {
			util.DebugLog("Error occurred while reading from HDFS.")
			if readErr == io.EOF {
				util.DebugLog("HDFS socket closed. Closing local socket...")
				hdfs.Close()
				return
			}
		}

		buf = buf[0:bytesRead]
		//TODO POTENTIAL BUG the packet number should always be at the front of the response packet
		//but, the docs don't actually explicitly mention this
		var packetNumber uint32
		byteBuffer := bytes.NewBuffer(buf)

		//read in the packet number (should be a uint32)
		binary.Read(byteBuffer, binary.BigEndian, &packetNumber)

		//create a generic response packet. Since we know the packet
		//number, it doesn't particularly matter what type of packet it 
		//actually is
		//genericResp := namenode_rpc.NewGenericResponsePacket(buf, packetNumber)
		genericResp := namenode_rpc.BuildResponsePacket(buf, packetNumber, p.currentRequest)

		//load in the buffer contents as field values
		genericResp.Load(buf)
		//receivedBytes := buf

		//TODO this is a useful hack, but probably will not work at scale.
		switch genericResp.(type) {
		case *namenode_rpc.GenericResponsePacket:
			genericResp := genericResp.(*namenode_rpc.GenericResponsePacket)
			genericResp = p.preprocessHDFS(genericResp)
		}

		//detects EOF's etc.
		if readErr != nil {
			util.LogError(readErr.Error())
			conn.Close()
			hdfs.Close()
			return
		}

		if(bytesRead > 0) {
			//fmt.Println("From HDFS: \n", genericResp.Bytes(), "\nlength: ", len(genericResp.Bytes()))
			//fmt.Println("Time in getting response: ", time.Now().Sub(TIMECOUNTER).Nanoseconds())
			p.hdfsTimeLogger.Println(time.Now().Sub(TIMECOUNTER).Nanoseconds())
 
			//cache the response (CacheResponse should find out if there is
			//a matching request to this response)
			p.CacheResponse(genericResp)

			//proxy the read data to the associated client socket
			util.DebugLogger.Println("About to write to connection...")
			if !p.skipResponse {
				/*
				fmt.Println("Received response: ")
				fmt.Println(hex.Dump(receivedBytes))
				fmt.Println("Writing response: ")
				fmt.Println(hex.Dump(genericResp.GetBuf())) */	
				conn.Write(genericResp.GetBuf())
			} else {
				p.skipResponse = false
			}
			util.DebugLogger.Println("Written to connection.")
		}
	}
}


//process a request packet
//with the cache, this should look inside the cache
//and either return a response to send to the client
//or, return a nil packet
func (p *Processor) Process(req *namenode_rpc.RequestPacket) namenode_rpc.ResponsePacket {
	methodName := string(req.MethodName)
	log.Println("Trying to do something here...")
	if methodName == "getFileInfo" {
		util.DebugLogger.Println("method is GFI, querying GFICache...")

		//this will return a correct response if we can find one
		//cached, or it will simply return nil so that we now
		//that a result was not found in the
		res := p.cacheSet.GfiCache.Query(req)
		return res
	} else if methodName == "getListing" {
		fmt.Println("Checking getListing cache...")
		res := p.cacheSet.GetListingCache.Query(req)
		fmt.Println("Checked getListing cache.")
		return res
	} else if methodName == "create" {
		//if a new object is being created, we're going to to wrap it
		//in a ProcessorEvent object and fire it off to the other processors
		filepath := req.GetCreateRequestPath()
		event := NewObjectCreatedEvent(filepath)
		
		//we send the event in a nonblocking fashion so that this
		//goroutine does *not* block since it replies directly to
		//the client
		go func() {
			p.EventChannel <- *event
		}()
	} else {
		log.Println("Not getFileInfo call, trying to return nil now")
	}
	return nil
}

//fills in the correct packets into RequestResponse mapping
func (p *Processor) MapRequest(req namenode_rpc.ReqPacket) {
	//get the value and check if the packetnumber currently exists
	//in the mapping
	packetNum := PacketNumber(req.GetPacketNumber())
	pair, present := p.RequestResponse[packetNum]

	//if it is not present, a new "slot" has to be made in the map
	if !present {
		pair := namenode_rpc.NewPacketPair()
		pair.Request = req
		p.RequestResponse[packetNum] = *pair
	} else {
		//if it is present, put in the correct slot
		pair.Request = 	req
		p.RequestResponse[packetNum] = pair
	}
}

func(p *Processor) MapResponse(resp namenode_rpc.ResponsePacket) {
	packetNum := PacketNumber(resp.GetPacketNumber())
	pair, present := p.RequestResponse[packetNum]

	//if it is not present, a new "slot" has to be made in the map
	if !present {
		pair := namenode_rpc.NewPacketPair()
		pair.Response = resp
		p.RequestResponse[packetNum] = *pair
	} else {
		//if it is present, put in the correct slot
		pair.Response = resp
		p.RequestResponse[packetNum] = pair
	}
}

//this method runs in a separate goroutine and processes an 'event list';
//it listens for events, for example, clearing the GFI cache when a 
//different processor gets an event that shows that a file has been added
func (p *Processor) EventLoop() {
	for {
		event := <- p.EventChannel
		log.Println("Event received: ", event)
	}
}
