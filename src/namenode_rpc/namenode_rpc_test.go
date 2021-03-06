package namenode_rpc
import (
	"testing"
	"fmt"
	"bytes"

	//for deep equal
	"reflect"

	//for hexadecimal report
	"strconv"
)

func TestNewHeaderPacket(t *testing.T) {
	h := NewHeaderPacket()

	//assert conditions on the different
	//defaults
	if h.version != 4 {
		t.FailNow();
		fmt.Println("h.version: ", h.version)
	}
	if string(h.header) != "hrpc" {
		t.FailNow();
		fmt.Println("h.header: ", string(h.header))
	}
	if h.auth_method != 80 {
		t.FailNow();
		fmt.Println("h.auth_method: ", string(h.auth_method))
	}
	if h.serialization_type != 0 {
		t.FailNow();
		fmt.Println("h.serialization_type: ", string(h.serialization_type))
	}
	fmt.Println("pass")
}

//Unit test for Headers()
//Needs some testing itself with netcat, not
//sure if it is a valid test yet
func TestHeaders(t *testing.T) {
  header := NewHeaderPacket()

  byte_packet := header.Bytes()
  if byte_packet == nil {
  	t.FailNow();
  }


  //first four bytes are "hrpc"
  //fifth byte: version
  //sixth byte: auth method
  //seventh byte: serialization method
  expected := []byte{104, 114, 112, 99, 4, 80, 0}

  if bytes.Compare(byte_packet, expected) != 0 {
		fmt.Println("byte_packet: ", byte_packet, "expected: ", expected)
  	t.FailNow();
  }

  fmt.Println("byte packet: ", byte_packet)
}

/* ResponsePacket tests*/

/* RequestPacket tests */

//test RequestPacket constructor
func TestNewRequestPacket(t *testing.T) {
	req_packet := NewRequestPacket()
	if req_packet == nil {
		t.FailNow()
	}

}

//test RequestPacket.Load()
func TestRequestPacketLoad(t *testing.T) {
	req_packet := NewRequestPacket()

	//the byte buffer should have:
	//total length: something, no idea
	//header length: 1
	//header serialized: "!"
	//request length: 1
	//request serialized: "!"

	//whole test case has been extracted from Wireshark
	//hopefully they have their RPC implementation straight
	buf := []byte{0, 0, 0, 60, 0, 0, 0, 2, 0, 10, 103, 101, 116, 
		76, 105, 115, 116, 105, 110, 103, 0, 0, 0, 2, 0, 16, 106, 
		97, 118, 97, 46, 108, 97, 110, 103, 46, 83, 116, 114, 105, 
		110, 103, 0, 12, 47, 117, 115, 101, 114, 47, 104, 100, 117, 
		115, 101, 114, 0, 2, 91, 66, 0, 0, 0, 0}

	err := req_packet.Load(buf)
	if err != nil {
		t.FailNow()
	}

	if req_packet.Length != 60 {
		t.Fail()
	}

	if req_packet.PacketNumber != 2 {
		t.Fail()
	}

	if req_packet.NameLength != 10 {
		t.Fail()
	}

	if string(req_packet.MethodName) != "getListing" {
		t.Fail()
	}

	if req_packet.ParameterNumber != 2 {
		t.FailNow()
	}

	p1 := req_packet.Parameters[0]
	p2 := req_packet.Parameters[1]


	//test first parameter with type and value
	if p1.TypeLength != 16 {
		fmt.Println("failed here, type length: ", p1.TypeLength)
		t.Fail()
	}

	if string(p1.Type) != "java.lang.String" {
		fmt.Println("failed, type: ", string(p1.Type))
		t.Fail()
	}

	if p1.ValueLength != 12 {
		fmt.Println("failed, value length: ", string(p1.ValueLength))
		t.Fail()
	}

	if string(p1.Value) != "/user/hduser" {
		fmt.Println("failed, value: ", string(p1.Value))
		t.Fail()
	}

	//test second parameter for type and value
	if p2.TypeLength != 2 {
		t.Fail()
	}

	if string(p2.Type) !=  "[B" {
		t.Fail()
	}

}

/* GetFileInfoResponse tests */

//test the constructor
func TestNewGetFileInfoResponse(t *testing.T) {
	gf := NewGetFileInfoResponse ()
	if gf == nil {
		t.FailNow()
	}
}

//this is the test case, but in hexademical because converting is hard
var GetFileInfoResponseTestCase []byte = []byte{0,0,0,1,0,0,0,0,0,46,111,
114,103,
	46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,100,102,115,46,
	112,114,
	111,116,111,99,111,108,46,72,100,102,115,70,105,108,101,83,116,97,116,
	117,115,0,
	46,111,114,103,46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,
	100,102,
	115,46,112,114,111,116,111,99,111,108,46,72,100,102,115,70,105,108,101,
	83,116,97,
	116,117,115,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,1,65,230,
	55,105,95,
	0,0,0,0,0,0,0,0,1,237,6,104,100,117,115,101,114,10,115,117,112,101,114,
	103,114,111,
	117,112}


//expected response by the parser
var GetFileInfoExpected GetFileInfoResponse = GetFileInfoResponse {
	PacketNumber: 1,
	Success: 0,
	ObjectNameLength: 46,
	ObjectName: []byte("org.apache.hadoop.hdfs.protocol.HdfsFileStatus"),
	ObjectNameLength2: 46,
	ObjectName2: []byte("org.apache.hadoop.hdfs.protocol.HdfsFileStatus"),
	FilePermission: 0, 
	FileNameLength: 0,
	FileName: []byte{},
	FileSize: 0,
	IsDirectory: 1,
	BlockReplicationFactor: 0,
	BlockSize: 0,
	ModifiedTime: 1382546893151,
	AccessTime: 0,

	FilePermission2: 493,

	OwnerNameLength: 6,
	OwnerName: []byte("hduser"),
	GroupNameLength: 10,
	GroupName: []byte("supergroup"),
	Loaded: true,
	LoadedBytes: GetFileInfoResponseTestCase}

//test the loading of the packet number
func TestLoadPacketNumber(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if gf.PacketNumber != 1 {
		fmt.Println("Packet number incorrect, value:", gf.PacketNumber)
		t.FailNow()
	}
}

//test the loading of the success value
func TestLoadSuccess(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if gf.Success != 0 {
		fmt.Println("Incorrect success value: ", gf.Success)
		t.FailNow()
	}
}

func TestLoad(t *testing.T) {
	gf := GetFileInfoResponse{}
	gf.Load(GetFileInfoResponseTestCase)

	//the parser did not parsing according to the expected
	//result
	if(!reflect.DeepEqual(GetFileInfoExpected, gf)) {
		t.Fail()
	}
}

//test the loading of the object name length
func TestLoadObjectNameLength(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if gf.ObjectNameLength != 46 {
		t.FailNow()
	}
}


func TestLoadObjectName(t *testing.T) {
	gf := NewGetFileInfoResponse()
	fmt.Println("test case: ", GetFileInfoResponseTestCase)

	gf.Load(GetFileInfoResponseTestCase)

	if string(gf.ObjectName) != "org.apache.hadoop.hdfs.protocol.HdfsFileStatus" {
		fmt.Println("Incorrect ObjectName len: ", len(gf.ObjectName))

		fmt.Println("Incorrect ObjectName bytes: ")
		fmt.Print("[")
		for i := 0; i<len(gf.ObjectName); i++ {
			fmt.Print(strconv.FormatInt(int64(gf.ObjectName[i]), 16), ",")
		}
		fmt.Print("]")

		fmt.Println("Incorrect ObjectName: ", string(gf.ObjectName))
		t.FailNow()
	}
}

func TestLoadObjectNameLength2(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if gf.ObjectNameLength2 != 46 {
		t.FailNow()
	}
}

func TestLoadObjectName2(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	
	if string(gf.ObjectName2) != "org.apache.hadoop.hdfs.protocol.HdfsFileStatus" {
		t.FailNow()
	}
}

func TestLoadFilePermission(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if gf.FilePermission != 0 {
		t.FailNow()
	}
}

func TestLoadFileNameLength(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if gf.FileNameLength != 0 {
		t.FailNow()
	}
}
//	FileName []byte
func TestLoadFileName(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if string(gf.FileName) != "" {
		t.Fail()
	}
}

//	FileSize uint64
func TestLoadFileSize(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if gf.FileSize != 0 {
		t.Fail()
	}
}

//	IsDirectory byte
func TestLoadIsDirectory(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if gf.IsDirectory != 1 {
		t.Fail()
	}
}

func TestLoadBlockReplicationFactor(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if gf.BlockReplicationFactor != 0 {
		t.Fail()
	}
}

func TestLoadBlockSize(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if gf.BlockSize != 0 {
		t.Fail()
	}
}
	
func TestLoadModifiedTime(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if gf.ModifiedTime != 1382546893151 {
		t.Fail()
	}
}

func TestLoadAccessTime(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	if gf.AccessTime != 0 {
		t.Fail()
	} 	
}

func TestBytes(t *testing.T) {
	gf := NewGetFileInfoResponse()
	gf.Load(GetFileInfoResponseTestCase)

	buf := gf.Bytes()
	if !reflect.DeepEqual(buf, GetFileInfoResponseTestCase) {
		fmt.Println("Not equal: ", buf, GetFileInfoResponseTestCase)
		t.Fail()
	}

	gf = NewGetFileInfoResponse()
	buf = gf.Bytes()

	if buf != nil {
		t.Fail()
	}
}

var CreateRequestPacketTestCase []byte = []byte{0,0,0,248,0,0,0,2,0,6,99,114,
	101,97,116,101,0,0,0,6,0,16,106,97,118,97,46,108,97,110,103,46,83,116,114,
	105,110,103,0,29,47,117,115,101,114,47,104,100,117,115,101,114,47,114,112,
	99,45,116,101,115,116,47,110,101,119,102,105,108,101,0,44,111,114,103,46,
	97,112,97,99,104,101,46,104,97,100,111,111,112,46,102,115,46,112,101,114,
	109,105,115,115,105,111,110,46,70,115,80,101,114,109,105,115,115,105,111,
	110,0,44,111,114,103,46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,
	102,115,46,112,101,114,109,105,115,115,105,111,110,46,70,115,80,101,114,
	109,105,115,115,105,111,110,1,237,0,16,106,97,118,97,46,108,97,110,103,46,
	83,116,114,105,110,103,0,36,68,70,83,67,108,105,101,110,116,95,78,79,78,77,
	65,80,82,69,68,85,67,69,95,45,49,54,57,51,55,48,51,49,52,50,95,49,0,7,98,111,
	111,108,101,97,110,1,0,5,115,104,111,114,116,0,1,0,4,108,111,110,103,0,0,0,0,
	4,0,0,0,0}

var CreateRequestPacketExpectedFP string = "/user/hduser/rpc-test/newfile"


func TestGetCreateRequestPath (t *testing.T) {
	rq := NewRequestPacket()
	rq.Load(CreateRequestPacketTestCase)

	fmt.Println("Number of parameters: ", rq.ParameterNumber)
	fmt.Println("Parameters: ", rq.Parameters)
	//has to match the epected name
	if rq.GetCreateRequestPath() != CreateRequestPacketExpectedFP {
		fmt.Println("Not equal: ", rq.GetCreateRequestPath(), CreateRequestPacketExpectedFP)
		t.Fail()
	}
}

//GetListingResponse tests
//simple constructor test
func TestNewGetListingResponse (t *testing.T) {
	glr := NewGetListingResponse()
	if glr == nil {
		t.Fail()
	}
}

var BlockBeingWrittenTestCase = []byte{0,0,1,11,0,0,0,3,0,24,98,108,111,99,107,115,66,101,105,110,103,87,114,105,116,116,101,110,82,101,112,111,114,116,0,0,0,2,0,59,111,114,103,46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,100,102,115,46,115,101,114,118,101,114,46,112,114,111,116,111,99,111,108,46,68,97,116,97,110,111,100,101,82,101,103,105,115,116,114,97,116,105,111,110,0,59,111,114,103,46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,100,102,115,46,115,101,114,118,101,114,46,112,114,111,116,111,99,111,108,46,68,97,116,97,110,111,100,101,82,101,103,105,115,116,114,97,116,105,111,110,0,14,49,50,55,46,48,46,48,46,49,58,49,51,56,57,0,41,68,83,45,54,55,56,48,48,50,48,54,49,45,49,50,55,46,48,46,49,46,49,45,49,51,56,57,45,49,51,56,55,55,51,52,56,50,50,52,50,54,195,155,195,100,255,255,255,215,4,220,11,33,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,255,0,0,0,0,0,2,91,74,0,0,0,0,0}

//test the Bytes() method of the RequestPacket

/*
func TestRequestPacketBytes(t *testing.T) {
	rp := NewRequestPacket()
	rp.Load(CreateRequestPacketTestCase)

	if !reflect.DeepEqual(rp.Bytes(), CreateRequestPacketTestCase) {
		t.Fail()
		fmt.Println("real param number: ", rp.ParameterNumber)
		fmt.Println("TestRequestPacketBytes, not equal: ")
		fmt.Println(rp.Bytes())
		fmt.Println("Real type length for i = 2: ", rp.Parameters[2].TypeLength)
		fmt.Println("Real type for i=2", rp.Parameters[2].Type)
		fmt.Println(CreateRequestPacketTestCase)
	}
} */


/*
func TestRequestPacketBytesReverse(t *testing.T) {
	rp := NewRequestPacket()
	rp.Load(BlockBeingWrittenTestCase)
	fmt.Println("Initial rp.Length: ", int(rp.Length))

	bytes := rp.Bytes()
	new_rp := NewRequestPacket()
	new_rp.Load(bytes)

	if !reflect.DeepEqual(bytes, BlockBeingWrittenTestCase) {
		fmt.Println("Not equal: ");
		fmt.Println("Correct: ", BlockBeingWrittenTestCase)
		fmt.Println("Bytes(): ", bytes)
		t.Fail();
	}

	if !reflect.DeepEqual(new_rp.MethodName, rp.MethodName) {
		fmt.Println("Method names do not match")
		t.Fail()
	}

	if !reflect.DeepEqual(new_rp.ParameterNumber, rp.ParameterNumber) {
		fmt.Println("ParameterNumbers not equal")
		t.Fail()
	} else {
		fmt.Println("Parameter numbers match: ", rp.ParameterNumber)
	}

	if !reflect.DeepEqual(new_rp.Parameters[0], rp.Parameters[0]) {
		fmt.Println("Parameter[0] does not match")
		t.Fail()
	} else {
		fmt.Println("Paramter[0] matches: ", rp.Parameters[0])
	}

	if !reflect.DeepEqual(new_rp.Parameters[1], rp.Parameters[1]) {
		fmt.Println("Parameter[1] does not match")
		t.Fail()
	} else {
		fmt.Println("Paramter[1] matches: ", rp.Parameters[1])
	}
	
	fmt.Println("rp.Length: ", rp.Length)
	fmt.Println("Length of bytes: ", len(bytes))
	fmt.Println("Length of expected: ", len(BlockBeingWrittenTestCase)) 
} */


//TEST THE AUTHPACKET STRUCTURE

func TestAuthPacketConstructor(t *testing.T) {
	ap := NewAuthPacket()
	if ap == nil {
		t.Fail();
	}
}

var AuthPacketTestCase = []byte{0,0,0,58,46,111,114,103,46,97,112,97,
99,104,101,46,104,97,100,111,111,112,46,104,100,102,115,46,112,114,111,
116,111,99,111,108,46,67,108,105,101,110,116,80,114,111,116,111,99,111,
108,1,0,7,100,104,97,105,118,97,116,0,0,0,0,108,0,0,0,0,0,18,103,101,
116,80,114,111,116,111,99,111,108,86,101,114,115,105,111,110,0,0,0,2,
0,16,106,97,118,97,46,108,97,110,103,46,83,116,114,105,110,103,0,46,
111,114,103,46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,
100,102,115,46,112,114,111,116,111,99,111,108,46,67,108,105,101,110,
116,80,114,111,116,111,99,111,108,0,4,108,111,110,103,0,0,0,0,0,0,0,61,0}

func TestAuthPacketLoad(t *testing.T) {
	ap := NewAuthPacket()
	ap.Load(AuthPacketTestCase)

	fmt.Println("AuthenticationLength: ", ap.AuthenticationLength)
	if ap.AuthenticationLength != 58 {
		t.Fail()
	}

	/*
	if ap.AuthenticationBits != []byte('.org.apache.hadoop.hdfs.protocol.ClientProtocodhaivat') {
		t.Fail()
	}*/

	if ap.Length != 108 {
		t.Fail()
	}

	if ap.PacketNumber != 0 {
		t.Fail()
	}

	if ap.NameLength != 18 {
		t.Fail()
	}

	if !reflect.DeepEqual(ap.MethodName, []byte("getProtocolVersion")) {
		t.Fail()
	}

	if ap.ParameterNumber != 2 {
		t.Fail()
	}

	if len(ap.Parameters) != 2 {
		t.Fail()
	}

	first_param := ap.Parameters[0]
	if first_param.TypeLength != 16 {
		t.Fail()
	}
	if string(first_param.Type) != "java.lang.String" {
		t.Fail()
	}

	fmt.Println("Authentication bits: ", string(ap.AuthenticationBits))
}

var GenericResponsePacketTestCase = []byte{0,0,0,2,0,0,0,0,0,59,111,114,103,46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,100,102,115,46,115,101,114,118,101,114,46,112,114,111,116,111,99,111,108,46,68,97,116,
97,110,111,100,101,82,101,103,105,115,116,114,97,116,105,111,110,0,59,111,114,103,46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,100,102,115,46,115,101,114,118,101,114,46,112,114,111,116,111,99,111,108,46,68,
97,116,97,110,111,100,101,82,101,103,105,115,116,114,97,116,105,111,110,0,14,49,50,55,46,48,46,48,46,49,58,50,48,49,48,0,41,68,83,45,54,55,56,48,48,50,48,54,49,45,49,50,55,46,48,46,49,46,49,45,50,48,49,48,45,49,51,56,
55,55,51,52,56,50,50,52,50,54,195,155,195,100,255,255,255,215,4,220,11,33,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,255,0,0,0,0,0}

var GenericResponsePacketRes = GenericResponsePacket{
	PacketNumber: 2,
	Success: 0,
	ObjectNameLength1: 59,
	ObjectName1: []byte("org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration"),
	ObjectNameLength2: 59,
	ObjectName2: []byte("org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration"),
	ParameterLength: 14,
	ParameterValue: []byte("127.0.0.1:2010")}

func TestGenericResponsePacketLoad(t *testing.T) {
	ap := NewGenericResponsePacket([]byte{}, 0)
	ap.Load(GenericResponsePacketTestCase)
	grpr := GenericResponsePacketRes

	if ap.PacketNumber != grpr.PacketNumber {
		t.Fail()
	}

	if ap.Success != grpr.Success {
		t.Fail()
	}

	if !reflect.DeepEqual(ap.ObjectName1, grpr.ObjectName1) {
		t.Fail()
	}

	if !reflect.DeepEqual(ap.ObjectNameLength1, grpr.ObjectNameLength1) {
		t.Fail()
	}
	
	if ap.ObjectNameLength2 != grpr.ObjectNameLength2 {
		t.Fail()
	}

	if !reflect.DeepEqual(ap.ObjectName2, grpr.ObjectName2) {
		t.Fail()
	}

	if ap.ParameterLength != grpr.ParameterLength {
		t.Fail()
	}

	if !reflect.DeepEqual(ap.ParameterValue, grpr.ParameterValue) {
		t.Fail()
	}
	
	fmt.Println(GenericResponsePacketRes)
	fmt.Println(*ap)
}
