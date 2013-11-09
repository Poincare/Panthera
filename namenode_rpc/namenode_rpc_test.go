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

	//TODO this part makes absolutely no sense, but 
	//it is how Wireshark seems to be reporting it
	//TODO does the [B have a special meaning attached to it?
	//test second parameter for type and value
	if p2.TypeLength != 2 {
		t.Fail()
	}

	//TODO WTF does this mean
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
var GetFileInfoResponseTestCase []byte = []byte{0,0,0,1,0,0,0,0,0,46,111,114,103,
	46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,100,102,115,46,112,114,
	111,116,111,99,111,108,46,72,100,102,115,70,105,108,101,83,116,97,116,117,115,0,
	46,111,114,103,46,97,112,97,99,104,101,46,104,97,100,111,111,112,46,104,100,102,
	115,46,112,114,111,116,111,99,111,108,46,72,100,102,115,70,105,108,101,83,116,97,
	116,117,115,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,1,65,230,55,105,95,
	0,0,0,0,0,0,0,0,1,237,6,104,100,117,115,101,114,10,115,117,112,101,114,103,114,111,
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

	//TODO not exactly sure what the two 
	//file permission headers specify
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

//TODO currently failing
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

	//TODO this is replicated from the TestLoadObjectName
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

//TODO this whole method is kinda monkeypatched in there
func TestGetCreateRequestPath (t *testing.T) {
	rq := NewRequestPacket()
	rq.Load(CreateRequestPacketTestCase)

	//has to match the epected name
	if rq.GetCreateRequestPath() != CreateRequestPacketExpectedFP {
		fmt.Println("Not equal: ", rq.GetCreateRequestPath(), CreateRequestPacketExpectedFP)
		t.Fail()
	}
}

