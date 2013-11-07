package datanode_rpc

import (
	"testing"
	"reflect"
	"fmt"
)

var DataRequestTestCase []byte = []byte{0,17,81,97,18,75,209,17,
	9,75,203,0,0,0,0,0,0,4,164,0,0,0,0,0,0,0,0,0,0,0,0,0,13,112,
	198,35,68,70,83,67,108,105,101,110,116,95,78,79,78,77,65,80,
	82,69,68,85,67,69,95,49,53,48,50,55,50,49,56,50,57,95,49,0,
	0,0,0,}

var DataRequestTestExpected = DataRequest{
	ProtocolVersion: 17,
	Command: 81,
	BlockId: 6994736532565871563,
	Timestamp: 1188,
	StartOffset: 0,
	BlockLength: 880838,
	ClientIdLength: 35,
	ClientId: []byte("DFSClient_NONMAPREDUCE_1502721829_1"),
	AccessIdLength: 0,
	AccessId: []byte{},
	AccessPasswordLength: 0,
	AccessPassword: []byte{},
	AccessTypeLength: 0,
	AccessType: []byte{},
	AccessServiceLength: 0,
	AccessService: []byte{}}

func TestDataRequestConstructor(t *testing.T) {
	dnr := NewDataRequest()
	if dnr == nil {
		t.Fail()
	}
}

func TestDataRequestLoad(t *testing.T) {
	dnr := NewDataRequest()
	dnr.Load(DataRequestTestCase)

	if !reflect.DeepEqual(*dnr, DataRequestTestExpected) {
		fmt.Println("Expected: ", DataRequestTestExpected, " got: ", *dnr)
		t.Fail()
	}
}

func TestDataRequestBytes (t *testing.T) {
	dnr := NewDataRequest()
	dnr.Load(DataRequestTestCase)

	if !reflect.DeepEqual(dnr.Bytes(), DataRequestTestCase) {
		t.Fail()
	}
}

/* Data Response structure unit tests */

var DataResponseTestCase []byte = []byte{0,0,17,0,0,0,0,0,0,0,0,0,
	0,0,0,0,0,0,0,1,0,0,0,9,101,122,199,95,115,111,109,101,116,101,
	120,116,10,0,0,0,0,0}

var DataResponseTestExpected DataResponse = DataResponse {
	StatusCode: 0,
	ChecksumType: 1,
	ChunkSize: 512,
	ChunkOffset: 0,
	DataLength: 17,
	InBlockOffset: 0,
	SequenceNumber: 0,
	LastPacketNumber: 1,
	DataLength2: 9,
	Data: []byte("sometext")}

func TestDataResponseConstructor (t *testing.T) {
	dResp := NewDataResponse()

	if dResp == nil {
		t.Fail()
	}
}


func TestDataResponseLoad(t *testing.T) {
	dResp := NewDataResponse()
	dResp.Load(DataResponseTestCase)

	if !reflect.DeepEqual(*dResp, DataResponseTestExpected) {
		fmt.Println("Not equal, expected: ", DataRequestTestExpected, " got: ", *dResp)
		t.Fail()
	}
}