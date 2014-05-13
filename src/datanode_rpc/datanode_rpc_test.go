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

var DataResponseTestCase []byte = []byte{0,0,1,0,0,2,0,0,0,0,0,0,0,0,0,0,
	0,0,17,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,9,101,122,199,95,115,111,
	109,101,116,101,120,116,10,0,0,0,0,0}

var DataResponseTestCase2 []byte = []byte {0,0,1,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,
	14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,6,4,64,144,10,116,114,101,101,115,
	10,0,0,0,0,0}

var DataResponseTestCase3 []byte = []byte{0,0,1,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,15,
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,7,125,167,232,228,103,111,108,97,110,
	103,10,0,0,0,0,0}

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
	Data: []byte("sometext"),
	Buf: DataResponseTestCase}

var DataResponseTestExpected2 DataResponse = DataResponse {
	StatusCode: 0,
	ChecksumType: 1,
	ChunkSize: 512,
	ChunkOffset: 0,
	DataLength: 14,
	InBlockOffset: 0,
	SequenceNumber: 0,
	LastPacketNumber: 1,
	DataLength2: 6,
	Data: []byte("trees"),
	Buf: DataResponseTestCase2}

var DataResponseTestExpected3 DataResponse = DataResponse {
	StatusCode: 0,
	ChecksumType: 1,
	ChunkSize: 512,
	ChunkOffset: 0,
	DataLength: 15,
	InBlockOffset: 0,
	SequenceNumber: 0,
	LastPacketNumber: 1,
	DataLength2: 7,
	Data: []byte("golang"),
	Buf: DataResponseTestCase3}

func TestDataResponseConstructor(t *testing.T) {
	dResp := NewDataResponse()

	if dResp == nil {
		t.Fail()
	}
}

func TestDataResponseLoad(t *testing.T) {
	dResp := NewDataResponse()
	dResp.Load(DataResponseTestCase)

	if !reflect.DeepEqual(*dResp, DataResponseTestExpected) {
		fmt.Println("Not equal, expected: ", DataResponseTestExpected, 
		" got: ", *dResp)
		fmt.Println("Expected data: ", string(DataResponseTestExpected.Data), 
		"got bytes: ", string(dResp.Data))
		t.Fail()
	}
}

//same thing, different test structure
func TestDataResponseLoadTwo(t *testing.T) {
	dResp := NewDataResponse()
	dResp.Load(DataResponseTestCase2)

	if !reflect.DeepEqual(*dResp, DataResponseTestExpected2) {
		fmt.Println("Not equal, expected: ", DataResponseTestExpected2, 
		" got: ", *dResp)
		fmt.Println("Expected data: ", string(DataResponseTestExpected2.Data), 
		"got bytes: ", string(dResp.Data))
		t.Fail()		
	}
}