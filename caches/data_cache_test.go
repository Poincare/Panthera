package caches

import (
	//go imports
	"testing"
	"reflect"
	"fmt"

	//local imports
	"datanode_rpc"
)

func TestDataCacheConstructor(t *testing.T) {
	dc := NewDataCache(15)
	if dc.CacheSize != 15 {
		t.Fail()
	}

	if dc.Hits != 0 {
		t.Fail()
	}

	if dc.Misses != 0 {
		t.Fail()
	}

	if dc.Enabled != true {
		t.Fail()
	}

	if dc.RpcStore == nil {
		t.Fail()
	}

	if len(dc.RpcStore) != 0 {
		t.Fail()
	}
}

func TestDataCacheDisable (t *testing.T) {
	dc := NewDataCache(15)
	dc.Disable()

	if dc.Enabled != false {
		t.Fail()
	}
}

func TestDataCacheAddRpcPair (t *testing.T) {
	dc := NewDataCache(2)
	req := datanode_rpc.NewDataRequest()
	resp := datanode_rpc.NewDataResponse()
	pair1 := datanode_rpc.NewRequestResponse(req, resp)

	//just setting this to some random "magic" number
	//to see a difference
	req.ProtocolVersion = 19

	pair2 := datanode_rpc.NewRequestResponse(req, resp)
	pair3 := datanode_rpc.NewRequestResponse(req, resp)

	dc.AddRpcPair(*pair1)
	dc.AddRpcPair(*pair2)

	if len(dc.RpcStore) != 2 {
		t.Fail()
	}

	dc.AddRpcPair(*pair3)

	if len(dc.RpcStore) != 2 {
		t.Fail()
	}

	if !reflect.DeepEqual(dc.RpcStore[0], *pair2) {
		t.Fail()
	}

	if !reflect.DeepEqual(dc.RpcStore[1], *pair3) {
		t.Fail()
	}
}

func TestDataCacheQuery(t *testing.T) {
	dc := NewDataCache(2)
	req := datanode_rpc.NewDataRequest()
	resp := datanode_rpc.NewDataResponse()
	pair1 := datanode_rpc.NewRequestResponse(req, resp)

	dc.AddRpcPair(*pair1)

	req2 := datanode_rpc.NewDataRequest()
	req2.ProtocolVersion = 19

	cachedResp := dc.Query(*req2)
	if cachedResp != nil {
		fmt.Println("cached resp was not nil")
		t.Fail()
	}

	cachedResp = dc.Query(*req)
	if !reflect.DeepEqual(resp, cachedResp) {
		fmt.Println("resp and cachedResp not equal, resp: ", resp, " ,cachedResp: ", cachedResp)
		t.Fail()
	} 
}

