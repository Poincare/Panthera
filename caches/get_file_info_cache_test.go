package caches

import (
	"testing"
	"namenode_rpc"
	"reflect"
	"fmt"
)

func TestGFICacheConstructor(t *testing.T) {
	cs := 15
	gf := NewGetFileInfoCache(cs)
	if gf.Cache.CacheSize != cs {
		t.Fail()
	}
}

func TestGFICacheQuery(t *testing.T) {
	cs := 3
	gf := NewGetFileInfoCache(cs)
	req := namenode_rpc.NewRequestPacket()
	resp := namenode_rpc.NewGetFileInfoResponse()

	gf.Cache.Add(req, resp)

	if !reflect.DeepEqual(resp, gf.Cache.Query(req)) {
		fmt.Println("unequal: ", *resp, gf.Cache.Query(req))
		t.Fail()
	}
}
