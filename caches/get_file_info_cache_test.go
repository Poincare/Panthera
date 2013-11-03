package caches

import (
	"testing"
)

func TestGFICacheConstructor(t *testing.T) {
	cs := 15
	gf := NewGetFileInfoCache(cs)
	if gf.CacheSize != cs {
		t.Fail()
	}
	if len(gf.PastRequests) != cs {
		t.Fail()
	}
}
