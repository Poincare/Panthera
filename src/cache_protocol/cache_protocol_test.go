package cache_protocol

import (
	//go imports
	"testing"
	"bytes"
	"reflect"

	//local imports
	"caches"
)

func TestConstants(t *testing.T) {
	if LRU != uint16(0) {
		t.Fail()
	}

	if EMPTY != uint16(1) {
		t.Fail()
	}
}

func TestCreateCacheDescription(t *testing.T) {
	dataCache := caches.NewWritableDataCache(15)
	descr := CreateCacheDescription(dataCache)

	if descr.CacheSize != uint32(15) {
		t.Fail()
	}

	if descr.ReplaceAlgorithm != LRU {
		t.Fail()
	}

	if descr.CurrSize != uint32(0) {
		t.Fail()
	}
}

func TestCacheDescriptionRead(t *testing.T) {
	buf := []byte{0, 0, 0, 0, 0, 2, 0, 0, 0, 1}
	byteBuf := bytes.NewBuffer(buf)

	c := NewCacheDescription()
	c.Read(byteBuf)

	if c.ReplaceAlgorithm != 0 {
		t.Fail()
	}

	if c.CacheSize != 2 {
		t.Fail()
	}

	if c.CurrSize != 1 {
		t.Fail()
	}
}

func TestCacheDescriptionWrite(t *testing.T) {
	byteBuf := new(bytes.Buffer)
	expectedBuf := []byte{0, 0, 0, 0, 0, 2, 0, 0, 0, 1}

	c := NewCacheDescription()
	c.ReplaceAlgorithm = LRU
	c.CacheSize = 2
	c.CurrSize = 1
	c.Write(byteBuf)

	if !reflect.DeepEqual(byteBuf.Bytes(), expectedBuf) {
		t.Fail()
	}
}

func TestNewBlockDescription(t *testing.T) {
	descr := NewBlockDescription()

	if descr == nil {
		t.Fail()
	}
}

func TestBlockDescriptionRead(t *testing.T) {
	descr := NewBlockDescription()
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 16}
	byteBuf := bytes.NewBuffer(buf)

	descr.Read(byteBuf)

	if descr.BlockId != 16 {
		t.Fail()
	}
}

func TestBlockDescriptionWrite(t *testing.T) {
	descr := NewBlockDescription()
	descr.BlockId = 14
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 14}
	byteBuf := new(bytes.Buffer)

	descr.Write(byteBuf)

	if !reflect.DeepEqual(byteBuf.Bytes(), buf) {
		t.Fail()
	}
}


