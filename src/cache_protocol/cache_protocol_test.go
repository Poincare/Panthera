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

/*
* CachedBlocks
*/

func TestCachedBlocksNew(t *testing.T) {
	c := NewCachedBlocks()
	if c == nil {
		t.Fail()
	}
}

func TestCachedBlocksCreate(t *testing.T) {
	
}

func TestCachedBlocksRead(t *testing.T) {
	numBlocks := uint32(2)
	block := NewBlockDescription()
	block.BlockId = 64

	block2 := NewBlockDescription()
	block.BlockId = 16

	buf := []byte{0, 0, 0, 2}
	byteBuf := bytes.NewBuffer(buf)
	block.Write(byteBuf)
	block2.Write(byteBuf)

	c := NewCachedBlocks()
	c.Read(byteBuf)

	if c.NumBlocks != numBlocks {
		t.Fail()
	}

	if !reflect.DeepEqual(*c.Blocks[0], *block) {
		t.Fail()
	}

	if !reflect.DeepEqual(*c.Blocks[1], *block2) {
		t.Fail()
	}
}

func  TestCachedBlocksWrite(t *testing.T) {
	expected := []byte{0, 0, 0, 2}
	expectedBuf := bytes.NewBuffer(expected)
	block := NewBlockDescription()
	block.BlockId = 64
	block2 := NewBlockDescription()
	block2.BlockId = 16
	block.Write(expectedBuf)
	block2.Write(expectedBuf)

	c := NewCachedBlocks()
	c.NumBlocks = 2
	c.Blocks = make([]*BlockDescription, c.NumBlocks)
	c.Blocks[0] = block
	c.Blocks[1] = block2

	resBuf := new(bytes.Buffer)
	err := c.Write(resBuf)
	if err != nil {
		t.Fail()
	}

	if !reflect.DeepEqual(resBuf.Bytes(), expectedBuf.Bytes()) {
		t.Fail()
	}
}

/*
* Request 
*/

func TestRequestRead(t *testing.T) {
	r := new(Request)
	buf := []byte{0, 0}
	byteBuf := bytes.NewBuffer(buf)

	r.Read(byteBuf)
	if r.RequestType != 0 {
		t.Fail()
	}
}

func TestRequestWrite(t *testing.T) {
	r := NewRequest(REQ_CACHED_BLOCKS)
	expectedBuf := []byte{0, 1}
	resBuf := new(bytes.Buffer)
	err := r.Write(resBuf)
	if err != nil {
		t.Fail()
	}

	if !reflect.DeepEqual(resBuf.Bytes(), expectedBuf) {
		t.Fail()
	}
}