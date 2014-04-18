/*
* Describes a protocol between the Panthera job scheduler and
* the Panthera data cache. 
* Communicates the contents of the cache.
*/

package cache_protocol

import (
	//go imports

	//local imports
	"caches"
	"writables"
)

/* CacheDescription.ReplaceAlgorithm */
const (
	//least recently used cache algorithm
	LRU uint16 = iota

	//empty the cache on fill
	EMPTY
)

/*
* CacheDescription
*/ 

type CacheDescription struct {
	//cache replacement algorithm (the default
	//is LRU)
	ReplaceAlgorithm uint16

	//represents the number of chunks that 
	//this cache can hold at a time
	CacheSize uint32

	//current size of the cache
	CurrSize uint32
}

func NewCacheDescription() *CacheDescription {
	cacheDescription := CacheDescription{ReplaceAlgorithm: LRU}
	return &cacheDescription
}

//create a cache description from an instance of writables.WritableDataCache
func CreateCacheDescription(cache *caches.WritableDataCache) *CacheDescription {
	c := CacheDescription{ReplaceAlgorithm: LRU,
	CacheSize: uint32(cache.CacheSize),
	CurrSize: uint32(cache.CurrSize())}

	return &c
}

func (c *CacheDescription) Read(reader writables.Reader) error {
	var err error
	c.ReplaceAlgorithm, err = writables.ReadShortInt(reader)
	if err != nil {
		return err
	}

	c.CacheSize, err = writables.ReadInt(reader)
	if err != nil {
		return err
	}

	c.CurrSize, err = writables.ReadInt(reader)
	if err != nil {
		return err
	}

	return nil
}

func (c *CacheDescription) Write(writer writables.Writer) error {
	var err error
	err = writables.WriteShortInt(c.ReplaceAlgorithm, writer)
	if err != nil {
		return err
	}

	err = writables.WriteInt(c.CacheSize, writer)
	if err != nil {
		return err
	}

	err = writables.WriteInt(c.CurrSize, writer)
	if err != nil {
		return err
	}

	return nil
}

/**
* BlockDescription
*/

type BlockDescription struct {
	//(see writables.Block)
	BlockId uint64
}

func NewBlockDescription() *BlockDescription {
	b := BlockDescription{}
	return &b
}

func (b *BlockDescription) Read(reader writables.Reader) error {
	var err error
	b.BlockId, err = writables.ReadLongInt(reader)
	return err
}

func (b *BlockDescription) Write(writer writables.Writer) error {
	var err error
	err = writables.WriteLongInt(b.BlockId, writer)
	return err
}

/**
* CachedBlocks
*/

type CachedBlocks struct {
	NumBlocks uint32
	Blocks []*BlockDescription
}

func NewCachedBlocks() *CachedBlocks {
	c := CachedBlocks{}
	return &c
}

func (c *CachedBlocks) Read(reader writables.Reader) error {
	var err error
	c.NumBlocks, err = writables.ReadInt(reader)
	if err != nil {
		return err
	}

	c.Blocks = make([]*BlockDescription, c.NumBlocks)
	for i := 0; i < int(c.NumBlocks); i++ {
		c.Blocks[i] = NewBlockDescription()
		err = c.Blocks[i].Read(reader)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *CachedBlocks) Write(writer writables.Writer) error {
	var err error
	err = writables.WriteInt(c.NumBlocks, writer)
	if err != nil {
		return err
	}

	for i := 0; i < int(c.NumBlocks); i++ {
		err = c.Blocks[i].Write(writer)
		if err != nil {
			return err
		}
	}

	return nil
}

/**
** Request types
*/

const (
	REQ_CACHE_DESCRIPTION = uint16(iota)
	REQ_CACHED_BLOCKS
)

/** 
* Request
*/

type Request struct {
	RequestType uint16
}

func NewRequest(requestType uint16) *Request {
	r := Request{RequestType: requestType}
	return &r
}

func (r *Request) Read(reader writables.Reader) error {
	var err error
	r.RequestType, err = writables.ReadShortInt(reader)
	if err != nil {
		return err
	}

	return nil
}

func (r *Request) Write(writer writables.Writer) error {
	var err error
	err = writables.WriteShortInt(r.RequestType, writer)
	if err != nil {
		return err
	}

	return nil
}