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