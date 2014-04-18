package cache_info_server

import (
	//go packages
	"math/rand"
	"fmt"
	"net"

	//local packages
	"caches"
	"cache_protocol"
	"writable_processor" //used for writable_processor.Connection
)

type Processor struct {

	//notice this id is different from the CacheInfoServer id;
	//each CacheInfoServer can have multiple processors, all with
	//different ids
	Id uint64

	//instance of data cache (used to answer requests)
	DataCache *caches.WritableDataCache

	//socket to to the client
	Client *writable_processor.Connection
}

func NewProcessor(dataCache *caches.WritableDataCache, 
	client net.Conn) *Processor {

	p := Processor{Id: uint64(rand.Int63n(999999999)),
		DataCache: dataCache}
	c := writable_processor.NewConnection(client)
	p.Client = c

	return &p
}

func (p *Processor) HandleCacheDescription(r *cache_protocol.Request) error {
	descr := cache_protocol.CreateCacheDescription(p.DataCache)
	return descr.Write(p.Client)
}

//Looks at the request that is read and responds to it
func (p *Processor) HandleRequest(r *cache_protocol.Request) error {
	switch(r.RequestType) {
	case cache_protocol.REQ_CACHE_DESCRIPTION:
		return p.HandleCacheDescription(r)
	}
	return nil
}

//handles the client. Reads cache_protocol.Request instances
//and responds accordingly
func (p *Processor) HandleClient() error {
	for {
		r := new(cache_protocol.Request)

		//read in the request from the client
		err := r.Read(p.Client)
		if err != nil {
			return err
		}

		fmt.Println("Request received: ", *r)
		err = p.HandleRequest(r)
		if err != nil {
			fmt.Println("Could not process request: ", err)
		}
	}
}
