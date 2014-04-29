package cache_comm

import (
	//go packages
	"net"
	"fmt"

	//local pacakges
	"scheduler/configuration"
	"writable_processor"
	"cache_protocol"
)

type Client struct {
	//location of the cache that this 
	//client is communicating with
	CacheLoc configuration.CacheLocation

	//connection to the cacheLoc
	Conn *writable_processor.Connection
}

func NewClient(loc configuration.CacheLocation) (*Client, error) {
	c := Client{}
	c.CacheLoc = loc

	//set up the connection to the cacheLoc
	conn, err := net.Dial("tcp", c.CacheLoc.GetString())
	if err != nil {
		return nil, err
	}
	c.Conn = writable_processor.NewConnection(conn)

	return &c, nil
}

func (c *Client) GetCacheDescription() (
	*cache_protocol.CacheDescription, error) {

	conn := c.Conn
	req := cache_protocol.NewRequest(cache_protocol.REQ_CACHE_DESCRIPTION)
	err := req.Write(conn)
	if err != nil {
		return nil, err
	}

	res := cache_protocol.NewCacheDescription()
	err = res.Read(conn)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Client) GetCachedBlocks() (
	*cache_protocol.CachedBlocks, error) {

	conn := c.Conn
	fmt.Println("Writing request...")
	req := cache_protocol.NewRequest(cache_protocol.REQ_CACHED_BLOCKS)
	err := req.Write(conn)
	if err != nil {
		return nil, err
	}
	fmt.Println("Written request.")

	fmt.Println("Reading request...")
	res := cache_protocol.NewCachedBlocks()
	err = res.Read(conn)
	if err != nil {
		return nil, err
	}
	fmt.Println("Read request.")

	return res, nil
}
