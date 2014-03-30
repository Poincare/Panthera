package writable_processor

import (
	"net"
)

//This structure is meant to add
//ReadByte() and WriteByte() methods to net.Conn so that
//it satisfies the writables.Reader and writables.Writer interfaces
type Connection struct {
	Conn net.Conn
}

func NewConnection(conn net.Conn) *Connection {
	c := Connection{Conn: conn}
	return &c
}

func (c *Connection) Read(p []byte) (n int, err error) {
	return c.Conn.Read(p)
}

func (c *Connection) ReadByte() (byte, error) {
	buf := make([]byte, 1)

	_, err := c.Read(buf)
	return buf[0], err
}

func (c *Connection) Write(p []byte) (n int, err error) {
	return c.Conn.Write(p)
}

func (c *Connection) WriteByte(p byte) (err error) {
	buf := []byte{p}

	_, err = c.Conn.Write(buf)
	return err
}