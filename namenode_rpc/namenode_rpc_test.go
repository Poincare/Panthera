package namenode_rpc
import (
	"testing"
	"fmt"
	"bytes"
)

func TestNewHeaderPacket(t *testing.T) {
	h := NewHeaderPacket()

	//assert conditions on the different
	//defaults
	if h.version != 4 {
		t.FailNow();
		fmt.Println("h.version: ", h.version)
	}
	if string(h.header) != "hrpc" {
		t.FailNow();
		fmt.Println("h.header: ", string(h.header))
	}
	if h.auth_method != 80 {
		t.FailNow();
		fmt.Println("h.auth_method: ", string(h.auth_method))
	}
	if h.serialization_type != 0 {
		t.FailNow();
		fmt.Println("h.serialization_type: ", string(h.serialization_type))
	}
	fmt.Println("pass")
}

//Unit test for Headers()
//Needs some testing itself with netcat, not
//sure if it is a valid test yet
func TestHeaders(t *testing.T) {
  header := NewHeaderPacket()

  byte_packet := header.Bytes()
  if byte_packet == nil {
  	t.FailNow();
  }


  //first four bytes are "hrpc"
  //fifth byte: version
  //sixth byte: auth method
  //seventh byte: serialization method
  expected := []byte{104, 114, 112, 99, 4, 80, 0}

  if bytes.Compare(byte_packet, expected) != 0 {
		fmt.Println("byte_packet: ", byte_packet, "expected: ", expected)
  	t.FailNow();
  }

  fmt.Println("byte packet: ", byte_packet)
}

/* ResponsePacket tests*/

//Unit test for ResponsePacket constructor
func TestNewResponsePacket(t *testing.T) {
	message_packet := NewResponsePacket()
	if message_packet == nil {
		t.FailNow();
	}
}

/* RequestPacket tests */

//test RequestPacket constructor
func TestNewRequestPacket(t *testing.T) {
	req_packet := NewRequestPacket()
	if req_packet == nil {
		t.FailNow()
	}

	if req_packet.LengthBoth != 0 {
		t.Fail()
	}

	if req_packet.HeaderLength != 0 {
		t.Fail()
	}

	if req_packet.RequestLength != 0 {
		t.Fail()
	}
}

//test RequestPacket.Load()
func TestRequestPacketLoad(t *testing.T) {
	req_packet := NewRequestPacket()

	//the byte buffer should have:
	//total length: something, no idea
	//header length: 1
	//header serialized: "!"
	//request length: 1
	//request serialized: "!"
	buf := []byte{0, 0, 0, 4, 1, 33, 1, 33}
	err = req_packet.Load(buf)
	fmt.Println("req packet header length:", req_packet.HeaderLength)
}


