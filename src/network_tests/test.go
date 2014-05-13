package main
import (
	"fmt"
	"net"
	"namenode_rpc"
)

func main() {
	host_port := "1101"
	host := "127.0.0.1"

	conn, err := net.Dial("tcp", host + ":" + host_port)
	if err != nil {
		fmt.Println("Error occurred in connecting: " + err.Error())
	}

	headerPacket := namenode_rpc.NewHeaderPacket()
	conn.Write(headerPacket.Bytes())

	byteBuffer := make([]byte, 1024)

	bytesRead, err := conn.Read(byteBuffer)
	if bytesRead < 0 || err != nil {
		fmt.Println("Error occurred in reading from the server: ", 
		err.Error())
		return
	}

	fmt.Println("RECVD: ", byteBuffer, "as string: ", string(byteBuffer))
}
