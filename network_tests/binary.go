package main

import (
	"fmt"
	"bytes"
	"encoding/binary"
)

func main() {
	//should equal 15
	buf := []byte{0, 0, 0, 4, 1}
	byte_buffer := bytes.NewBuffer(buf)

	var leng uint32

	binary.Read(byte_buffer, binary.BigEndian, &leng)

	res, err := binary.ReadVarint(byte_buffer)
	if err != nil {
		fmt.Println("Error occurred: ", err.Error())
		return
	}
	
	fmt.Println("res: ", res)
	if(res == 150) {
		fmt.Println("Success.")
	}
}