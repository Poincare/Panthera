package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
)

func main() {
	var input uint64 = 17547979945164609488 
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, &input)
	fmt.Println("Byte array: ", buf.Bytes())

	fmt.Print("{")
	for i := 0; i<len(buf.Bytes()); i++ {
		formattedInt := strconv.FormatInt(int64(buf.Bytes()[i]), 16)
		fmt.Print(formattedInt, ",")
	}
	fmt.Println("}\n")
}
