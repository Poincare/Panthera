package main 

import (
	"fmt"
	"strings"
	"strconv"
)

func main() {
	fmt.Println("-")

	/*
	packet := "00 00 00 00 01 00 00 00 00 00 2e 6f 72 67 2e" +
	"61 70 61 63 68 65 2e 68 61 64 6f 6f 70 2e 68 64" + 
	"66 73 2e 70 72 6f 74 6f 63 6f 6c 2e 48 64 66 73" +
	"46 69 6c 65 53 74 61 74 75 73 00 2e 6f 72 67 2e" +
	"61 70 61 63 68 65 2e 68 61 64 6f 6f 70 2e 68 64" +
	"66 73 2e 70 72 6f 74 6f 63 6f 6c 2e 48 64 66 73" +
	"46 69 6c 65 53 74 61 74 75 73 00 00 00 00 00 00" +
	"00 00 00 00 00 00 01 00 00 00 00 00 00 00 00 00" +
	"00 00 00 01 41 e6 37 69 5f 00 00 00 00 00 00 00" +
	"00 01 ed 06 68 64 75 73 65 72 0a 73 75 70 65 72" +
	"67 72 6f 75 70" */

	packet := "00 11 51 61 12 4b d1 11 09 4b cb 00 00 00 00 00 " +
"00 04 a4 00 00 00 00 00 00 00 00 00 00 00 00 00 " + 
"0d 70 c6 23 44 46 53 43 6c 69 65 6e 74 5f 4e 4f " +
"4e 4d 41 50 52 45 44 55 43 45 5f 31 35 30 32 37 " +
"32 31 38 32 39 5f 31 00 00 00 00"

	pieces := strings.Split(packet, " ")
	res := make([]byte, len(pieces))

	for i := 0; i<len(pieces); i++ {
		portion, err := strconv.ParseInt(pieces[i], 16, 0)

		if err != nil {
			fmt.Println("Error while decoding string: ", err.Error())
			return
		}
		res[i] = byte(portion)
	}

	fmt.Print("[")
	for i := 0; i<len(res); i++ {
		fmt.Print(res[i], ",")
	}
	fmt.Println("]")
}
