package main 

import (
	"fmt"
	"strings"
	"strconv"
	"io/ioutil"
)

func loadPacket(filename string) string {
	//get a string representation of the file
	byte_contents, _ := ioutil.ReadFile(filename)

	contents := string(byte_contents)
	fmt.Println("contents: ", contents)

	lines := strings.Split(contents, "\n")
	fmt.Println("lines: ")

	packet := ""
	for i := 0; i < len(lines); i++ {
		if lines[i] != "" {
			fmt.Println(lines[i])
			packet = packet + lines[i] + " "
		}
	}
	return packet
}

func main() {
	fmt.Println("-")
	filename := "packet.convert"

	packet := loadPacket(filename)

	/*
	packet = "00 00 00 75 00 00 00 02 00 11 67 65 74 42 6c 6f " +
"63 6b 4c 6f 63 61 74 69 6f 6e 73 00 00 00 03 00 " +
"10 6a 61 76 61 2e 6c 61 6e 67 2e 53 74 72 69 6e " +
"67 00 2a 2f 75 73 65 72 2f 68 64 75 73 65 72 2f " +
"67 75 74 65 6e 62 65 72 67 2d 6f 75 74 70 75 74 " +
"2f 70 61 72 74 2d 72 2d 30 30 30 30 30 00 04 6c " +
"6f 6e 67 00 00 00 00 00 00 00 00 00 04 6c 6f 6e " +
"67 00 00 00 00 28 00 00 00" */

	pieces := strings.Split(packet, " ")
	res := make([]byte, len(pieces))

	for i := 0; i<len(pieces); i++ {
		if pieces[i] == "" {
			continue
		}
		
		portion, err := strconv.ParseInt(pieces[i], 16, 0)

		if err != nil {
			fmt.Println("Error while decoding string: ", err.Error())
			return
		}
		res[i] = byte(portion)
		fmt.Println("piece: ", pieces[i])
	}

	fmt.Print("[")
	for i := 0; i<len(res); i++ {
		fmt.Print(res[i], ",")
	}
	fmt.Println("]")

	fmt.Println("As string: ", string(res))
}
