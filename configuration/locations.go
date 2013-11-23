package configuration

/* This file describes all the configuration
data structures used. For example, the configuration
for which ports correspond to which DataNode addresses
is handled here */
import (
	"fmt"
	"strconv"
)

type DataNodeLocation struct {
	Ip string
	Port string
}

func NewDataNodeLocation(ip string, port string) *DataNodeLocation {
	dnl := DataNodeLocation{Ip: ip,
		Port: port}

	return &dnl	
}

type Port string

type DataNodeMap map[Port]*DataNodeLocation

//makes a map out of the given port offeset and locations
func MakeDataNodeMap(dnls []*DataNodeLocation, portOffset int) DataNodeMap {
	res := make(DataNodeMap)
	for i := 0; i < len(dnls); i++ {
		fmt.Println("For port: ", strconv.Itoa(portOffset+i), ", dnls[i]: ", dnls[i])
		res[Port(strconv.Itoa(portOffset+i))] = dnls[i]
	}

	return res
}