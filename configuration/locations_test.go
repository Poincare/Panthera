package configuration

import (
	"testing"
	"reflect"
	"fmt"
)

func TestDataNodeLocationConstructor (t *testing.T) {
	dnl := NewDataNodeLocation("127.0.0.1", "1101")
	if dnl == nil {
		t.Fail()
	}

	if dnl.Ip != "127.0.0.1" {
		t.Fail()
	}

	if dnl.Port != "1101" {
		t.Fail()
	}
} 

func TestDataNodeMapConstructor (t *testing.T) {
	dnl := NewDataNodeLocation("127.0.0.1", "1337")
	dnLocations := make([]*DataNodeLocation, 0)
	dnLocations = append(dnLocations, dnl)

	dnLocationMap := MakeDataNodeMap(dnLocations, 2000)

	if !reflect.DeepEqual(dnLocationMap[Port("2000")], dnLocations[0]) {
		fmt.Println("Failed, not equal: ", dnLocationMap["2000"], dnLocations[0])
		t.Fail()
	}

	if !reflect.DeepEqual(dnLocationMap[Port("2000")], dnl) {
		fmt.Println("Failed second, not equal:", dnLocationMap["2000"], dnl)
		t.Fail()
	}

	dnLocationMap[Port("2000")] = dnl
	fmt.Println("Did it work: ", dnLocationMap[Port("2000")])
}