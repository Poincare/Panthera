package main
import (
	"fmt"
)

func forever() {
	for {

	}
}

func dosomething(c chan int) {
	for i := 1; i < 10; i++ {
		fmt.Println(i)
	}
	c <- 1
}

func main() {
	c := make(chan int)
	go forever()
	go dosomething(c)
	<- c
}
