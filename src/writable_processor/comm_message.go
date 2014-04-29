package writable_processor

/*
* Communication messages between goroutines in 
* writable_processsor
*/

type CommMessage struct {
	SocketClose bool
}

func NewCommMessage() *CommMessage {
	c := CommMessage{
		SocketClose: false}
	return &c
}

func NewSocketCloseMsg() *CommMessage {
	c := CommMessage{
		SocketClose: true}
	return &c
}