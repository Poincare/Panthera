package hdfs_requests


//describes an event that a processor
//can send to another
type ProcessorEvent interface {
}

//event that is sent when a new file
//is sent in to HDFS (this can be 
//hooked into by the GFI cache, for 
//example)
//handled by processor.EventLoop() and fired by processor.HandleHDFS
//and processor.HandleConnection
type ObjectCreatedEvent struct {
	Filepath string
}

func NewObjectCreatedEvent(filepath string) *ObjectCreatedEvent {
	oce := ObjectCreatedEvent{
		Filepath: filepath}
	return &oce
}
