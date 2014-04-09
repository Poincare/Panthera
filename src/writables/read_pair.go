package writables

/**
* An OP_READ_BLOCK request and its 
* corresponding set of responses
*/
type ReadPair struct {
	Request *ReadBlockHeader
	ResponseSet *BlockResponseSet
}

func NewReadPair(request *ReadBlockHeader) *ReadPair {
	r := ReadPair{Request: request}
	r.ResponseSet = NewBlockResponseSet()
	return &r
}

//conv method
func (r *ReadPair) AddBlockPacket(q *BlockPacket) {
	r.ResponseSet.AddBlockPacket(q)
}

//convin. method
func (r *ReadPair) AddChunk(q *BlockPacket) {
	r.AddBlockPacket(q)
}