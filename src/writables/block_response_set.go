package writables

/**
** The set of responses received when
** an OP_READ_BLOCK request is received
**/
type BlockResponseSet struct {
	//one header per set of responses
	Header *BlockResponseHeader

	//multiple chunks per block
	Chunks []*BlockPacket
}

func NewBlockResponseSet(header *BlockResponseHeader) *BlockResponseSet {
	b := BlockResponseSet{Header: header}
	b.Chunks = make([]*BlockPacket, 0)
	return &b
}

func (b *BlockResponseSet) AddBlockPacket(q *BlockPacket) {
	b.Chunks = append(b.Chunks, q)
}

func (b *BlockResponseSet) AddChunk(q *BlockPacket) {
	b.AddBlockPacket(q)
}

func (b *BlockResponseSet) Size() int {
	return len(b.Chunks)
}

