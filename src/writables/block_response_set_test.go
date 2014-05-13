package writables

import (
	"testing"
)

var header *BlockResponseHeader

func setupBRH() {
	header = NewBlockResponseHeader()
}

func TestNewBlockResponseSet(t *testing.T) {
	setupBRH()

	b := NewBlockResponseSet(header)
	if b == nil {
		t.Fail()
	}

	if b.Header == nil {
		t.Fail()
	}
}

//test the BlockResponseSet.AddChunk() and 
//BlockResponseSet.Size() methods.
func TestBRSAddChunkAndSize(t *testing.T) {
	setupBRH()

	b := NewBlockResponseSet(header)
	chunk := NewBlockPacket(header)
	b.AddChunk(chunk)

	if len(b.Chunks) != 1 {
		t.Fail()
	}

	if b.Chunks[0] != chunk {
		t.Fail()
	}

	b.AddChunk(chunk)
	if len(b.Chunks) != 2 {
		t.Fail()
	}

	if b.Chunks[1] != chunk {
		t.Fail()
	}

	if b.Size() != len(b.Chunks) {
		t.Fail()
	}
}