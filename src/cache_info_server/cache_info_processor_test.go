package cache_info_server

import (
	//go imports
	"testing"

)

func TestProcessorNew(t *testing.T) {
	setup()

	p := NewProcessor(dataCache)
	if p == nil {
		t.Fail()
	}

	if p.Id == 0 {
		t.Fail()
	}
}
