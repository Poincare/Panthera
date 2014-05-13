package caches

import (
	"testing"
)

func TestGetListingCacheConstructor (t *testing.T) {
	glc := NewGetListingCache(15)
	if glc == nil {
		t.Fail()
	}

	if glc.Cache == nil {
		t.Fail()
	}

	if !glc.IsEnabled() {
		t.Fail()
	}
}

func TestGetListingCacheDisable (t *testing.T) {
	glc := NewGetListingCache(15)
	glc.Disable()

	if glc.IsEnabled() {
		t.Fail()
	}
}

