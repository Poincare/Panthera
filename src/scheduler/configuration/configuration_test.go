package configuration

import (
	//go packages
	"testing"

	//local pacakges
)

func TestConfigurationRead(t *testing.T) {
	c := NewConfiguration()
	buf := []byte("{\"CacheInfoHostname\" : \"127.0.0.1\",\"CacheInfoPort\" : \"1337\", \"JobInfoDir\":\"job_info_conf\"}")
	c.Read(buf)

	if c.CacheInfoHostname != "127.0.0.1" {
		t.Fail()
	}

	if c.CacheInfoPort != "1337" {
		t.Fail()
	}
}