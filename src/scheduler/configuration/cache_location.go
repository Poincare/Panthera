package configuration

type CacheLocation struct {
	Hostname string
	Port string
}

func NewCacheLocation(hostname string, port string) *CacheLocation {
	c := CacheLocation{Hostname: hostname, Port: port}
	return &c
}

func (c *CacheLocation) GetString() string {
	return c.Hostname + ":" + c.Port
}