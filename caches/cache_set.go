package caches

//holds a structure w/ all the enabled caches
//this is shared amongst different hdfs_request.Processor
//instances
type CacheSet struct {
	//this should be enabled
	GfiCache *GetFileInfoCache
	GetListingCache *GetListingCache
}

func NewCacheSet() *CacheSet {
	cs := CacheSet{}
	cs.GfiCache = NewGetFileInfoCache(0)
	return &cs
}