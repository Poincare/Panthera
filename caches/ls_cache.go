package caches

import (
	"fmt"
	"namenode_rpc"
)

/*
* This file implements a simple "ls" cache.
* That means that we will cache "getListing"
* calls that are supposed to go to the NameNode
* and just return them to the client
* as long as we know that no file changes have been made
* through the NameNode or the DataNode.
*/
