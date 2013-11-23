Write a proper data caching layer that fullfills the requirements by the spec.

Steps:

1) Implement the basic set of servers that will correspond to the different DataNodes
	- each server has to listen for requests, forward it to the correct DataNode
	- essentially, write another set of processors that will now use the DataNode protocol
