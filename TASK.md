
The problem with the GetListing caching is that the response from the server
can be split into several different packets. 

Solution:
	* Introduce a state machine
		* Once we get a request from the client, until a new request comes in,
		all packets from the server are responding to that specific request
		* So, once there is a new request, we take a bytes.Buffer then load it
		into a GenericResponse instance and cache it.
		* (as opposed to caching it immediately after receiving)
		* Read the packetNumber from ONLY the first packet (which means there has 
		bool switch that determines if we've already read one packet)
