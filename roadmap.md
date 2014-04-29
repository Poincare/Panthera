#Panthera Roadmap

##General notes
1. Fix up the research paper ASAP
2. HDFS is built around "write once, read many times"
  * works really well for a cache use case
  * mention it on the board somewhere
3. Low Latency Access is currently a major issue within HDFS
  * solving it with caching seems like a viable solution given #2
4. Note that Hadoop is not really suitable for lots of small files
  * that doesn't really matter since blocks are what we are caching, not the files
5. HDFS has a 64 MB (i.e. very large) block
  * RAM memory is getting larger and larger
6. Map tasks usually operate on one block at a time
  * This means that we can schedule the specific tasks while considering
  what's in a cache. This could work for **any kind of Hadoop application!**
  (not only those that have multiple *operations* one after another)


##Issues that need solving
1. "Whisper" protocol between NameNode and the cache
2. Need the "put" method to start working
3. Need to get it running on multiple (esp. with the DataNode registration thing)
4. Make sure the way we are measuring latency is valid

##Future developments
1. Test Panthera with the new YARN-based Hadoop
