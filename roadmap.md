#ROADMAP

## Issues to solve
1. Implement another system that sits on the namenode and listens around
for calls that change things on the filesystem. Should be independent of the 
caching system entirely, i.e. a separate executable.
2. Make the data caching work on multinode (might have to make a Writeable implementation)
3. **Make PUT calls work**

## New ideas
1. Have to test the system on a popular usecase and see what happens