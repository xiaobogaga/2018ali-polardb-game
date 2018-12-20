the 2018 ali polardb game. see [here](https://tianchi.aliyun.com/programming/introduction.htm?spm=5176.11165261.5678.1.2b7378eeGvMcZj&raceId=231689) for more information.

my base idea for this game is : 

* partition the key for multi threads.

* <b> Writing strategy :</b> key value write seperately. key writing uses mapped file, and value writing use pagecache, both are write-ahead.

* <b> Reading strategy :</b> read key file and then sort the keys. when a search query coming, first find the partition of this key by using partition function, then find the value file and offset by binary search on the keys, after getting offset and value file, just do random read.

* <b> Range query strategy :</b> one thread reads the keys and data file, then the data would be used as a global queue, we guaranteed that a partition can be holder in memory, all readers would share this queue, when all readered finished this partition, then the thread would read next partition. 
