package com.alibabacloud.polar_race.engine.common;

import java.io.File;
import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

public class BigHashTable {
	
	private long capacity;
	
	private String PATH;
	
	private String filePath = "/mmap/";

	private String fileName = "metadata";

	private FileChannel channel;

	private MappedByteBuffer hashTable;

	private LongBuffer buffer;

	private long position = 0l;

	private long size = 1024 * 1024 * 32;

	private long item_size = 8 * 3;

	public BigHashTable(long capacity) {
		this.capacity = capacity;
	}
	
	public void init(String PATH) throws IOException {
		this.PATH = PATH;
		File path = new File(this.PATH + filePath);
		if (!path.exists()) path.mkdirs();
		this.channel = FileChannel.open(Paths.get(this.PATH + filePath + fileName));
		this.hashTable = this.channel.map(FileChannel.MapMode.READ_WRITE, position, size * item_size);
		this.buffer = this.hashTable.asLongBuffer();
	}

	public long hashCode(long key) {
		return key % this.size;
	}

	public void addOrUpdate(long key, long offset) {
		long loc = hashCode(key);
		while (isUse(loc) && !notMatch(loc, key)) {
			loc ++;
		}

	}

	private boolean isUse(long loc) {
		return this.buffer.get((int) loc * 3) == 1;
	}

	private boolean notMatch(long loc, long key) {
		return false;
	}

	public void tryGet(byte[] key) {

	}


}
