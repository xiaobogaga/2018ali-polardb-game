package com.alibabacloud.polar_race.engine.common;

import java.io.File;
import java.io.IOException;
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

	private long position = 0l;

	private long size = 1024 * 1024 * 32;

	public BigHashTable(long capacity) {
		this.capacity = capacity;
	}
	
	public void init(String PATH) throws IOException {
		this.PATH = PATH;
		File path = new File(this.PATH + filePath);
		if (!path.exists()) path.mkdirs();
		this.channel = FileChannel.open(Paths.get(this.PATH + filePath + fileName));
		this.hashTable = this.channel.map(FileChannel.MapMode.READ_WRITE, position, size);
	}

	public void tryAdd(byte[] key) {

	}

	public void tryGet(byte[] key) {

	}


}
