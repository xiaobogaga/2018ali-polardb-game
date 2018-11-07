package com.alibabacloud.polar_race.engine.common;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.BitSet;
import java.io.RandomAccessFile;

public class BigHashTable {
	
	private String PATH;
	
	private String filePath;

	private String fileName = "metadata";
	
	private RandomAccessFile mmapedFile;

	private FileChannel channel;

	private MappedByteBuffer hashTable;

	private LongBuffer buffer;

	private int size = 1024 * 1024 * 32;

	private long item_size = 8 * 4;

	public BigHashTable(String PATH, String filePath) {
		this.PATH = PATH;
		this.filePath = filePath;
	}
	
	public void init() throws IOException {
		System.err.println("init mapped");
		File path = new File(this.PATH + filePath);
		if (!path.exists()) path.mkdirs();
		mmapedFile = new RandomAccessFile(new File(this.PATH + filePath + fileName), "rw");
		this.channel = mmapedFile.getChannel();
		this.hashTable = this.channel.map(FileChannel.MapMode.READ_WRITE, 
			0l, size * item_size);
		this.buffer = this.hashTable.asLongBuffer();
	}

	public int hashCode(long key) {
		int ans = (int)(key ^ (key >>> 32));
		if (ans < 0) return -ans;
		return ans;
	}

	public void addOrUpdate(long key, long offset, long fileNo) {
		int loc = hashCode(key) % this.size;
		while (isUse(loc)) {
			if (match(loc, key)) {
				if (EngineRace.printAll) 
					System.err.printf(
						"update %d with offset %d and fileNo %d, loc : %d\n", 
							key, offset, fileNo, loc);;
				update(loc, offset, fileNo);
				return ;
			}
			if (EngineRace.printAll) System.err.printf("Writing. conflict for %d try a new place\n", 
				key);
			loc = (loc + 1) % this.size;
		}
		if (EngineRace.printAll) 
			System.err.printf("insert %d with offset %d and fileNo %d, loc : %d\n", 
				key, offset, fileNo, loc);;
		put(loc, key, offset, fileNo);
	}

	private void update(int loc, long offset, long fileNo) {
		loc = loc * 4;
		buffer.put(loc + 1, offset);
		buffer.put(loc + 2, fileNo);
	}

	public void put(int loc, long key, long offset, long fileNo) {
		loc = loc * 4;
		this.buffer.put(loc, key);
		this.buffer.put(loc + 1, offset);
		this.buffer.put(loc + 2, fileNo);
		this.buffer.put(loc + 3, 1l);
	}


	private boolean isUse(long loc) {
		// System.out.println("Loc : " + loc);
		return this.buffer.get( (int) (loc * 4 + 3) ) == 1l;
	}

	private boolean match(int loc, long key) {
		return this.buffer.get(loc * 4) == key;
	}

	public Location tryGet(long key) {
		int loc = hashCode(key) % this.size;
		while (isUse(loc)) {
			if (match(loc, key)) {
				Location ans = new Location(this.buffer.get(loc * 4 + 1), 
					this.buffer.get(loc * 4 + 2));
				if (EngineRace.printAll)
					System.err.printf("found %d at %d with offset %d and fileNo  %d\n", key,
						loc, ans.offset, ans.fileNo);
				return ans;
			}
			if (EngineRace.printAll) 
				System.err.printf("reading. conflict for %d try a new place\n", key);
			loc = (loc + 1) % this.size;
		}
		if (EngineRace.printAll)
			System.err.printf("not found %d\n", key);
		return null;
	}

	public void close() {
		System.err.println("closing bighashtable");
		try {
			this.channel.close();
			this.mmapedFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		AccessController.doPrivileged(new PrivilegedAction() {

			public Object run() {
				try {
					Method getCleanerMethod = hashTable.getClass().getMethod("cleaner",new Class[0]);
					getCleanerMethod.setAccessible(true);
					sun.misc.Cleaner cleaner =(sun.misc.Cleaner)getCleanerMethod.invoke(hashTable,new Object[0]);
					cleaner.clean();
				} catch(Exception e) {
					e.printStackTrace();
				}
				return null;
			}
			
		});

	}

}
