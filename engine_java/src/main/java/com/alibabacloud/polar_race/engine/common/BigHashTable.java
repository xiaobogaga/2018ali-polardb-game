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

	public static int size = 1024 * 1024 * 32 * 3; //这个究竟可以设置多大.

	// public static int size = 1024 * 50;

	public static long item_size = 8 * 2;

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

	public void addOrUpdate(long key, int offset, int fileNo) {
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

	private void update(int loc, int offset, int fileNo) {
		loc = loc * 2;
		buffer.put(loc + 1, wrap(offset, fileNo));
	}

	private static long wrap(int offset, int fileNo) {
		long ans = 0;
		for (int i = 0; i < 32; i++) {
			ans |= ( ((long) ((offset >>> i) & 1)) << i);
			ans |= ( ((long) ((fileNo >>> i) & 1)) << (32 + i));
		}
		return ans;
	}

	private static int unwrapOffset(long wrapper) {
		int ans = 0;
		for (int i = 0; i < 32; i++) {
			ans |= (((wrapper >>> i) & 1) << i);
		}
		return ans;
	}

	private static int unwrapFileNo(long wrapper) {
		int ans = 0;
		for (int i = 0; i < 32; i++) {
			ans |= (((wrapper >>> (i + 32) ) & 1) << i);
		}
		return ans;
	}


	public void put(int loc, long key, int offset, int fileNo) {
		loc = loc * 2;
		this.buffer.put(loc, key);
		this.buffer.put(loc + 1, wrap(offset, fileNo));
	}


	private boolean isUse(long loc) {
		// System.out.println("Loc : " + loc);
		return unwrapFileNo(this.buffer.get( (int) (loc * 2 + 1) )) != 0;
	}

	private boolean match(int loc, long key) {
		return this.buffer.get(loc * 2) == key;
	}

	public Location tryGet(long key) {
		int loc = hashCode(key) % this.size;
		while (isUse(loc)) {
			if (match(loc, key)) {
				long info = this.buffer.get(loc * 2 + 1);
				Location ans = new Location(unwrapOffset(info),
					unwrapFileNo(info));
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
		
		try {
			this.mmapedFile.close();
			this.channel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
