package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * the importance is the enginerace3 here. imaging there are 64 threads are calling write or read, if 
 * for writing, we could write 64 each IO, then we can decrease the IO time to 64, but this needs the file
 * is saved together, which might have an impact to multi threading performance. Anyway, lets try it here.
 */
public class EngineRace3 extends AbstractEngine {

	private static final String DATA_FILE = "/mydata/";
	private static String PATH;
	private static String fileName = "mydata";
	private Object[] keys = new Object[THREAD_SIZE];
	private Object[] values = new Object[THREAD_SIZE];
	private AtomicInteger counter = new AtomicInteger(0);
	private CyclicBarrier barrier = new CyclicBarrier(64, new Runnable() {
		
		public void run() {
			if (writer == null) initWriter();
			for (int i = 0; i < THREAD_SIZE; i++) {
				writer.write((byte[]) keys[i]);
				writer.write((byte[]) values[i]);
			}
			// writer.flush(); do we need flush here ?
		} // flush the data.
		
	});
	private static final int THREAD_SIZE = 64;
	private RandomAccessFile writer;

	@Override
	public synchronized void open(String path) throws EngineException {
		if (PATH == null) PATH = path;
	}
	
	private void initWriter() {
		writer = new RandomAccessFile(PATH + DATA_FILE + fileName, "rw");
	}

	public RandomAccessFile getDataFile() throws IOException {
		
	}

	public RandomAccessFile getIndexFile() throws IOException {
		
	}

	@Override
	public void write(byte[] key, byte[] value) throws EngineException {
		try {
			queue.put(new Pair(key, value);
			if (queue.isFull() ) {
				// do flush.
				
			}
		} catch (IOException e) {

		}
	}

	private long keyToLong(byte[] key) {
		long ans = 0;
		for (int i = 0; i < 64; i++) {
			ans |= (((long) (key[i / 8] >>> (i % 8))) << i);
		}
		return ans;
	}

	private HashMap<Long, Long> initIndex() {
		// todo

		return null;
	}

	@Override
	public byte[] read(byte[] key) throws EngineException {
		if (fileMaps == null) initIndex();
		

		return value;
	}
	
	@Override
	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {

	}
	
	@Override
	public void close() {
		try {
			
		} catch (Exception e) {

		}
	}

}
