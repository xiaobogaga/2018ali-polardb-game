package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * a engine race implementation. this implementation holds a index for all data which could be used
 * to index the location of a key, for example the offset of a specific file for the key.
 */
public class EngineRace extends AbstractEngine {

	private static final String DATA_FILE = "/mydata/";
	private static String PATH;
	private static HashMap<Long, Long> offsetMaps;
	private static HashMap<Long, Integer> fileMaps;
	private static ThreadLocal<RandomAccessFile> datafileThreadLocal = new
			ThreadLocal<RandomAccessFile>();
	private static ThreadLocal<RandomAccessFile> indexFileThreadLocal = new
			ThreadLocal<RandomAccessFile>();
	private static ThreadLocal<String> fileNameThreadlocal = new ThreadLocal<String>();
	private ConcurrentLinkedQueue<RandomAccessFile> datafiles =
			new ConcurrentLinkedQueue<RandomAccessFile>();
	private ConcurrentLinkedQueue<RandomAccessFile> indexfiles =
			new ConcurrentLinkedQueue<RandomAccessFile>();


	@Override
	public synchronized void open(String path) throws EngineException {
		if (PATH == null) PATH = path;
	}

	public RandomAccessFile getDataFile() throws IOException {
		if (fileNameThreadlocal.get() == null)
			fileNameThreadlocal.set(String.valueOf(System.currentTimeMillis()));
		if (datafileThreadLocal.get() == null) {
			RandomAccessFile file = new RandomAccessFile(new File(PATH + DATA_FILE +
					fileNameThreadlocal.get()), "rw");
			datafileThreadLocal.set(file);
			datafiles.add(file);
		}
		return datafileThreadLocal.get();
	}

	public RandomAccessFile getIndexFile() throws IOException {
		if (indexFileThreadLocal.get() == null) {
			RandomAccessFile file = new
					RandomAccessFile(PATH + DATA_FILE + fileNameThreadlocal.get() + "-index", "rw");
			indexFileThreadLocal.set(file);
			indexfiles.add(file);
		}
		return indexFileThreadLocal.get();
	}

	@Override
	public void write(byte[] key, byte[] value) throws EngineException {
		try {
			RandomAccessFile dataFile = getDataFile();
			long loc = dataFile.getFilePointer();
			dataFile.write(key);
			dataFile.write(value);
			// writing twice.
			RandomAccessFile indexFile = getIndexFile();
			indexFile.write(key);
			indexFile.writeLong(loc); // the pointer of data.
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
		byte[] value = null;

		return value;
	}
	
	@Override
	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {

	}
	
	@Override
	public void close() {
		try {
			if (datafiles != null) {
				for (RandomAccessFile file : datafiles)
					file.close();
			}
			if (indexfiles != null) {
				for (RandomAccessFile file : indexfiles)
					file.close();
			}
		} catch (Exception e) {

		}
	}

}
