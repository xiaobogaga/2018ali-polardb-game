package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * a engine race implementation. this implementation doesn't holds a index for data, instead, it just
 * use a data file, and it would init index when reading. this strategy would decrease IO times, which
 * I think is more efficient than using index.
 *
 * Note that this class assume that there is no same key. for example, its update use no same key.
 * and its range query would iterate all keys, if there are same key, it would iterator twice.
 */
public class EngineRace2 extends AbstractEngine {

    private static final String DATA_FILE = "/mydata/";
    private static String PATH;
    private static HashMap<Long, Long> offsetMaps;
    private static HashMap<Long, Integer> fileMaps;
    private static HashMap<Integer, RandomAccessFile> readFiles;
    private static ThreadLocal<RandomAccessFile> datafileThreadLocal = new
            ThreadLocal<RandomAccessFile>();
    private ConcurrentLinkedQueue<RandomAccessFile> writeFiles =
            new ConcurrentLinkedQueue<RandomAccessFile>();
    private static int VALUE_SIZE =- 1024 * 4;
    private static int KEY_SIZE = 8;


    @Override
    public synchronized void open(String path) throws EngineException {
        if (PATH == null) PATH = path;
    }

    public RandomAccessFile getDataFile() throws IOException {
        if (datafileThreadLocal.get() == null) {
            RandomAccessFile file = new RandomAccessFile(new File(PATH + DATA_FILE +
                    System.currentTimeMillis()), "rw");
            datafileThreadLocal.set(file);
            writeFiles.add(file);
        }
        return datafileThreadLocal.get();
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        try {
            RandomAccessFile dataFile = getDataFile();
            dataFile.write(key);
            dataFile.write(value);
			// dataFile.flush(); ? need flush ?
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

    private synchronized void initIndex() throws IOException {
        // todo
        if (offsetMaps == null) {
            offsetMaps = new HashMap<Long, Long>();
            fileMaps = new HashMap<Long, Integer>();
            readFiles = new HashMap<Integer, RandomAccessFile>();
            File p = new File(PATH + DATA_FILE);
            int i = 0;
            byte[] key = new byte[KEY_SIZE];
            for (File file : p.listFiles()) {
                RandomAccessFile reader = new RandomAccessFile(file, "r"); // how about direct buffer.
                readFiles.put(i++, reader);
                // how about using buffered reader here.
                // BufferedInputStream read = new BufferedInputStream(new FileInputStream(file));
                initMapsFromReader(reader, i, key);
            }
        }
    }

    private void initMapsFromReader(RandomAccessFile reader, int i, byte[] key) throws IOException{
        int t = VALUE_SIZE;
        while (t == VALUE_SIZE) {
            long loc = reader.getFilePointer();
            reader.readFully(key);
            long k = keyToLong(key);
            fileMaps.put(k, i);
            offsetMaps.put(k, loc);
            t = reader.skipBytes(VALUE_SIZE); // value Size.
        }
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        try {
            if (offsetMaps == null) {
                initIndex();
            }
            long k = keyToLong(key);
            Long loc = offsetMaps.get(k);
            if (loc == null) throw new EngineException(RetCodeEnum.NOT_FOUND, "not found" );
            RandomAccessFile file = readFiles.get(fileMaps.get(k));
            file.seek(loc + KEY_SIZE);
            byte[] ans = new byte[VALUE_SIZE]; // how about using a constant.
            file.readFully(ans);
            return ans;
        } catch (IOException e) {

        }
        return null;
    }

    /**
     * todo : optimization.
     *
     * @param lower start key
     * @param upper end key
     * @param visitor is check key-value pair,you just call visitor.visit(String key, String value)
     *                function in your own engine.
     * @throws EngineException
     */
    @Override
    public synchronized void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        try {
            File p = new File(PATH + DATA_FILE);
            int i = 0;
            byte[] key = new byte[KEY_SIZE];
            byte[] value = new byte[VALUE_SIZE];
            for (File file : p.listFiles()) {
                RandomAccessFile reader = new RandomAccessFile(file, "r"); // how about direct buffer.
                // readFiles.put(i++, reader);
                // how about using buffered reader here.
                // BufferedInputStream read = new BufferedInputStream(new FileInputStream(file));
                while (true) {
                    long loc = reader.getFilePointer();
                    reader.readFully(key);
                    reader.readFully(value);
                    visitor.visit(key, value);
                }
            }
        } catch (IOException e) {}
    }

    @Override
    public void close() {
        try {
            if (writeFiles != null) {
                for (RandomAccessFile file : writeFiles) {
					file.flush(); // do flush here.
                    file.close();
				}
            }
            if (readFiles != null) {
                for (RandomAccessFile file : readFiles.values())
                    file.close();
            }
        } catch (Exception e) {

        }
    }

}
