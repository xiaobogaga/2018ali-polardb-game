package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * a engine race implementation. this implementation doesn't holds a index for data, instead, it just
 * use a data file, and it would init index when reading. this strategy would decrease IO times, which
 * I think is more efficient than using index.
 *
 * Note that this class assume that there is no same key. for example, its update use no same key.
 * and its range query would iterate all keys, if there are same key, it would iterator twice.
 */
public class EngineRace extends AbstractEngine {

    private final String DATA_FILE = "/mydata/";
    private String PATH;
    private HashMap<Long, Holder> fileOffsetMaps;
    private HashMap<Long, Integer> keyVersionMaps;
    private RandomAccessFile[] readFiles;
    private ThreadLocal<RandomAccessFile> datafileThreadLocal = new
            ThreadLocal<RandomAccessFile>();
    private ConcurrentLinkedQueue<RandomAccessFile> writeFiles =
            new ConcurrentLinkedQueue<RandomAccessFile>();
    private long VALUE_SIZE = 1024 * 4;
    private long KEY_SIZE = 8;
    private AtomicInteger fileCounter;

    class Holder {
        long offset;
        int file;
        public Holder(long offset, int file) { this.offset = offset; this.file = file; }
    }

    @Override
    public void open(String path) throws EngineException {
        if (PATH == null) PATH = path;
        // init the key-version maps from this offset.
        if (keyVersionMaps == null) {
            initKeyVersionMaps();
        }
    }

    private void initKeyVersionMaps() {
        File f = new File(PATH + DATA_FILE);
        if (!f.exists()) f.mkdirs();
        fileOffsetMaps = new HashMap<Long, Holder>();
        keyVersionMaps = new HashMap<Long, Integer>();
        File[] fs = f.listFiles();
        readFiles = new RandomAccessFile[fs.length];
        fileCounter = new AtomicInteger(fs.length);
        int i = 0;
        int dataCounter = 0;
        for (File temp : fs) {
            RandomAccessFile reader = null;
            try {
                reader = new RandomAccessFile(temp, "r");
                byte[] key = new byte[(int) KEY_SIZE];
                while (reader.length() > reader.getFilePointer()) {
                    dataCounter ++;
                    reader.readFully(key);
                    long l = keyToLong(key);
                    int version = reader.readInt();
                    long p = reader.getFilePointer();
                    if (keyVersionMaps.get(l) == null || keyVersionMaps.get(l) < version) {
                        keyVersionMaps.put(l, version);
                        fileOffsetMaps.put(l, new Holder(p, i));
                    }
                    reader.seek(p + VALUE_SIZE);
                }
                readFiles[i++] = reader;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("finish reading " + PATH + ". we have write " +
                keyVersionMaps.size() + " different keys under " +
                fs.length + " different files " + " and total size : " + dataCounter);
    }

    public RandomAccessFile getDataFile() throws IOException {
        if (datafileThreadLocal.get() == null) {
            RandomAccessFile file = new RandomAccessFile(new File(PATH + DATA_FILE +
                    fileCounter.getAndIncrement()), "rw");
            datafileThreadLocal.set(file);
            writeFiles.add(file);
        }
        return datafileThreadLocal.get();
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        try {
            long l = keyToLong(key);
            int version = updateKeyVersionMaps(l);
            RandomAccessFile dataFile = getDataFile();
            dataFile.write(key);
            dataFile.writeInt(version);
            dataFile.write(value);
        } catch (IOException e) {

        }
    }

    private synchronized int updateKeyVersionMaps(long l) { // can we avoid using lock here.
        Integer version = keyVersionMaps.get(l);
        if (version != null) {
            version ++;
            keyVersionMaps.put(l, version);
        } else {
            version = 0;
            keyVersionMaps.put(l, version);
        }
        return version;
    }

    private long keyToLong(byte[] key) {
        long ans = 0;
        for (int i = 0; i < 64; i++) {
            ans |= (((long) (key[i / 8] >>> (i % 8))) << i);
        }
        return ans;
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        try {
            long k = keyToLong(key);
            Holder h = fileOffsetMaps.get(k);
            if (h == null) throw new EngineException(RetCodeEnum.NOT_FOUND, "not found" );
            long loc = h.offset;
            int f = h.file;
            RandomAccessFile file = readFiles[f];
            byte[] ans = new byte[(int) VALUE_SIZE]; // how about using a constant.
            synchronized (file) {
                file.seek(loc);
                file.readFully(ans);
            }
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
    public synchronized void range(byte[] lower, byte[] upper, AbstractVisitor visitor)
            throws EngineException {
        try {
            File p = new File(PATH + DATA_FILE);
            int i = 0;
            byte[] key = new byte[(int) KEY_SIZE];
            byte[] value = new byte[(int) VALUE_SIZE];
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
                    file.close();
				}
            }
            if (readFiles != null) {
                for (RandomAccessFile file : readFiles)
                    file.close();
            }
        } catch (Exception e) {

        }
    }

}
