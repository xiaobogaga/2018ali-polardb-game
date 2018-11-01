package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class EngineRace extends AbstractEngine {

    private final String KEY_PATH = "/key/";
	private final String VALUE_PATH = "/value/";
    private final String MMAP_PATH = "/mmap/";
   // private MappedByteBuffer buffer;
    private RandomAccessFile keyWriteFile;
	private RandomAccessFile valueWriteFile;
    private String PATH;
    private final long WRITE_MAPED_SIZE = 1024 * 1024 * 1024;
    private HashMap<Long, Long> maps;
    private long KEY_SIZE = 8;
    private long VALUE_SIZE = 1024 * 4;
    private volatile boolean finished = false;
    private long waiting_read_time = 100;
    private long writing_size = 0l;
    // private ThreadLocal<Holder> ansThreadLocal;
    private RandomAccessFile[] readFiles;
    private HashMap<Long, Integer> keyFiles;

    class Holder {
        byte[] ans;
        public Holder(byte[] ans) { this.ans = ans; }
    }

    public EngineRace() {
        System.out.println("creating an engineRace instance");
    }

    @Override
    public void open(String path) throws EngineException {
        System.out.println("open db");
        if (PATH == null) PATH = path;
        // ansThreadLocal = new ThreadLocal<Holder>();
        maps = null;
        keyWriteFile = null;
		valueWriteFile = null;
        keyFiles = null;
        readFiles = null;
    }

    private void initFile() {
        if (keyWriteFile == null) {
            try {
                File keyPath = new File(PATH + KEY_PATH);
				File valuePath = new File(PATH + VALUE_PATH);
                if (!keyPath.exists()) { keyPath.mkdirs(); valuePath.mkdirs(); }
				System.out.println("key files : " + keyPath.listFiles().length);
				System.out.println("value files : " + valuePath.listFiles().length);
                String keyFileName = String.valueOf(keyPath.listFiles().length);
				String valueFileName = String.valueOf(valuePath.listFiles().length);
                keyWriteFile = new RandomAccessFile(
					new File(PATH + KEY_PATH + keyFileName), "rw");
				valueWriteFile = new RandomAccessFile(
					new File(PATH + VALUE_PATH + valueFileName), "rw");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static long keyToLong(byte[] key) {
        long ans = 0;
        for (int i = 0; i < 64; i++) {
            ans |= (((long) (key[i / 8] >>> (i % 8))) << i);
        }
        return ans;
    }

    private static byte[] parse(long key, byte[] ans) {
        for (int i = 0; i < 64; i++) {
            ans[i / 8] |= (((key >>> i) & 1) << (i % 8));
        }
        return ans;
    }

    @Override
    public synchronized void write(byte[] key, byte[] value) throws EngineException {
        if (keyWriteFile == null) initFile();
        try {
            keyWriteFile.write(key);
            valueWriteFile.write(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initMaps() {
        if (maps == null) {
            maps = new HashMap<Long, Long>();
            keyFiles = new HashMap<Long, Integer>();
            long totalSize = 0;
            byte[] key = new byte[(int) KEY_SIZE];
            File[] fs = new File(PATH + KEY_PATH).listFiles();
            readFiles = new RandomAccessFile[fs.length];
            for (int i = 0; i < fs.length; i ++) {
                File keyTemp = new File(PATH + KEY_PATH + String.valueOf(i));
				File valueTemp = new File(PATH + VALUE_PATH + String.valueOf(i));
				long pointer = 0l;
                try {
                    RandomAccessFile file = new RandomAccessFile(keyTemp, "r");
                    while (file.length() > file.getFilePointer()) {
                        file.readFully(key);
                        totalSize++;
                        long k = keyToLong(key);
						keyFiles.put(k, i);
                        maps.put(k, pointer);
						pointer += VALUE_SIZE;
                    }
					file.close();
                    readFiles[i] = new RandomAccessFile(valueTemp, "r");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // finished = true;
            System.out.println("Finished. we have " + maps.size() +
                    " different keys and totalSize : " + totalSize + 
					" under " + readFiles.length + " files");
        }
    }

    private byte[] getData(long l) {
        try {
            byte[] ans = new byte[(int) VALUE_SIZE];
           // System.out.println("get key : " + l + " , p : " + maps.get(l));
            RandomAccessFile file = readFiles[keyFiles.get(l)];
            file.seek(maps.get(l));
            file.readFully(ans);
            return ans;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


/*
    private byte[] getAns() {
        if (ansThreadLocal.get() == null) {
            ansThreadLocal.set(new Holder(new byte[(int) VALUE_SIZE]));
        }
        return ansThreadLocal.get().ans;
    }
*/

    @Override
    public synchronized byte[] read(byte[] key) throws EngineException {
        if (maps == null) initMaps();
//        while (!finished) { // waiting for finish.
//            try {
//                Thread.sleep(waiting_read_time);
//            } catch (InterruptedException e) {
//            }
//        }
        long l = keyToLong(key);
        if (maps.containsKey(l))
            return getData(l);
        else throw new EngineException(RetCodeEnum.NOT_FOUND, "not found");
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        visitAll(visitor);
    }

    private synchronized void visitAll(AbstractVisitor visitor) {
        throw new UnsupportedOperationException("unsupported now");
    }


    @Override
    public void close() {
        try {
            // if (buffer != null) cleanBuffer();
            System.out.println("closing db");
            if (keyWriteFile != null) {
				keyWriteFile.close();
				valueWriteFile.close();
			}
            if (readFiles != null) {
                for (RandomAccessFile f : readFiles)
                    f.close();
            }
			clean();
        } catch (IOException e) {
			e.printStackTrace();
        }
    }
	
	public void clean() {
		// ansThreadLocal = null;
		maps = null;
        keyWriteFile = null;
		valueWriteFile = null;
        keyFiles = null;
        readFiles = null;
	}

}