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

    private final String P = "/mydata/";
    private final String MMAP_PATH = "/mmap/";
   // private MappedByteBuffer buffer;
    private RandomAccessFile writeFile;
    private String PATH;
    private final long WRITE_MAPED_SIZE = 1024 * 1024 * 1024;
    private HashMap<Long, Long> maps;
    private long KEY_SIZE = 8;
    private long VALUE_SIZE = 1024 * 4;
    private volatile boolean finished = false;
    private long waiting_read_time = 100;
    private long writing_size = 0l;
    private ThreadLocal<Holder> ansThreadLocal;
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
        ansThreadLocal = new ThreadLocal<Holder>();
        maps = null;
        writeFile = null;
        keyFiles = null;
        readFiles = null;
    }

    private void initFile() {
        if (writeFile == null) {
            try {
                File path = new File(PATH + P);
                if (!path.exists()) path.mkdirs();
                String fileName = String.valueOf(path.listFiles().length);
                writeFile = new RandomAccessFile(new File(PATH + P + fileName), "rw");
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
        if (writeFile == null) initFile();
        try {
            writeFile.write(key);
            writeFile.write(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // writing_size += KEY_SIZE + VALUE_SIZE;
    }

    private void initMaps() {
        if (maps == null) {
            maps = new HashMap<Long, Long>();
            keyFiles = new HashMap<Long, Integer>();
            long totalSize = 0;
            byte[] key = new byte[(int) KEY_SIZE];
            File[] fs = new File(PATH + P).listFiles();
            readFiles = new RandomAccessFile[fs.length];
            for (int i = 0; i < fs.length; i ++) {
                File temp = new File(PATH + P + String.valueOf(i));
                try {
                    RandomAccessFile file = new RandomAccessFile(temp, "r");
                    while (file.length() > file.getFilePointer()) {
                        file.readFully(key);
                        totalSize++;
                        long k = keyToLong(key);
                        long p = file.getFilePointer();
                        file.seek(p + VALUE_SIZE);
                        maps.put(k, p);
                    }
                    readFiles[i] = file;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            finished = true;
            System.out.println("Finished. we have " + maps.size() +
                    " different keys and totalSize : " + totalSize);
            if (maps.size() > 5000000) {
                System.out.println("Finished. we have " + maps.size() +
                        " different keys and totalSize : " + totalSize);
                System.exit(1);
            }
        }
    }

    private byte[] getData(long l) {
        try {
            byte[] ans = getAns();
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



    private byte[] getAns() {
        if (ansThreadLocal.get() == null) {
            ansThreadLocal.set(new Holder(new byte[(int) VALUE_SIZE]));
        }
        return ansThreadLocal.get().ans;
    }

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
            if (writeFile != null) writeFile.close();
            if (readFiles != null) {
                for (RandomAccessFile f : readFiles)
                    f.close();
            }
        } catch (IOException e) {
        }
    }

}