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
    private final String fileName = "data";
   // private MappedByteBuffer buffer;
    private RandomAccessFile file;
    private String PATH;
    private final long WRITE_MAPED_SIZE = 1024 * 1024 * 1024;
    private HashMap<Long, Long> maps;
    private long KEY_SIZE = 8;
    private long VALUE_SIZE = 1024 * 4;
    private volatile boolean finished = false;
    private long waiting_read_time = 100;
    private long writing_size = 0l;

    @Override
    public void open(String path) throws EngineException {
        if (PATH == null) PATH = path;
    }

    private void initFile() {
        if (file == null) {
            try {
                File path = new File(PATH + P);
                if (!path.exists()) path.mkdirs();
                file = new RandomAccessFile(new File(PATH + P + fileName), "rw");
//                buffer = file.getChannel().map(FileChannel.MapMode.READ_WRITE, file.length(), file.length());
//                System.out.println("file length : " + file.length() +
//                        " , position : " + buffer.position()
//                    + " , limit : " + buffer.limit());
//                buffer.flip();
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
        if (file == null) initFile();
        try {
            file.write(key);
            file.write(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // writing_size += KEY_SIZE + VALUE_SIZE;
    }

    private void initMaps() {
        if (maps == null) {
            maps = new HashMap<Long, Long>();
            try {
                file = new RandomAccessFile(new File(PATH + P + fileName), "r");
                byte[] key = new byte[(int) KEY_SIZE];
                // byte[] value = new byte[VALUE_SIZE];
                long totalSize = 0;
                while (file.length() > file.getFilePointer()) {
                    file.readFully(key);
                    totalSize++;
                    long k = keyToLong(key);
                    long p = file.getFilePointer();
                    file.seek(p + VALUE_SIZE);
                    maps.put(k, p);
                }

                finished = true;
                if (maps.size() > 5000000) {
                    System.out.println("Finished. we have " + maps.size() +
                            " different keys and totalSize : " + totalSize);
                    System.exit(1);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private byte[] getData(long l) {
        try {
            byte[] ans = new byte[(int) VALUE_SIZE];
           // System.out.println("get key : " + l + " , p : " + maps.get(l));
            file.seek(maps.get(l));
            file.readFully(ans);
            return ans;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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
        if (maps == null) {
            initMaps();
        }
        try {
            byte[] key = new byte[(int) KEY_SIZE];
            byte[] value = new byte[(int) VALUE_SIZE];
            for (Map.Entry<Long, Long> entry : maps.entrySet()) {
                long offset = entry.getValue();
                file.seek(offset);
                file.readFully(value);
                visitor.visit(parse(entry.getKey(), key), value);
            }
        } catch (IOException e) {
        }
    }


    @Override
    public void close() {
        try {
            // if (buffer != null) cleanBuffer();
            if (file != null) file.close();
        } catch (IOException e) {
        }
    }

//    private void cleanBuffer() {
//        AccessController.doPrivileged(new PrivilegedAction<Object>() {
//            public Object run() {
//                try {
//                    // System.out.println(buffer.getClass().getName());
//                    Method getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
//                    getCleanerMethod.setAccessible(true);
//                    sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(buffer, new Object[0]);
//                    cleaner.clean();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                return null;
//            }
//        });
//    }

}