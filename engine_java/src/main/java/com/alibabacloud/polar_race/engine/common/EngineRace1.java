package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.tomzhu.tree.LongLongTreeMap;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;

/**
 * a trivial solution, it turns out that it would consume too much memory.
 */
public class EngineRace1 extends AbstractEngine {

    public static boolean printAll = false;

    private final int singleFileSize = 1024 * 1024 * 256;
    private final String KEY_PATH = "/key/";
    private final String VALUE_PATH = "/value/";
    private final String META_PATH = "/meta/";
    private final String keyFileName = "key";
    private final String metaFileName = "meta";
    private final int metaFileSize = 1024 * 4;
    private final int keyFileSize = 1024 * 1024 * 1024;
    private final long VALUE_SIZE = 1024 * 4;
    private int fileNo = 0;
    private int keyOffset = 0;
    private int valueOffset = 0;
    private String PATH;
    private LongLongTreeMap maps;
    private RandomAccessFile keyWriteFile;
    private MappedByteBuffer keyMappedBuffer;
    private LongBuffer keyLongBuffer;
    private RandomAccessFile valueWriteFile;
    private RandomAccessFile metaFile;
    private MappedByteBuffer metaMappedBuffer;
    private LongBuffer metaLongBuffer;
    private RandomAccessFile[] readFiles;
    private HashMap<Long, Integer> keyFiles;

    public EngineRace1() {
        System.out.println("creating an engineRace instance");
    }

    @Override
    public void open(String path) throws EngineException {
        System.err.println("open db");
        if (PATH == null) PATH = path;
        maps = null;
        keyWriteFile = null;
        metaFile = null;
        metaMappedBuffer = null;
        metaLongBuffer = null;
        valueWriteFile = null;
        keyFiles = null;
        readFiles = null;
        keyMappedBuffer = null;
        keyLongBuffer = null;
        this.fileNo = 0;
    }

    private void initFile() {
        if (keyWriteFile == null) {
            try {
                File keyPath = new File(PATH + KEY_PATH);
                File valuePath = new File(PATH + VALUE_PATH);
                File metaPath = new File(PATH + META_PATH);
                if (!keyPath.exists()) { keyPath.mkdirs(); valuePath.mkdirs(); metaPath.mkdirs(); }
                this.metaFile = new RandomAccessFile(PATH + META_PATH + this.metaFileName, "rw");
                this.metaMappedBuffer = this.metaFile.getChannel().map(FileChannel.MapMode.READ_WRITE,
                        0, metaFileSize);
                this.metaLongBuffer = this.metaMappedBuffer.asLongBuffer();
                this.keyOffset = (int) this.metaLongBuffer.get(0);
                this.fileNo = valuePath.listFiles().length;
                if (this.fileNo == 0) this.fileNo ++;
                keyWriteFile = new RandomAccessFile(new File(PATH + KEY_PATH + this.keyFileName), "rw");
                this.keyMappedBuffer = keyWriteFile.getChannel().
                        map(FileChannel.MapMode.READ_WRITE, 0, this.keyFileSize);
                this.keyLongBuffer = this.keyMappedBuffer.asLongBuffer();
                this.keyLongBuffer.position(this.keyOffset);
                valueWriteFile = new RandomAccessFile(
                        new File(PATH + VALUE_PATH + this.fileNo), "rw");
                this.valueOffset = (int) valueWriteFile.length();
                valueWriteFile.seek(this.valueOffset);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static long keyToLong(byte[] key) {
        long ans = 0;
        for (int i = 0; i < 64; i++) {
            ans |= ((long) ((key[i / 8] >>> (i % 8)) & 1)) << i;
        }
        return ans;
    }

    private static byte[] parse(long key, byte[] ans) {
        for (int i = 0; i < 64; i++) {
            ans[i / 8] |= (((key >>> i) & 1) << (i % 8));
        }
        return ans;
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

    @Override
    public synchronized void write(byte[] key, byte[] value) throws EngineException {
        if (keyWriteFile == null) initFile();
        try {
            if (this.valueOffset >= singleFileSize) {
                openNewFile();
            }
            long k = keyToLong(key);
            keyLongBuffer.put(k);
            long info = wrap(this.valueOffset, this.fileNo);
            keyLongBuffer.put(info);
            this.keyOffset += 2;
            // System.out.println("puting key : " + k + " , info : " + info);
            metaLongBuffer.put(0, this.keyOffset);
            valueWriteFile.write(value);
            this.valueOffset += VALUE_SIZE;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void openNewFile() throws IOException {
        valueWriteFile.close(); // closing previous file
        this.valueOffset = 0;
        this.fileNo ++;
        String fileName = PATH + VALUE_PATH + String.valueOf(this.fileNo);
        valueWriteFile = new RandomAccessFile(new File(fileName), "rw");
    }

    private void initMaps() {
        if (maps == null) {
            try {
                this.maps = new LongLongTreeMap();
                File valuePath = new File(PATH + VALUE_PATH);
                this.metaFile = new RandomAccessFile(PATH + META_PATH + this.metaFileName, "r");
                this.metaMappedBuffer = this.metaFile.getChannel().map(FileChannel.MapMode.READ_ONLY,
                        0, metaFileSize);
                this.metaLongBuffer = this.metaMappedBuffer.asLongBuffer();
                this.keyOffset = (int) this.metaLongBuffer.get(0);
                this.fileNo = valuePath.listFiles().length;
                keyWriteFile = new RandomAccessFile(new File(PATH + KEY_PATH + this.keyFileName), "r");
                this.keyMappedBuffer = keyWriteFile.getChannel().map(FileChannel.MapMode.READ_ONLY,
                        0, this.keyFileSize);
                this.keyLongBuffer = this.keyMappedBuffer.asLongBuffer();
                keyLongBuffer.position(0);
                for (int i = 0; i < keyOffset;) {
                    long k = this.keyLongBuffer.get();
                    long info = this.keyLongBuffer.get();
                    this.maps.insert(k, info);
                    i += 2;
                }
                AccessController.doPrivileged(new PrivilegedAction() {

                    public Object run() {
                        try {
                            Method getCleanerMethod = keyMappedBuffer.getClass().getMethod("cleaner",new Class[0]);
                            getCleanerMethod.setAccessible(true);
                            sun.misc.Cleaner cleaner =(sun.misc.Cleaner)getCleanerMethod.invoke(keyMappedBuffer,new Object[0]);
                            cleaner.clean();

                            getCleanerMethod = metaMappedBuffer.getClass().getMethod("cleaner",new Class[0]);
                            getCleanerMethod.setAccessible(true);
                            cleaner =(sun.misc.Cleaner)getCleanerMethod.invoke(metaMappedBuffer,new Object[0]);
                            cleaner.clean();

                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    }

                });
                keyWriteFile.close();
                metaFile.close();
                keyWriteFile = null;
                metaFile = null;
                this.readFiles = new RandomAccessFile[this.fileNo];
                StringBuilder build = new StringBuilder();
                build.append(PATH).append(VALUE_PATH);
                for (int i = 0; i < this.fileNo; i++) {
                    int l = build.length();
                    this.readFiles[i] = new RandomAccessFile(build.append(i + 1).toString(), "r");
                    build.delete(l, build.length());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public long readLong(byte[] key, int from) {
        long ans = 0;
        for (int i = 0; i < 64; i++) {
            ans |= ((long) ((key[from + i / 8] >>> (i % 8)) & 1)) << i;
        }
        return ans;
    }

    public void readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        buffer.clear();
        int ret = 1;
        while (ret >= 0 && buffer.hasRemaining()) {
            ret = channel.read(buffer);
        }
    }

    public void initMaps2() {
        if (maps == null) {
            try {
                this.maps = new LongLongTreeMap();
                File valuePath = new File(PATH + VALUE_PATH);
                this.metaFile = new RandomAccessFile(PATH + META_PATH + this.metaFileName, "r");
                this.metaMappedBuffer = this.metaFile.getChannel().map(FileChannel.MapMode.READ_ONLY,
                        0, metaFileSize);
                this.metaLongBuffer = this.metaMappedBuffer.asLongBuffer();
                this.keyOffset = (int) this.metaLongBuffer.get(0);
                this.fileNo = valuePath.listFiles().length;
                keyWriteFile = new RandomAccessFile(new File(PATH + KEY_PATH + this.keyFileName), "r");
                FileChannel channel = keyWriteFile.getChannel();
                int len = 1024 * 1024 * 16;
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(len);
                for (int i = 0; i < keyOffset; ) {
                    readFully(channel, byteBuffer);
                    byteBuffer.rewind();
                    for (int j = 0; j < len; j+= 8) {
                        long k = byteBuffer.getLong();
                        j += 8;
                        long info = byteBuffer.getLong();
                        i += 2;
                        maps.insert(k, info);
                        if (i >= keyOffset) break;
                    }
                }
                ((DirectBuffer) byteBuffer).cleaner().clean();
                AccessController.doPrivileged(new PrivilegedAction() {

                    public Object run() {
                        try {
                            Method getCleanerMethod = metaMappedBuffer.getClass().getMethod("cleaner",new Class[0]);
                            getCleanerMethod.setAccessible(true);
                            sun.misc.Cleaner cleaner =(sun.misc.Cleaner)getCleanerMethod.invoke(metaMappedBuffer,new Object[0]);
                            cleaner.clean();
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    }

                });
                keyWriteFile.close();
                metaFile.close();
                keyWriteFile = null;
                metaFile = null;
                this.readFiles = new RandomAccessFile[this.fileNo];
                StringBuilder build = new StringBuilder();
                build.append(PATH).append(VALUE_PATH);
                for (int i = 0; i < this.fileNo; i++) {
                    int l = build.length();
                    this.readFiles[i] = new RandomAccessFile(build.append(i + 1).toString(), "r");
                    build.delete(l, build.length());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private byte[] getData(long l) {
        try {
            byte[] ans = new byte[(int) VALUE_SIZE];
            RandomAccessFile file = readFiles[unwrapFileNo(l) - 1];
            file.seek(unwrapOffset(l));
            file.readFully(ans);
            return ans;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public synchronized byte[] read(byte[] key) throws EngineException {
        if (maps == null) initMaps2();
        long l = keyToLong(key);
        long ans = maps.get(l);
        // System.out.println("getting key : " + l + " , info : " + ans);
        if (ans != -1l)
            return getData(ans);
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
            System.err.println("closing db");
            if (metaFile != null) {
                AccessController.doPrivileged(new PrivilegedAction() {

                    public Object run() {
                        try {
                            Method getCleanerMethod = keyMappedBuffer.getClass().getMethod("cleaner",new Class[0]);
                            getCleanerMethod.setAccessible(true);
                            sun.misc.Cleaner cleaner =(sun.misc.Cleaner)getCleanerMethod.invoke(keyMappedBuffer,new Object[0]);
                            cleaner.clean();

                            getCleanerMethod = metaMappedBuffer.getClass().getMethod("cleaner",new Class[0]);
                            getCleanerMethod.setAccessible(true);
                            cleaner =(sun.misc.Cleaner)getCleanerMethod.invoke(metaMappedBuffer,new Object[0]);
                            cleaner.clean();

                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    }

                });
                keyWriteFile.close();
                metaFile.close();
            }

            if (valueWriteFile != null) valueWriteFile.close();

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
        maps = null;
        keyWriteFile = null;
        metaFile = null;
        metaMappedBuffer = null;
        metaLongBuffer = null;
        valueWriteFile = null;
        keyFiles = null;
        readFiles = null;
        keyMappedBuffer = null;
        keyLongBuffer = null;
    }

}