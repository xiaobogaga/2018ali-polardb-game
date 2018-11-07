package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 */
public class EngineRace extends AbstractEngine {
	
	public static boolean printAll = false;

	private final long singleFileSize = 1024 * 4 * 1024;
    private final String VALUE_PATH = "/value/";
    private final String MMAP_PATH = "/mmap/";
	private BigHashTable keyWriteFile;
    private RandomAccessFile valueWriteFile;
    private String PATH;
    private long VALUE_SIZE = 1024 * 4;
    private long counter = 0;
    private int fileNo = -1;
    private long offset = 0;
    private RandomAccessFile[] readFiles;

    public EngineRace() {
        System.err.println("creating an engineRace instance");
    }

    @Override
    public void open(String path) throws EngineException {
        System.err.println("open db");
        if (PATH == null) PATH = path;
        valueWriteFile = null;
        keyWriteFile = null;
        readFiles = null;
        counter = 0;
		fileNo = -1;
		offset = 0;
    }

    private void initFile() {
        if (valueWriteFile == null) {
            try {
                keyWriteFile = new BigHashTable(this.PATH, this.MMAP_PATH);
                this.keyWriteFile.init();
                File valuePath = new File(PATH + VALUE_PATH);
                if (!valuePath.exists()) { valuePath.mkdirs(); }
				this.fileNo = valuePath.listFiles().length;
				if (this.fileNo == 0) this.fileNo ++;
                valueWriteFile = new RandomAccessFile(
                        new File(PATH + VALUE_PATH + String.valueOf(fileNo)), "rw");
                offset = valueWriteFile.length();
				valueWriteFile.seek(offset);
                System.err.println("initFile finished. value files size: " + 
					valuePath.listFiles().length +
                        " , current File : " + valueWriteFile.length());
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

    @Override
    public synchronized void write(byte[] key, byte[] value)
            throws EngineException {
        if (valueWriteFile == null) initFile();
        try {
            if (this.offset >= singleFileSize) {
                openNewFile();
            }
            keyWriteFile.addOrUpdate(keyToLong(key), (int) this.offset, this.fileNo);
            valueWriteFile.write(value);
            this.offset += VALUE_SIZE;
            counter ++;
            if (counter > 1000000) { System.err.println("writing 1000000 data"); counter = 0;}
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void openNewFile() throws IOException {
        // todo
        valueWriteFile.close(); // closing previous file
        this.offset = 0;
        this.fileNo ++;
        String fileName = PATH + VALUE_PATH + String.valueOf(this.fileNo);
        valueWriteFile = new RandomAccessFile(new File(fileName), "rw");
        System.err.printf("create new file %s\n", fileName);
    }

    private void initMaps() {
        keyWriteFile = new BigHashTable(this.PATH, this.MMAP_PATH);
        try {
            keyWriteFile.init();
            File valuePath = new File(PATH + VALUE_PATH);
            int size = valuePath.listFiles().length;
			readFiles = new RandomAccessFile[size];
            for (int i = 1; i <= size; i++) {
                RandomAccessFile file = new RandomAccessFile(new File(PATH +
                        VALUE_PATH + String.valueOf(i)), "r");
                readFiles[i - 1] = file;
            }
            System.err.printf("init readfiles finished. %d files\n", size);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private byte[] getData(Location l) {
        try {
            byte[] ans = new byte[(int) VALUE_SIZE];
            RandomAccessFile file = readFiles[(int) l.fileNo - 1];
            file.seek(l.offset);
            file.readFully(ans);
            return ans;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public synchronized byte[] read(byte[] key) throws EngineException {
        if (keyWriteFile == null) initMaps();
        long l = keyToLong(key);
        Location ans = null;
        if ( (ans = keyWriteFile.tryGet(l)) != null) {
            return getData(ans);
        } else {
			// System.err.println("key not found");
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found");
		}
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor)
            throws EngineException {
        visitAll(visitor);
    }

    private synchronized void visitAll(AbstractVisitor visitor) {
        throw new UnsupportedOperationException("unsupported now");
    }


    @Override
    public void close() {
        try {
            // if (buffer != null) cleanBuffer();
            System.err.println("closing db");
            if (valueWriteFile != null) {
                valueWriteFile.close();
            }
            if (readFiles != null)
                for (RandomAccessFile f : readFiles)
                    f.close();
            if (keyWriteFile != null)
                keyWriteFile.close();
            clean();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void clean() {
        keyWriteFile = null;
        valueWriteFile = null;
        readFiles = null;
    }

}