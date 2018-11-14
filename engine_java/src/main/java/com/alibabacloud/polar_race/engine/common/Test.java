package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.tomzhu.tree.LongLongTreeMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

// import com.madhukaraphatak.sizeof.SizeEstimator;

public class Test {

    private static long keyToLong(byte[] key) {
        long ans = 0;
        for (int i = 0; i < 64; i++) {
            ans |= (((long) (key[i / 8] >>> (i % 8))) << i);
        }
        return ans;
    }

    public static String byteToBitString(byte b) {
        return ""
                + (byte) ((b >> 7) & 0x1) + (byte) ((b >> 6) & 0x1)
                + (byte) ((b >> 5) & 0x1) + (byte) ((b >> 4) & 0x1)
                + (byte) ((b >> 3) & 0x1) + (byte) ((b >> 2) & 0x1)
                + (byte) ((b >> 1) & 0x1) + (byte) ((b >> 0) & 0x1);
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

    public static void main(String args[]) throws EngineException, IOException {

        Random rand = new Random(System.currentTimeMillis());
        RandomAccessFile tempFile = new RandomAccessFile(new File("/tmp/test_perf"), "rw");
        int size = 64000000;
        int unique = 11000000;
        long[] nums = new long[11000000];
        for (int i = 0; i < unique; i++) {
            long temp = rand.nextLong();
            nums[i] = temp;
            tempFile.writeLong(temp);
            tempFile.writeLong(temp);
        }

        for (int i = 0; i < (size - unique) ; i++) {
            long temp = rand.nextLong();
            if (i % 6 == 0) {
                if (temp < 0) temp = -temp;
                long v = nums[(int) (temp % unique) ];
                tempFile.writeLong(v);
                tempFile.writeLong(v);
            }
            tempFile.writeLong(temp);
            tempFile.writeLong(temp);
        }

        tempFile.close();

        System.out.println("writing finished");
        long startTime = System.currentTimeMillis();
        LongLongTreeMap maps = new LongLongTreeMap();
        RandomAccessFile read = new RandomAccessFile(new File("/tmp/test_perf"), "r");
        long keyOffset = 64000000 * 2;
        int pos = 0;
        int len = 1024 * 4 * 1024 * 16;
        byte[] buffer = new byte[len];
        for (int i = 0; i < keyOffset; ) {
            read.readFully(buffer);
            for (int j = 0; j < len; j+= 8) {
                long k = readLong(buffer, j);
                j += 8;
                long info = readLong(buffer, j);
                i += 2;
                if (i >= keyOffset) break;
                maps.insert(k, info);
            }
        }
        System.out.println("spend time : " + ((System.currentTimeMillis() - startTime) / 1000));
    }

    public static long readLong(byte[] key, int from) {
        long ans = 0;
        for (int i = 0; i < 64; i++) {
            ans |= ((long) ((key[ (from + i) / 8] >>> (i % 8)) & 1)) << i;
        }
        return ans;
    }

}
