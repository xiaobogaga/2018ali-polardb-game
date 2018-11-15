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

        long startTime = System.currentTimeMillis();
        LongLongTreeMap maps = new LongLongTreeMap();
        int size = 60000000;
        int printLimit = 1000000;
        for (int i = 0; i < size; ) {
            maps.insert(i, i);
            i ++;
            if (i % printLimit == 0) System.out.println("has writing " + printLimit);
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
