package com.tomzhu.test;

import com.alibabacloud.polar_race.engine.common.EngineRace2;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class RangerPro {

    public static EngineRace2 engine = new EngineRace2();
    public static int THREAD_SIZE = 64;
    public static CountDownLatch countDownLatch = new CountDownLatch(THREAD_SIZE);
    public static HashMap<Long, WriterPro.Holder> maps;
    public static int RANGE_TIMES = 2;
    public static int KEY_SIZE = 8;
    public static int VALUE_SIZE = 1024 * 4;
    private static String PATH = "/tmp/midware/test";
    public static class RangerReadTask implements Runnable {

        public void testOneRangeRead() {
            long k = 0l;
            try {
                for (Map.Entry<Long, WriterPro.Holder> entry : maps.entrySet()) {
                    k = entry.getKey();
                    byte[] key = new byte[KEY_SIZE];
                    byte[] value = engine.read(parse(k, key));
                    if (!Arrays.equals(value, maps.get(k).value)) {
                        System.out.println("find a unmatching key : " + keyToLong(key));
                        engine.close();
                        System.exit(1);
                    }
                }
            } catch (EngineException e) {
                System.out.println("not found key : " + k);
            }
        }

        public void run() {
            for (int i = 0; i < RANGE_TIMES; i ++) {
                testOneRangeRead();
            }
            countDownLatch.countDown();
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

    public static void testRangeRead(HashMap<Long, WriterPro.Holder> data)
            throws EngineException, InterruptedException {
        engine.open(PATH);
        maps = data;
        Thread[] threads = new Thread[THREAD_SIZE];
        for (int i = 0; i < THREAD_SIZE; i++) threads[i] = new Thread(new RangerReadTask());
        for (int i = 0; i < THREAD_SIZE; i++) threads[i].start();
        countDownLatch.await();
        engine.close();
    }

}
