package com.tomzhu.test;

import com.alibabacloud.polar_race.engine.common.EngineRace1;
import com.alibabacloud.polar_race.engine.common.EngineRace2;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class ReaderPro {

    public static EngineRace1 engine;
	// public static EngineRace2 engine;
    public static int THREAD_SIZE = 0;
    public static CountDownLatch countDownLatch;
    public static HashMap<Long, WriterPro.Holder> maps;
    public static Object[] keys;
    public static int READER_TIMES = 0;
    public static int KEY_SIZE = 8;
    public static int VALUE_SIZE = 1024 * 4;
    private static long seed = 9054860549l;
    private static Random random = new Random(seed);
    private static String PATH = "C://tmp/midware/test";

    public static void generateRandomKey(byte[] key) {
        random.nextBytes(key);
    }

    public static class ReaderTask implements Runnable {

        public void testOneRead(byte[] key, boolean empty) {
            try {
                byte[] value = engine.read(key);
                if (!Arrays.equals(value, maps.get(keyToLong(key)).value)) {
                    engine.close();
                    System.out.println("find an unmatching key : " + keyToLong(key));
                    System.exit(1);
                }
            } catch (EngineException e) {
                // if (!empty) System.out.println("error not found key : " + keyToLong(key));
            }
        }

        public void run() {
            byte[] key = new byte[KEY_SIZE];
            for (int i = 0; i < READER_TIMES; i++) {
                long loc = random.nextLong();
                if (loc < 0) loc = -loc;
                if ((i & 1) == 1) testOneRead(parse((Long) keys[(int) (loc % keys.length)], key), false);
                else {
                    generateRandomKey(key);
                    long l = keyToLong(key);
                    if (maps.containsKey(l)) testOneRead(key, false);
                    else testOneRead(key, true);
                }
            }
            countDownLatch.countDown();
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

    public static void startReader(EngineRace1 engine1, HashMap<Long, WriterPro.Holder> data,
                                   int thread_size, int reading_times)
            throws EngineException, InterruptedException {
		THREAD_SIZE = thread_size;
        READER_TIMES = reading_times;
        engine = engine1;
        countDownLatch = new CountDownLatch(THREAD_SIZE);
        engine.open(PATH);
        maps = data;
        keys = maps.keySet().toArray();
        Thread[] threads = new Thread[THREAD_SIZE];
        for (int i = 0; i < THREAD_SIZE; i++) threads[i] = new Thread(new ReaderTask());
        for (int i = 0; i < THREAD_SIZE; i++) threads[i].start();
        countDownLatch.await();
        // engine.close();
    }

    public static void testPerformance(EngineRace1 engine1, HashMap<Long, WriterPro.Holder> data,
                                   int thread_size, int reading_times)
            throws EngineException, InterruptedException {
		THREAD_SIZE = thread_size;
        READER_TIMES = reading_times;
        long startTime = System.currentTimeMillis();
        engine = engine1;
        countDownLatch = new CountDownLatch(THREAD_SIZE);
        engine.open(PATH);
        maps = data;
        keys = maps.keySet().toArray();
        Thread[] threads = new Thread[THREAD_SIZE];
        for (int i = 0; i < THREAD_SIZE; i++) threads[i] = new Thread(new ReaderTask());
        for (int i = 0; i < THREAD_SIZE; i++) threads[i].start();
        countDownLatch.await();
        engine.close();
        System.out.println("finish reading. speed : " +
                ( (READER_TIMES * thread_size) /
                        ((System.currentTimeMillis() - startTime)
                        / 1000.0)) + " ops");
    }

}
