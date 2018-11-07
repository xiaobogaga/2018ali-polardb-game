package com.tomzhu.test;

import com.alibabacloud.polar_race.engine.common.EngineRace1;
import com.alibabacloud.polar_race.engine.common.EngineRace2;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Map;

/**
 * WriterPro.
 */
public class WriterPro {

    public static class Holder {
        public byte[] value;
        public Holder(byte[] value) { this.value = value; }
        @Override
        public boolean equals(Object o) {
            if (o == null) return false;
            if (o instanceof Holder) {
                Holder h = (Holder) o;
                return Arrays.equals(value, h.value);
            } else return false;
        }
    }

    private static int WRITER_TIMES = 0;
    private static int KEY_SIZE = 8;
    private static int VALUE_SIZE = 1024 * 4;
    private static int thread_size = 0;
    private static long seed = 7409172834l;
    private static Random random = new Random(seed);
    // public static EngineRace2 engine;
	public static EngineRace engine;
    private static String PATH = "C://tmp/midware/test";
    public static HashMap<Long, Holder> maps;
    public static volatile boolean shutdown = false;
    public static CountDownLatch countDownLatch;
	public static HashMap<Long, Holder> maps2;

    public static void generateRandomKey(byte[] key) {
        random.nextBytes(key);
    }

    public static void generateRandomValue(byte[] value) {
        random.nextBytes(value);
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
	
	public static String byteToBitString(byte b) {
        return ""
                + (byte) ((b >> 7) & 0x1) + (byte) ((b >> 6) & 0x1)
                + (byte) ((b >> 5) & 0x1) + (byte) ((b >> 4) & 0x1)
                + (byte) ((b >> 3) & 0x1) + (byte) ((b >> 2) & 0x1)
                + (byte) ((b >> 1) & 0x1) + (byte) ((b >> 0) & 0x1);
    }
	
	public static String printKey(byte[] key) {
		StringBuilder build = new StringBuilder();
		for (int i = 0; i < KEY_SIZE; i++) {
			build.append(byteToBitString(key[i]));
		}
		return build.toString();
	}

    public static synchronized void writeAValue(byte[] key, byte[] value) {
        generateRandomKey(key);
        generateRandomValue(value);
        try {
			long l = keyToLong(key);
            engine.write(key, value);
			byte[] key2 = new byte[KEY_SIZE];
			parse(l, key2);
            maps.put(l, new Holder(value));
			// if (EngineRace.printAll) System.err.printf("add a key %d with bits1: %s, bits2: %s\n",
			   // l, printKey(key), printKey(key2));;
            if (EngineRace.printAll) System.err.printf("add a key %d\n", l);
        } catch (EngineException e) {
            e.printStackTrace();
        }
    }

    public static class WriterTask implements Runnable {

        /**
         * writing data.
         */
        public void run() {
            for (int i = 0; i < WRITER_TIMES && !shutdown; i++) {
                byte[] key = new byte[KEY_SIZE];
                byte[] value = new byte[VALUE_SIZE];
                writeAValue(key, value);
            }
            if (countDownLatch != null) countDownLatch.countDown();
        }

    }

    public static void startWriter(int thread_s, int write_time) throws InterruptedException,
            BrokenBarrierException, EngineException {
		countDownLatch = new CountDownLatch(thread_s);
        maps = new HashMap<Long, Holder>();
        engine = new EngineRace();
		// engine = new EngineRace2();
        engine.open(PATH);
        thread_size = thread_s;
        WRITER_TIMES = write_time;
        Thread[] threads = new Thread[thread_size];
        for (int i = 0; i < thread_size; i++) threads[i] = new Thread(new WriterTask());
        for (int i = 0; i < thread_size; i++) threads[i].start();
    }

    public static void testPerformance(int thread_s, int write_time)
            throws InterruptedException, EngineException {
        long startTime = System.currentTimeMillis();
		maps2 = maps;
        maps = new HashMap<Long, Holder>();
        engine = new EngineRace();
		// engine = new EngineRace2();
        engine.open(PATH);
        thread_size = thread_s;
        WRITER_TIMES = write_time;
        Thread[] threads = new Thread[thread_size];
        countDownLatch = new CountDownLatch(thread_size);
        for (int i = 0; i < thread_size; i++) threads[i] = new Thread(new WriterTask());
        for (int i = 0; i < thread_size; i++) threads[i].start();
        countDownLatch.await();
        engine.close();
        System.err.println("finish writing. speed : " +
                ( (WRITER_TIMES * thread_size) /
                        ((System.currentTimeMillis() - startTime)
                        / 1000.0)) + " ops");
		mergeTwoMaps();
    }
	
	public static void mergeTwoMaps() {
		for (Map.Entry<Long, Holder> entry : maps2.entrySet()) {
			if (!maps.containsKey(entry.getKey())) maps.put(entry.getKey(), entry.getValue());
		}
	}



}
