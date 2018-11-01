package com.tomzhu.test;

import com.alibabacloud.polar_race.engine.common.EngineRace1;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TempTest {

    private static long seed = 97897l;
    private static Random random = new Random(seed);

    public static void generateRandomKey(byte[] key) {
        random.nextBytes(key);
    }

    public static void generateRandomValue(byte[] value) {
        random.nextBytes(value);
    }

    public static int KEY_SIZE = 8;

    public static int VALUE_SIZE = 1024 * 4;

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

    public static class Holder {
        public byte[] value;
        public Holder(byte[] value) { this.value = value; }
        @Override
        public boolean equals(Object o) {
            if (o == null) return false;
            if (o instanceof WriterPro.Holder) {
                WriterPro.Holder h = (WriterPro.Holder) o;
                return Arrays.equals(value, h.value);
            } else return false;
        }
    }

    public static void main(String args[]) throws IOException, EngineException {
        EngineRace1 engine = new EngineRace1();
        String PATH = "c://tmp/midware/temptest";
        engine.open(PATH);
        HashMap<Long, Holder> maps = new HashMap<Long, Holder>();

        int size = 2;
        for (int i = 0; i < size; i++) {
            byte[] key = new byte[KEY_SIZE];
            byte[] value = new byte[VALUE_SIZE];
            generateRandomKey(key);
            generateRandomValue(value);
            maps.put(keyToLong(key), new Holder(value));
            System.out.println("puting key : " + keyToLong(key));
            engine.write(key, value);
        }
        engine.close();
        engine = new EngineRace1();
        engine.open(PATH);
        for (Map.Entry<Long, Holder> entry : maps.entrySet()) {
            byte[] key = new byte[8];
            try {
                byte[] v = engine.read(parse(entry.getKey(), key));
                if (!Arrays.equals(v, entry.getValue().value))
                    System.out.println("not equal for key : " + keyToLong(key));
            } catch (EngineException e) {
                System.out.println("not found for key : " + keyToLong(key));
            }
        }
        engine.close();
    }

}
