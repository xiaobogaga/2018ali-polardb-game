package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

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
            ans |= (((offset >>> i) & 1) << i);
            ans |= (((fileNo >>> i) & 1) << (32 + i));
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
        for (int i = 32; i < 64; i++) {
            ans |= (((wrapper >>> i) & 1) << (32 - i));
        }
        return ans;
    }

    public static void main(String args[]) throws EngineException {

        HashMap<Integer, Integer> maps = new HashMap<Integer, Integer> ();
        Random rand = new Random(System.currentTimeMillis());
        int test = 10;
        while (test > 0) {
            int temp1 = rand.nextInt();
            int temp2 = rand.nextInt();
            if (maps.containsKey(temp1)) continue;
            else maps.put(temp1, temp2);
            test --;
        }

        for (Map.Entry<Integer, Integer> entry : maps.entrySet()) {
            int temp1 = entry.getKey();
            int temp2 = entry.getValue();
            long ans = wrap(temp1, temp2);
            int ans1 = unwrapOffset(ans);
            int ans2 = unwrapFileNo(ans);
            if (ans1 != temp1 || ans2 != temp2) System.out.println("failed");
        }

    }

}
