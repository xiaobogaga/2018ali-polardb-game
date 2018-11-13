package com.index;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Test {

    static long[] buffer = new long[100];

    public static int binarySearch(long key, int size) {
        int left = 0, right = size - 1;
        int ans = -1;
        while (left <= right) {
            int mid = (left + right) >>> 1;
            // System.out.println(mid);
            if (buffer[mid * 2] <= key) {
                ans = left;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return ans;
    }

    public static void main(String args[]) throws IOException, EngineException {


        String path = "/tmp/btree";
        BPlusTree bPlusTree = new BPlusTree(path, 1024);
        HashMap<Long, Long> hashMap = new HashMap<Long, Long>();
        int size = 300;
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < size; ) {
            long temp = rand.nextLong();
//            if (i == size - 1)
//                System.out.println(temp);
            if (hashMap.containsKey(temp)) continue;
            else {
            //    System.out.println("adding " + temp);
                hashMap.put(temp, temp);
                bPlusTree.add(temp, temp);
                i ++;
            }
        }

        for (Map.Entry<Long, Long> entry: hashMap.entrySet()) {
            if ( entry.getValue() != (long) bPlusTree.get(entry.getKey()))
                System.out.println(entry.getValue() +
                        " error " + bPlusTree.get(entry.getKey()));
        }

        bPlusTree.close();

        bPlusTree = new BPlusTree(path, 1024);
        for (Map.Entry<Long, Long> entry: hashMap.entrySet()) {
            if ( entry.getValue() != (long) bPlusTree.get(entry.getKey()))
                System.out.println(entry.getValue() +
                        " error " + bPlusTree.get(entry.getKey()));
        }

        bPlusTree.close();

    }

}
