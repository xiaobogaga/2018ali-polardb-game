package com.index;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.io.File;
import java.io.IOException;

public class TempTest {

    static long[] buffer = new long[100];

    public static int binarySearch(long key, int size) {
        int left = 0, right = size - 1;
        int ans = -1;
        while (left <= right) {
            int mid = (left + right) >>> 1;
            System.out.println(mid);
            if (buffer[mid * 2] <= key) {
                ans = mid;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return ans;
    }

    public static void main(String args[]) throws IOException, EngineException {
        long[] key = new long[]{
                507,
                759,
                178,
                567,
                127,
                98,
                229,
                119,
                637,
                294,
                752,
                529,
                869,
                79,
                19,
                719,
                979,
                937,
                492,
                835,
        };

        String path = "/tmp/btree";
        File dir = new File(path);
        if (dir.exists()) {
            for (File f : dir.listFiles())
                if (!f.delete()) System.out.println("delete failed");
        }
        BPlusTree tree = new BPlusTree(path, 1024);

        for (long k : key) {
            System.err.println(k + ",");
            tree.add(k, k);
        }

        tree.printTree();
        System.out.println(tree.get(229));


    }

}
