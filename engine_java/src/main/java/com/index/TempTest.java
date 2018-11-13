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
        long[] key = new long[] {
                643, 977, 642, 297, 997, 609, 17, 135, 870, 323
        };

        String path = "/tmp/btree";
        File dir = new File(path);
        if (dir.exists()) {
            for (File f : dir.listFiles())
                if (!f.delete()) System.out.println("delete failed");
        }
        BPlusTree tree = new BPlusTree(path, 1024);

        for (long k : key) {
            tree.add(k, k);
        }

        System.out.println(tree.get(643));

    }

}
