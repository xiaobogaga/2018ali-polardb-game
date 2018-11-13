package com.index;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

public class CalcCapacity {

    public static void main(String args[]) throws IOException {
        String path = "/tmp/btree";
        File dir = new File(path);
        if (dir.exists()) {
            for (File f : dir.listFiles())
                if (!f.delete()) System.out.println("delete failed");
        }
        BPlusTree bPlusTree = new BPlusTree(path, 1024);
        HashMap<Long, Long> hashMap = new HashMap<>();
        Random rand = new Random();
        long size = 0;
        long temp = 0;
        int printLimit = 1000000;
        while (true) {
//            long temp = rand.nextLong();
//            if (hashMap.containsKey(temp)) continue;
            // else {
//                hashMap.put(temp, temp);
            bPlusTree.add(temp, temp);
            size++;
            temp++;
            if (size % printLimit == 0) System.out.println("have inserting size " + size);
            if (size > 20000000) break;
            //}
        }
    }

}
