package com.index;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class BPlusTreeTest extends TestCase {

    public int size = 8000;
    public BPlusTree tree;
    public HashMap<Long, Long> hashMap;
    public Random rand;
    public String path = "/tmp/btree2";
    public boolean clearPath = true;

    public synchronized void testAdd() throws IOException, EngineException {
        File dir = new File(path);
        if (dir.exists()) {
            for (File f : dir.listFiles())
                if (!f.delete()) System.out.println("delete failed");
        }
        rand = new Random(System.currentTimeMillis());
        tree = new BPlusTree(path, 1024);
        hashMap = new HashMap<>();
        for (int i = 0; i < size;) {
            long temp = rand.nextInt();
            // System.err.println("adding : " + temp);
            if (hashMap.containsKey(temp)) continue;
            else {
                hashMap.put(temp, temp);
                tree.add(temp, temp);
            }
            i ++;
        }

        // System.out.println("finished");

        for (Map.Entry<Long, Long> entry : hashMap.entrySet()) {
            // System.err.println("assert equals for key : " + entry.getKey());
            assertEquals((long) entry.getValue(), (long) tree.get(entry.getKey()));
        }

    }

    public void testGet() throws IOException, EngineException {
        File dir = new File(path);
        if (dir.exists()) {
            for (File f : dir.listFiles())
                if (!f.delete()) System.out.println("delete failed");
        }
        tree = new BPlusTree(path, 1024);
        HashMap<Long, Long> hashMap = new HashMap<Long, Long>();
        rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < size;) {
            long temp = rand.nextLong();
            if (hashMap.containsKey(temp)) continue;
            else {
                hashMap.put(temp, temp);
                tree.add(temp, temp);
            }
            i ++;
        }

        // test get method
        for (Map.Entry<Long, Long> entry : hashMap.entrySet()) {
            assertEquals((long) entry.getValue(), (long) tree.get(entry.getKey()));
        }
        tree.close();
        tree = new BPlusTree(path, 1024);
        // now test recover and updating.
        for (int i = 0; i < size;) {
            long temp = rand.nextLong();
            if ((temp & 1l) == 1) {
                // updating value.
                if (temp < 0) temp = -temp;
                long k = (Long) hashMap.keySet().toArray()[(int) (temp % hashMap.size()) ];
                hashMap.put(k, k + temp);
                tree.add(k, k + temp);
                i ++;
            } else {
                if (hashMap.containsKey(temp)) continue;
                else {
                    hashMap.put(temp, temp);
                    tree.add(temp, temp);
                }
                i ++;
            }
        }
        // test get method.
        for (Map.Entry<Long, Long> entry : hashMap.entrySet()) {
            assertEquals((long) entry.getValue(), (long) tree.get(entry.getKey()));
        }
        tree.close();
        tree = new BPlusTree(path, 1024);
        for (int i = 0; i < size;) {
            long temp = rand.nextLong();
            if ((temp & 1l) == 1) {
                // updating value.
                if (temp < 0) temp = -temp;
                long k = (Long) hashMap.keySet().toArray()[(int) (temp % hashMap.size()) ];
                hashMap.put(k, k + temp);
                tree.add(k, k + temp);
                i ++;
            } else {
                if (hashMap.containsKey(temp)) continue;
                else {
                    hashMap.put(temp, temp);
                    tree.add(temp, temp);
                }
                i ++;
            }
        }
        // test get method.
        for (Map.Entry<Long, Long> entry : hashMap.entrySet()) {
            assertEquals((long) entry.getValue(), (long) tree.get(entry.getKey()));
        }
        tree.close();

    }

}