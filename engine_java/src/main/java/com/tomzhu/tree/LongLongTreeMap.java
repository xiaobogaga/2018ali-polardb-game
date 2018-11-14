package com.tomzhu.tree;

/**
 * Created by tomzhu on 18-4-5.
 * a simple map implementation based on binary search tree.
 */
public class LongLongTreeMap {

    private KeydRedBlackTree tree;

    public LongLongTreeMap() {
        this.tree = new KeydRedBlackTree();
    }

    /**
     * check whether this tree contains the element.
     * @param key
     * @return
     */
    public boolean contains(long key) {
        // this is really ugly
        return this.tree.contains(key);
    }

    public void insert(long key, long value) {
        this.tree.insert(key , value);
    }

    public long get(long key) {
        return this.tree.get(key);
    }

    /**
     * remove a item specified by key from this map.
     * @param key
     * @return
     */
    public boolean remove(long key) {
        if (this.contains(key)) {
            this.tree.removeByKey(key);
            return true;
        } else {
            return false;
        }
    }


}
