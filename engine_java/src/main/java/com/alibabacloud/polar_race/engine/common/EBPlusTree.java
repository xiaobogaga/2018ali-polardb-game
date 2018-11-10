package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

/**
 * an external memory b plus tree implementation.
 * we use two files: a file for storing leaf nodes and a file
 * for non-leaf nodes and the meta data located at non-leaf files.
 
 * meta information organization:
 * |int| -> order
 * then root node: so root node start at offset 8.
 * there are two kinds of nodes in a btree. 
 */
 
 public class EBPlusTree {
	 
	 private int leaf_order = ;
	 private int mid_order = 1024 / 8;
	 
	 private long offset = 8;
	 
	 private final String PATH;
	 private String leafNodeFileName = "leaf";
	 private String midNodeFileName = "mid";
	 private RandomAccessFile leafNodeFile;
	 private RandomAccessFile midNodeFile;
	 private long conflictTime = 0l;
	 
	 public EBPlusTree(String path, int leaf_order, int mid_order) {
		 this.PATH = path;
		 this.leaf_order = leaf_order;
		 this.mid_order = mid_order;
		 init();
	 }
	 
	 private void init() {
		 conflictTime = 0;
		 System.err.println("init EBPlusTree");
		 this.leafNodeFile = new RandomAccessFile(new File(this.PATH + leafNodeFileName), "rw");
		 this.midNodeFile = new RandomAccessFile(new File(this.PATH + midNodeFileName), "rw");
		 if (this.midNodeFile.length() != 0) {
			 this.leaf_order = this.midNodeFile.readInt();
			 this.mid_order = this.midNodeFile.readInt();
		 } else {
			 this.midNodeFile.writeInt(this.leaf_order);
			 this.midNodeFile.writeInt(this.mid_order);
		 }
		 this.leafNodeFile.seek(this.leafNodeFile.length());
		 this.midNodeFile.seek(this.midNodeFile.length());
	 }
	 
	 public void insert(long key, long info) {
		 /*
			the inserting procedure works like : first find the inserting leaf.
			then do insert and then check whether needs split from bottom -> up
		 */
		 
	 }
	 
	 public long get(long key) {
		 /*
		 
		 */
		 return 0l;
	 }
	 
	
	 
	 
	 
 }
