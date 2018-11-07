package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.util.HashMap;

public class Item {

	public static final int SIZE = 32;

	private long key;
	
	private long fileNo;
	
	private long offset;

	private long inUse;
	
	public Item(long key, long fileNo, long offset, long inUse) {
		this.key = key;
		this.fileNo = fileNo;
		this.offset = offset;
		this.inUse = inUse;
	}
	
	public long getKey() {
		return this.key;
	}
	
	public long getFileNo() {
		return this.fileNo;
	}
	
	public long getOffset() {
		return  this.offset;
	}

	public boolean isInUse() {
		return this.inUse == 1l;
	}
	
	public void setKey(long key) {
		this.key = key;
	}

	public void setInUse(long inUse) {
		this.inUse = inUse;
	}
	
	public void setFileNo(long fileNo) {
		this.fileNo = fileNo;
	}
	
	public void setOffset(long offset) {
		this.offset = offset;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == null) return false;
		if (o instanceof Item) {
			Item i = (Item) o;
			return i.key == this.key;
		} else return false;
	}
	
	public int hashCode() {
		return (int)(key ^ (key >>> 32));
	}
	
}
