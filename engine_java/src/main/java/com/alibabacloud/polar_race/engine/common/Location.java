package com.alibabacloud.polar_race.engine.common;

public class Location {

    public long offset;

    public long fileNo;

    public Location(long offset, long fileNo) {
        this.offset = offset;
        this.fileNo = fileNo;
    }

}
