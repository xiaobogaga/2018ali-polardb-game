package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.madhukaraphatak.sizeof.SizeEstimator;

import java.util.HashMap;

public class Test {

    public static void main(String args[]) throws EngineException {

        float loadFactor = 32f;
        int size = 1024 * 1024;
        HashMap<Integer, Integer> maps = new HashMap<Integer, Integer>(size, loadFactor);
        int tSize = size / 3;
        for (int i = 0; i < tSize; i++)
            maps.put(i, i);
        System.out.println(SizeEstimator.estimate(maps));


    }

}
