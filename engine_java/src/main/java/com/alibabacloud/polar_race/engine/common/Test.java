package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.madhukaraphatak.sizeof.SizeEstimator;

public class Test {

    public static void main(String args[]) throws EngineException {

        EngineRace1 engineRace1 = new EngineRace1();
        engineRace1.open("/tmp");
        byte[] key = new byte[8];
        byte[] value = new byte[1024 * 4];
        engineRace1.write(key, value);
        System.out.println(SizeEstimator.estimate(engineRace1));
        engineRace1.close();
        engineRace1 = new EngineRace1();
        engineRace1.open("/tmp");
        engineRace1.read(key);
        System.out.println(SizeEstimator.estimate(engineRace1));
        engineRace1.close();


    }

}
