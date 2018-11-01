package com.tomzhu.test;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.util.concurrent.BrokenBarrierException;

public class Test {

    public static void main(String args[]) throws 
		InterruptedException, EngineException, BrokenBarrierException {

        if (args.length <= 1) {
            System.out.println("java com.tomzhu.test.ReaderPro thread_size writing_size");
            return;
        }
		System.out.println("Correcness Test");
        int thread_size = Integer.parseInt(args[0]);
        int write_size = Integer.parseInt(args[1]);
        String phase = args[2];
        System.out.println("start writing...");
        WriterPro.startWriter(thread_size, write_size);
        Thread.sleep(1000 * 15); // 15s.
        WriterPro.shutdown = true;
		WriterPro.countDownLatch.await();
        System.out.println("start reading");
        ReaderPro.startReader(WriterPro.engine, WriterPro.maps, thread_size, write_size);
        // performance test.
		System.out.println("Performance Test");
        WriterPro.shutdown = false;
        WriterPro.testPerformance(thread_size, write_size);
        ReaderPro.testPerformance(WriterPro.engine, WriterPro.maps, thread_size, write_size);
        
    }


}
