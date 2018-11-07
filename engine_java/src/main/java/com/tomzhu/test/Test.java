package com.tomzhu.test;

import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.io.Writer;
import java.util.concurrent.BrokenBarrierException;

public class Test {

    public static void main(String args[]) throws 
		InterruptedException, EngineException, BrokenBarrierException {

        if (args.length <= 2) {
            System.err.println("java com.tomzhu.test.ReaderPro thread_size writing_size w print_all");
            return;
        }
		System.err.println("Correcness Test");
        int thread_size = Integer.parseInt(args[0]);
        int write_size = Integer.parseInt(args[1]);
		int print_all = Integer.parseInt(args[3]);
        String phase = args[2];
		if (print_all == 1) EngineRace.printAll = true;
        System.err.println("start writing...");
        WriterPro.startWriter(thread_size, write_size);
        Thread.sleep(1000 * 15); // 15s.
        WriterPro.shutdown = true;
		WriterPro.countDownLatch.await();
        System.err.println("start reading");
        ReaderPro.startReader(WriterPro.engine, WriterPro.maps, thread_size, write_size);
		WriterPro.engine.close();
        // performance test.
		System.err.println("Performance Test");
        WriterPro.shutdown = false;
        WriterPro.testPerformance(thread_size, write_size);
        ReaderPro.testPerformance(WriterPro.engine, WriterPro.maps, thread_size, write_size);
        
    }


}
