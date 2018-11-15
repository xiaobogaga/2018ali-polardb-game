#!/bin/bash

thread_size=4
writing_times=10000
print_all=0
# -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/gc.hprof -XX:+PrintGCDetails
java -classpath "$classpath:/home/tomzhu/test/engine5/engine/engine_java/target/classes" -server -Xms1560m -Xmx1560m -XX:MaxDirectMemorySize=1124m -XX:NewRatio=1 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:-UseBiasedLocking com.tomzhu.test.Test $thread_size $writing_times w $print_all