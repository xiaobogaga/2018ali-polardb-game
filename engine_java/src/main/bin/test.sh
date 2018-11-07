#!/bin/bash

thread_size=4
writing_times=10000
print_all=0
java -classpath "$classpath:/home/tomzhu/test/engine3/engine/engine_java/target/classes" com.tomzhu.test.Test $thread_size $writing_times w $print_all