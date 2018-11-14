set thread_size=2
set writing_times=10
set print_all=0
java -classpath "%classpath%;C:\Users\m1820\Desktop\work\competition\polardb\engine2\engine\engine_java\target\classes" -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/gc.hprof -XX:+PrintGCDetails -server -Xms1560m -Xmx1560m -XX:MaxDirectMemorySize=1124m -XX:NewRatio=1 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:-UseBiasedLocking com.tomzhu.test.Test %thread_size% %writing_times% w %print_all%
set thread_size=
set writing_times=
set print_all=