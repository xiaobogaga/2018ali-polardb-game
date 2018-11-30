// here is mainly used for config data

#ifndef MY_CONFIG
#define MY_CONFIG

#include <stdio.h>
#include <string>

namespace polar_race {

    static const std::string dataPath = "/data/";
    static const char kDataFilePrefix[] = "DATA_";
    static const int kDataFilePrefixLen = 5;
    static const int valuesize = 4096;
    static const char kLockFile[] = "LOCK";
    static const double sleepTime = 200.1;
    static bool timerStop = false;
    static const std::string indexPrefix = "/index/";
    // related to parties.
    static const int parties = 64;
    static const int kSingleFileSize = 1024 * 1024 * 128;
    static const long long sep = 288230376151711743;
    static const int singleInfosSize = 1048576;
    static const int map_size = 1024 * 1024 * 12;
    static const double bf_p = 0.0001;
    static const int bf_capacity = 1000000;
    static const int queue_size = 1;

    struct Info {
        long long key;
        uint32_t info;
    };

    struct Item {
        uint32_t info;
        char key[8];
    };

    static inline int partition(long long key) {
        int party = ((unsigned long long) (key - INT64_MIN)) / sep;
        return party == parties ? parties - 1 : party;
    }

}



#endif