//
// Created by tomzhu on 18-11-30.
//

#ifndef ENGINE_RACE_CONFIG_H
#define ENGINE_RACE_CONFIG_H

#include <stdint.h>
#include <string>

namespace polar_race {

    static const std::string indexPrefix("/index/");
    static const std::string dataPath("/data/");
    static const char kDataFilePrefix[] = "DATA_";
    static const int kDataFilePrefixLen = 5;
    static const int valuesize = 4096;
    static const int keysize = 8;
    static const char kLockFile[] = "LOCK";
    static const double sleepTime = 100.1;
    static int threadSize = 64;

    // following variable needs to be changed for different parties.
//    static const int parties = 512;
//    static const int kSingleFileSize = 1024 * 1024 * 128;
//    static const int map_size = 1024 * 512 * 3;
//    static const int bf_capa = 1024 * 128;
//    static const double bf_p = 0.0001;
//    static const int infoArraySize = 1024 * 128;
//    static const int queueCapacity = 1024 * 128;
//    static const int queueSize = 2;

    static const int parties = 512;
    static const int kSingleFileSize = 1024 * 1024 * 128;
    static const int map_size = 1024 * 512 * 3;
    static const int bf_capa = 1024 * 128;
    static const double bf_p = 0.0001;
    static const int infoArraySize = 1024 * 128;
    static const int queueCapacity = 1024 * 128;
    static const int queueSize = 2;

    static volatile bool directStop = false;
    static volatile bool exceedTime = false;


    static inline int partition(long long key) {
        int party = ((unsigned long long) (key - INT64_MIN)) / 36028797018963967;
        return party == parties ? parties - 1 : party;
    }

//    static inline int partition(long long key) {
//        int party = ((unsigned long long) (key - INT64_MIN)) / 9223372036854775807;
//        return party == parties ? parties - 1 : party;
//    }

//    static inline int partition(long long key) {
//        int party = ((unsigned long long) (key - INT64_MIN)) / 36028797018963967;
//        return 50;
//    }

    struct Item {
        uint32_t info;
        char key[8];
    };

    struct Info {
        long long key;
        uint32_t info;
    };

    struct QueueItem {
        char data[valuesize];
    };

    static std::string FileName(const std::string &dir, uint32_t fileno) {
        return dir + "/" + kDataFilePrefix + std::to_string(fileno);
    }

}

#endif //ENGINE_RACE_CONFIG_H
