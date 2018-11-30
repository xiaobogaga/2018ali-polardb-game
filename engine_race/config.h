//
// Created by tomzhu on 18-11-30.
//

#ifndef ENGINE_RACE_CONFIG_H
#define ENGINE_RACE_CONFIG_H

#include <stdint.h>
#include <string>

namespace polar_race {

    static const int map_size = 1024 * 1024 * 12;
    static const std::string indexPrefix("/index/");
    static const std::string dataPath("/data/");
    static const char kDataFilePrefix[] = "DATA_";
    static const int kDataFilePrefixLen = 5;
    static const int kSingleFileSize = 1024 * 1024 * 128;
    static const int valuesize = 4096;
    static const char kLockFile[] = "LOCK";
    static const double sleepTime = 300.1;
    static const int parties = 64;
    static const int bf_capa = 1000000;
    static const double bf_p = 0.0001;
    static const int infoArraySize = 1024 * 1024;

    static inline int partition(long long key) {
        int party = ((unsigned long long) (key - INT64_MIN)) / 288230376151711743;
        return party == 64 ? 64 - 1 : party;
    }

    struct Item {
        uint32_t info;
        char key[8];
    };

    struct Info {
        long long key;
        uint32_t info;
    };

}

#endif //ENGINE_RACE_CONFIG_H
