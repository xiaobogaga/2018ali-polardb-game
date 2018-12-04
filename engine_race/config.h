//
// Created by tomzhu on 18-11-30.
//

#ifndef ENGINE_RACE_CONFIG_H
#define ENGINE_RACE_CONFIG_H

#include <stdint.h>
#include <string>

namespace polar_race {
    static const int My_valuesize_ = 4096;

// following variable needs to be changed for different parties.
//     const int parties = 512;
//     const int kSingleFileSize = 1024 * 1024 * 128;
//     const int map_size = 1024 * 512 * 3;
//     const int bf_capa = 1024 * 128;
//     const double bf_p = 0.0001;
//     const int infoArraySize = 1024 * 128;
//     const int queueCapacity = 1024 * 128;
//     const int queueSize = 2;

    static const int My_parties_ = 512;
    static const int My_kSingleFileSize_ = 1024 * 1024 * 128;
    static const int My_map_size_ = 1024 * 512 * 3;
    static const int My_infoArraySize_ = 1024 * 128;
    static const int My_queueCapacity_ = 1024 * 128;
    static const int My_queueSize_ = 2;

    extern volatile bool My_directStop_;
    extern volatile bool My_exceedTime_;


//    static inline int partition(long long key) {
//        int party = ((unsigned long long) (key - INT64_MIN)) / 9223372036854775807;
//        return party == parties ? parties - 1 : party;
//    }

//    static inline int partition(long long key) {
//        int party = ((unsigned long long) (key - INT64_MIN)) / 36028797018963967;
//        return 50;
//    }


    inline int partition(long long key) {
        int party = ((unsigned long long) (key - INT64_MIN)) / 36028797018963967;
        return party == My_parties_ ? My_parties_ - 1 : party;
    }

    struct Item {
        uint32_t info;
        char key[8];
    };

    struct Info {
        long long key;
        uint32_t info;
    };

    struct QueueItem {
        char data[My_valuesize_];
    };

}

#endif //ENGINE_RACE_CONFIG_H
