//
// Created by tomzhu on 18-11-30.
//

#ifndef ENGINE_RACE_GLOBALQUEUE_H
#define ENGINE_RACE_GLOBALQUEUE_H

#include <cstdint>
#include "stdio.h"
#include "config.h"
#include "data_store.h"

// we define a global concurrent blocking queue here.

namespace polar_race {

    void load(int party, struct Info *infos, int size, DataStore *store_) {

    }

        struct QueueItem {
        char data[4096];
        uint8_t indicator = 64;
    };

    class GlobalQueue {

        // loading a data set.


    private:
        int idx;
        int totalSize;
    };

}

#endif //ENGINE_RACE_GLOBALQUEUE_H
