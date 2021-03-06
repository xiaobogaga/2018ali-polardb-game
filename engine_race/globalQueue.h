//
// Created by tomzhu on 18-11-30.
//

#ifndef ENGINE_RACE_GLOBALQUEUE_H
#define ENGINE_RACE_GLOBALQUEUE_H

#include <stdint.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include "data_store.h"
#include "indexstore.h"
#include "config.h"

namespace polar_race {

    class MessageQueue {

    public:

        MessageQueue(DataStore *stores_, IndexStore *indexStores_, std::mutex* mutexes, int threadSize);

        // reading the data of i within this part, and partSize is the total size
        // of this part. for client, must iterator till partSize.
        char *get(int readId, int part, int *i, int *partSize, char** k);

        ~MessageQueue() {
            printInfo(stderr, "[MessageQueue] : finilizing\n");
            for (int i = 0; i < My_queueSize_; i++) free(this->items[i]);
            free(this->items);
            this->loader->join();
            delete this->loader;
            delete [] this->loadCon_;
            printInfo(stderr, "[MessageQueue] : finilized\n");
        }

    private:
        struct QueueItem** items;
        DataStore *stores;
        IndexStore *indexStores;
        std::thread *loader;
        int readCounter[My_parties_];
        std::mutex anotherLock;
        std::condition_variable* loadCon_;
    };

}


#endif //ENGINE_RACE_GLOBALQUEUE_H
