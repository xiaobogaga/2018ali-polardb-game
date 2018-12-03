//
// Created by tomzhu on 18-11-30.
//

#ifndef ENGINE_RACE_GLOBALQUEUE_H
#define ENGINE_RACE_GLOBALQUEUE_H

#include <stdint.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "data_store.h"
#include "indexstore.h"
#include "config.h"

namespace polar_race {

    static std::mutex* mutexLocks;
    static int realItemSizes[My_queueSize_];
    static int loaded = 0;
    static int readedPart = -1;
    static std::condition_variable* loadCon;

    class MessageQueue {

    public:

        MessageQueue(DataStore *stores_, IndexStore *indexStores_, std::mutex* mutexes);

        // reading the data of i within this part, and partSize is the total size
        // of this part. for client, must iterator till partSize.
        char *get(int part, int *i, int *partSize, long long *k);

        ~MessageQueue() {
            printInfo(stderr, "[MessageQueue] : finilizing\n");
            for (int i = 0; i < My_queueSize_; i++) free(this->items[i]);
            free(this->items);
            this->loader->join();
            delete this->loader;
            delete [] this->loadCon_;
            printInfo(stderr, "[MessageQueue] : finilized\n");
        }
        std::condition_variable notFullCV;

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
