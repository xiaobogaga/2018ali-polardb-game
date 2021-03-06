// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_

#include <pthread.h>
#include <string>
#include "../include/engine.h"
#include "indexstore.h"
#include "data_store.h"
#include "util.h"
#include <mutex>
#include <ctime>
#include <atomic>
#include <thread>
#include "globalQueue.h"
#include "config.h"

namespace polar_race {

    class EngineRace : public Engine {
    public:
        static RetCode Open(const std::string &name, Engine **eptr);

        explicit EngineRace(const std::string &dir)
                : db_lock_(NULL), writeCounter(0),
                  readCounter(0), rangeCounter(0), startRange(false), queue(NULL), min(INT64_MAX), minFileNo(1),
                  minOffset(0), allended(false) {
            this->store_ = new DataStore[My_parties_];
            this->indexStore_ = new IndexStore[My_parties_];
            this->mutexes = new std::mutex[My_parties_];
            RetCode ret;
            for (int i = 0; i < My_parties_; i++) {
                this->store_[i].setDir(dir);
                this->store_[i].setParty(i);
                ret = this->store_[i].Init();
                if (ret != kSucc) {
                    printInfo(stderr, "[EngineRace] : ERROR. init store failed \n");
                }
                this->indexStore_[i].init(dir, i);
            }
            this->timerTask = NULL;
            printInfo(stderr, "[EngineRace] : creating an engineRace instance at %s\n",
                    dir.c_str());
        }

        ~EngineRace();

        RetCode Write(const PolarString &key,
                      const PolarString &value) override;

        RetCode Read(const PolarString &key,
                     std::string *value) override;

        RetCode Range(const PolarString &lower,
                      const PolarString &upper,
                      Visitor &visitor) override;

        RetCode MyRange(const PolarString &lower, const PolarString &upper,
                                    Visitor &visitor);

        void resetCounter() {
            writeCounter = 0;
            readCounter = 0;
        }

    private:
        std::mutex mu_;
        FileLock *db_lock_;
        IndexStore *indexStore_;
        DataStore *store_;
        std::atomic_int writeCounter;
        std::atomic_int readCounter;
        volatile int rangeCounter;
        volatile bool startRange;
        time_t read_timer;
        std::mutex* mutexes;
        std::thread *timerTask;
        MessageQueue* queue;
        unsigned long long min;
        uint16_t minFileNo;
        uint16_t minOffset;
        std::mutex endLock;
        volatile bool allended;
    };

}  // namespace polar_race

// #define USE_HASH_TABLE 0

#endif  // ENGINE_EXAMPLE_ENGINE_EXAMPLE_H_
