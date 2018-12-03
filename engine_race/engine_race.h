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
#include "config.h"
#include "globalQueue.h"

namespace polar_race {

    class EngineRace : public Engine {
    public:
        static RetCode Open(const std::string &name, Engine **eptr);

        explicit EngineRace(const std::string &dir)
                : mu_(PTHREAD_MUTEX_INITIALIZER),
                  db_lock_(NULL), writeCounter(0),
                  readCounter(0), queue(NULL) {
            this->store_ = new DataStore[parties];
            this->indexStore_ = new IndexStore[parties];
            this->mutexes = new std::mutex[parties];
            RetCode ret;
            for (int i = 0; i < parties; i++) {
                this->store_[i].setDir(dir);
                this->store_[i].setParty(i);
                ret = this->store_[i].Init();
                if (ret != kSucc) {
                    printInfo(stderr, "[EngineRace] : init store failed \n");
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

        RetCode MyRange(int part, const PolarString &lower, const PolarString &upper,
                                    Visitor &visitor);

        void resetCounter() {
            writeCounter = 0;
            readCounter = 0;
        }

    private:
        pthread_mutex_t mu_;
        FileLock *db_lock_;
        IndexStore *indexStore_;
        DataStore *store_;
        std::atomic_int writeCounter;
        std::atomic_int readCounter;
        time_t read_timer;
        std::mutex* mutexes;
        std::thread *timerTask;
        MessageQueue* queue;
    };

}  // namespace polar_race

// #define USE_HASH_TABLE 0

#endif  // ENGINE_EXAMPLE_ENGINE_EXAMPLE_H_
