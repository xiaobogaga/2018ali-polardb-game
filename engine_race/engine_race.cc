// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <map>
#include <thread>
#include <assert.h>
#include "util.h"
#include "engine_race.h"
#include "config.h"

namespace polar_race {

    static const double My_sleepTime_ = 500.1;
    static const char My_kLockFile_[] = "LOCK";
    static const int My_keysize_ = 8;

    RetCode Engine::Open(const std::string &name, Engine **eptr) {
        return EngineRace::Open(name, eptr);
    }

// sleepint for 500s.
    void startTimer() {
        printInfo(stderr, "[Timer] : start timer\n");
        time_t timer;
        time(&timer);
        while (difftime(time(NULL), timer) <= My_sleepTime_ && !My_directStop_ ) {
            sleep(1);
        }
        if (! My_directStop_ ) {
            My_exceedTime_ = true;
            printInfo(stderr, "[Timer] : exceed time and exist\n");
            sleep(1); // sleep for 1 second.
            exit(0);
        }
    }

    Engine::~Engine() {
    }

    RetCode EngineRace::Open(const std::string &name, Engine **eptr) {
        printInfo(stderr, "[EngineRace] : open db\n");
        *eptr = NULL;
        EngineRace *engine_race = new EngineRace(name);
        engine_race->resetCounter();

        if (0 != LockFile(name + "/" + My_kLockFile_, &(engine_race->db_lock_))) {
            printInfo(stderr, "[EngineRace] : lock file failed\n");
            delete engine_race;
            return kIOError;
        }

        // start a timer task
        My_directStop_ = false;
        My_exceedTime_ = false;
        engine_race->timerTask = new std::thread(startTimer);

        *eptr = engine_race;
        return kSucc;
    }


    EngineRace::~EngineRace() {
        printInfo(stderr, "[EngineRace] : closing db\n");
        if (db_lock_) {
            UnlockFile(db_lock_);
        }
        // delete[] this->mutexes;
        delete[] this->store_;
        delete[] this->indexStore_;

        if (this->timerTask != NULL) {
            My_directStop_ = true;
            this->timerTask->join();
            delete this->timerTask;
            this->timerTask = NULL;
        }

        if (this->mutexes != NULL) {
            delete[] this->mutexes;
            this->mutexes = NULL;
        }
        if (this->queue != NULL) {
            delete this->queue;
            this->queue = NULL;
        }
    }

    RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
        const std::string &v = value.ToString();
        long long k = strToLong(key.data());
        int party = partition(k);
        this->mutexes[party].lock();
        // pthread_mutex_lock(&this->mutexes[party]);
        uint16_t offset = 0;
        uint16_t fileNo = 0;
        RetCode ret = store_[party].Append(v, &fileNo, &offset);
        uint32_t info = wrap(offset, fileNo);
        if (ret == kSucc) {
//    if (writeCounter == 0) {
//      time(&write_timer);
//        printInfo(stderr, "[EngineRace] : writing data. key : %lld, party : %d, offset : %d, fileNo : %d, info : %ld\n",
//                  k, party, offset, fileNo, wrap(offset, fileNo));
//    }
            this->indexStore_[party].add(key, info);
        }

        // printInfo(stderr, "w:%lld,%d,%d,%d\n", k, party, offset, fileNo);

//  writeCounter ++;
//  if (writeCounter % 300000 == 0) {
//    time_t current_time = time(NULL);
//     printInfo(stderr, "[EngineRace] : have writing 300000 data, and spend %f s\n", difftime(current_time, write_timer));
//    write_timer = current_time;
//  }

        // assert(this->writeCounter < 6400000);

        // pthread_mutex_unlock(&this->mutexes[party]);
        this->mutexes[party].unlock();
        return ret;
    }

    RetCode EngineRace::Read(const PolarString &key, std::string *value) {
        int c = readCounter.load();
        readCounter++;
        long long k = strToLong(key.data());
        int party = partition(k);
        uint16_t fileNo = -1;
        uint16_t offset = -1;
        uint32_t ans = 0;

        this->mutexes[party].lock();
        // pthread_mutex_lock(&this->mutexes[party]);

        RetCode ret = kSucc;
        this->indexStore_[party].get(k, &ans);
        if (ans == 0) ret = kNotFound;
        else {
            offset = unwrapOffset(ans);
            fileNo = unwrapFileNo(ans);
            if (c == 0) {
                time(&read_timer);
                //  printInfo(stderr, "[EngineRace] : reading first key : %lld... offset : %d, fileNo : %d, info : %ld\n",
                //          k, offset, fileNo, wrap(offset, fileNo));
            }
        }

        // printInfo(stderr, "r:%lld,%d,%d,%d\n", k, party, offset, fileNo);

        if (ret == kSucc) {
            value->clear();
            ret = store_[party].Read(fileNo, offset, value);
        }

//        if (c == 0) {
//            printInfo(stderr, "[EngineRace] : reading first data finished, key : %lld, and get %lu value\n",
//                    k, value->size());
//        }

        if (c % 300000 == 0) {
            time_t current_time = time(NULL);
            printInfo(stderr, "[EngineRace] : have read 300000 data and spend %f s\n",
                    difftime(current_time, read_timer));
            read_timer = current_time;
        }

        // pthread_mutex_unlock(&this->mutexes[party]);
        this->mutexes[party].unlock();
        return ret;
    }

    RetCode EngineRace::Range(const PolarString &lower, const PolarString &upper,
                              Visitor &visitor) {

        int part = 0;
        this->mu_.lock();
        part = this->rangeCounter ++;
        fprintf(stderr, "[EngineRace] : part %d coming\n", part);
        this->mu_.unlock();

        int temp = this->rangeCounter;
        int sameTime = 0;
        while ( (this->rangeCounter == 1 && sameTime < 10)) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            if (temp == this->rangeCounter) { sameTime ++;}
            else {
                temp = this->rangeCounter;
                sameTime = 0;
            }
        }

        this->mu_.lock();
        if (this->queue == NULL) {
            delete[] this->mutexes;
            this->mutexes = new std::mutex[My_parties_];
            this->queue = new MessageQueue(this->store_, this->indexStore_,
                    this->mutexes, this->rangeCounter == 1 ? 1 : 64);
        }
        this->mu_.unlock();

        printInfo(stderr, "[EngineRace-%d] : start range reader, current threadSize : %d\n",
                part, this->rangeCounter);
        time_t range_timer;
        long long low = lower.size() == 0 ? INT64_MIN : strToLong(lower.data());
        long long high = upper.size() == 0 ? INT64_MAX : strToLong(upper.data());
        time(&range_timer);
        // do range query.
        long size = 0;
        int partSize = 0, j = -1;
        long long keyPointer = -1;
        char buf[8];
        char* ans = NULL;
        for (int i = 0; i < My_parties_; i++) {
            j = -1;
            // printInfo(stderr, "[EngineRace-%d] : range read part %d\n", part, i);
            // it turns out there would be an error when i reach to 256. it is a little wired.
            // lets print some detail when at 256.

            if (i == 256) {
                // we must read some information now.
                printInfo(stderr, "[EngineRace-%d] : start read 256 part\n", part);
            }

            ans = this->queue->get(part, i, &j, &partSize, &keyPointer);
            for (j++; j <= partSize; j++) {
                if (ans == NULL) break;
                else {
                    longToStr(keyPointer, buf);
                    PolarString key(buf, My_keysize_);
                    PolarString value(ans, My_valuesize_);
                    visitor.Visit(key, value);
                    size ++;
                }
                ans = this->queue->get(part, i, &j, &partSize, &keyPointer);
            }
        }
        printInfo(stderr, "[EngineRace-%d] : range read. [%lld, %lld) with %ld data. spend %f s\n",
                    part, low, high, size, difftime(time(NULL), range_timer));
        return kSucc;

    }

    RetCode EngineRace::MyRange(const PolarString &lower, const PolarString &upper,
                              Visitor &visitor) {
        int part = 0;
        this->mu_.lock();
        part = this->rangeCounter ++;
        if (this->queue == NULL) {
            delete[] this->mutexes;
            this->mutexes = new std::mutex[My_parties_];
            this->queue = new MessageQueue(this->store_,
                        this->indexStore_, this->mutexes, 64);
        }
        this->mu_.unlock();

        printInfo(stderr, "[EngineRace] : part-%d : start range read with rangeCounter : %d\n", part,
                this->rangeCounter);
        time_t range_timer;
        long long low = lower.size() == 0 ? INT64_MIN : strToLong(lower.data());
        long long high = upper.size() == 0 ? INT64_MAX : strToLong(upper.data());
        time(&range_timer);
        // do range query.
        long size = 0;
        int partSize = 0, j = -1;
        long long keyPointer = -1;
        char buf[8];
        char* ans = NULL;
        for (int i = 0; i < My_parties_; i++) {
            j = -1;
            // printInfo(stderr, "[EngineRace] : part-%d : range read part %d\n", part, i);
            ans = this->queue->get(part, i, &j, &partSize, &keyPointer);
            for (j++; j <= partSize; j++) {
                if (ans == NULL) break;
                else {
                    longToStr(keyPointer, buf);
                    PolarString key(buf, My_keysize_);
                    PolarString value(ans, My_valuesize_);
                    visitor.Visit(key, value);
                    size ++;
                }
                ans = this->queue->get(part, i, &j, &partSize, &keyPointer);
            }
        }
        printInfo(stderr, "[EngineRace] : part-%d : range read. [%lld, %lld) with %ld data. spend %f s\n",
                part, low, high, size, difftime(time(NULL), range_timer));
        return kSucc;

    }

}  // namespace polar_race

