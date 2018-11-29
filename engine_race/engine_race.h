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

namespace polar_race {

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

  explicit EngineRace(const std::string& dir)
    : mu_(PTHREAD_MUTEX_INITIALIZER),
    db_lock_(NULL), writeCounter(0),
	readCounter(0), rangeCounter(0), parties(64) {
  		this->store_ = new DataStore[parties];
  		this->indexStore_= new IndexStore[parties];
  		RetCode ret;
  		for (int i = 0; i < parties; i++) {
  		    pthread_mutex_init(&this->mutexes[i], NULL);
			this->store_[i].setDir(dir);
            this->store_[i].setParty(i);
            ret = this->store_[i].Init();
            if (ret != kSucc) {
                fprintf(stderr, "[EngineRace] : init store failed \n");
            }
			this->indexStore_[i].init(dir, i);
  		}
  		this->idx = 0;
  		this->finished = false;
  		this->timerTask = NULL;
		fprintf(stderr, "[EngineRace] : creating an engineRace instance at %s\n", 
			dir.c_str());
    }

  ~EngineRace();

  RetCode Write(const PolarString& key,
      const PolarString& value) override;

  RetCode Read(const PolarString& key,
      std::string* value) override;

  RetCode Range(const PolarString& lower,
      const PolarString& upper,
      Visitor &visitor) override;

  void resetCounter() {
    writeCounter = 0;
	readCounter = 0;
	rangeCounter = 0;
  }

 private:
    pthread_mutex_t mu_;
  FileLock* db_lock_;
  IndexStore* indexStore_;
  DataStore* store_;
  std::atomic_int writeCounter;
  std::atomic_int readCounter;
  std::atomic_int rangeCounter;
  time_t write_timer;
  time_t read_timer;
  time_t range_timer;
  int parties;
  pthread_mutex_t mutexes[64];
  Visitor* visitors[64];
  std::atomic_int idx;
  bool finished;
  std::thread* timerTask;
};

}  // namespace polar_race

// #define USE_HASH_TABLE 0

#endif  // ENGINE_EXAMPLE_ENGINE_EXAMPLE_H_
