// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <pthread.h>
#include <string>
#include "../include/engine.h"
#include "door_plate.h"
#include "data_store.h"
#include "util.h"
#include "bplustree.h"
#include <ctime>

namespace polar_race {

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

  explicit EngineRace(const std::string& dir)
    : mu_(PTHREAD_MUTEX_INITIALIZER),
    db_lock_(NULL), plate_(dir), tree(NULL), store_(dir), writeCounter(0),
	readCounter(0) {
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
  }

 private:
  pthread_mutex_t mu_;
  FileLock* db_lock_;
  DoorPlate plate_;
  bplus_tree*  tree;
  DataStore store_;
  uint32_t writeCounter;
  uint32_t readCounter;
  time_t write_timer;
  time_t read_timer;

};

}  // namespace polar_race

// #define USE_HASH_TABLE 0

#endif  // ENGINE_EXAMPLE_ENGINE_EXAMPLE_H_
