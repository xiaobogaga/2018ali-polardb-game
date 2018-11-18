// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <pthread.h>
#include <string>
#include "include/engine.h"
#include "util.h"

namespace polar_race {

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

  explicit EngineRace(const std::string& dir)
    : mu_(PTHREAD_MUTEX_INITIALIZER),
    db_lock_(NULL), writeCounter(0),
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

  int size() override;

 private:
  pthread_mutex_t mu_;
  FileLock* db_lock_;
  uint32_t writeCounter;
  uint32_t readCounter;
};

}  // namespace polar_race

#endif  // ENGINE_EXAMPLE_ENGINE_EXAMPLE_H_
