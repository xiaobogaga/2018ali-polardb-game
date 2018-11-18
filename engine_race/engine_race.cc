// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <map>
#include "util.h"
#include "engine_race.h"

namespace polar_race {

static const char kLockFile[] = "LOCK";

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
  fprintf(stderr, "[EngineRace] : open db\n");
  *eptr = NULL;
  EngineRace *engine_race = new EngineRace(name);
  engine_race->resetCounter();

  if (0 != LockFile(name + "/" + kLockFile, &(engine_race->db_lock_))) {
    fprintf(stderr, "[EngineRace] : lock file failed\n");
    delete engine_race;
    return kIOError;
  }

  *eptr = engine_race;
  return kSucc;
}

EngineRace::~EngineRace() {
  fprintf(stderr, "[EngineRace] : closing db\n");
  if (db_lock_) {
    UnlockFile(db_lock_);
  }
}

RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  pthread_mutex_lock(&mu_);
  const std::string v(value.data_, value.size_);
  const std::string& k = key.ToString();
  // if (writeCounter == 0) {
    fprintf(stderr, "[EngineRace] : writing data. key length : %lu, value length : %lu\n",
        k.size(), v.size());
  // }
  
  if (writeCounter == 0) {
	  fprintf(stderr, "[EngineRace] : writing first data finished. key length : %lu, value length : %lu\n",
        k.size(), v.size());
  }
  writeCounter ++;
  if (writeCounter % 390000 == 0) {
    fprintf(stderr, "[EngineRace] : have writing 390000 data\n");
  }
  pthread_mutex_unlock(&mu_);
  return kSucc;
}

RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  pthread_mutex_lock(&mu_);
  const std::string& k = key.ToString();
  if (readCounter == 0) {
	  fprintf(stderr, "[EngineRace] : reading first data, key : %lu, and get %lu value\n",
		k.size(), value->size());
  }
  
  if (readCounter == 0) {
	  fprintf(stderr, "[EngineRace] : reading first data finished, key : %lu, and get %lu value\n",
		k.size(), value->size());
  }
  readCounter ++;
  if (readCounter % 390000 == 0) {
	  fprintf(stderr, "[EngineRace] : have read 390000 data\n");
  }
  pthread_mutex_unlock(&mu_);
  return kSucc;
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  pthread_mutex_lock(&mu_);

  pthread_mutex_unlock(&mu_);
  return kSucc;
}

int EngineRace::size() {
  return 0;
}

}  // namespace polar_race

