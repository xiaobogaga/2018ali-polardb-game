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
  RetCode ret = engine_race->plate_.Init();
  if (ret != kSucc) {
    delete engine_race;
    fprintf(stderr, "[EngineRace] : init plate failed \n");
    return ret;
  }
  ret = engine_race->store_.Init();
  if (ret != kSucc) {
    fprintf(stderr, "[EngineRace] : init store failed \n");
    delete engine_race;
    return ret;
  }

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
  std::string& v = value.ToString();
  std::string& k = key.ToString();
  if (counter == 0) {
    fprintf(stderr, "writing first data. key : %s and length : %ld, value length : %ld\n",
        k.c_str(), key.size(), v.size());
  }
  counter ++;
  if (counter % 390000 == 0) {
    fprintf(stderr, "have writing 390000 data\n");
  }
  uint32_t offset = 0;
  uint32_t fileNo = 0;
  RetCode ret = store_.Append(v, &fileNo, &offset);
  if (ret == kSucc) {
    ret = plate_.AddOrUpdate(k, fileNo, offset);
  } 
  pthread_mutex_unlock(&mu_);
  return ret;
}

RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  pthread_mutex_lock(&mu_);
  std::string& k = key.ToString();
  fprintf(stderr, "reading data %d\n", counter);
  uint32_t fileNo = 0;
  uint32_t offset = 0;
  RetCode ret = plate_.Find(k, &fileNo, &offset);
  if (ret == kSucc) {
    value->clear();
    ret = store_.Read(fileNo, offset, value);
  } 
  if (counter == 0) {
	  fprintf(stderr, "reading first data, key : %s, and get %ld value\n",
		k.c_str(), value->size());
  }
  counter ++;
  if (counter % 390000 == 0) {
	  fprintf(stderr, "have read 390000 data\n");
  }
  pthread_mutex_unlock(&mu_);
  return ret;
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

