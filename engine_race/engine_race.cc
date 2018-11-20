// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <map>
#include "util.h"
#include "engine_race.h"
#include "bplustree.h"

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
  RetCode ret = kSucc;
#ifdef USE_HASH_TABLE
  ret = engine_race->plate_.Init();
#else
  engine_race->tree = bplus_tree_init( (name + "/btree.bin").c_str(), 4096);
#endif

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
#ifdef USE_HASH_TABLE
  ;
#else
  bplus_tree_deinit(tree);
#endif
  if (db_lock_) {
    UnlockFile(db_lock_);
  }
}

RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  pthread_mutex_lock(&mu_);
  const std::string& v = value.ToString();
  const std::string& k = key.ToString();
  if (writeCounter == 0) {
    fprintf(stderr, "[EngineRace] : writing data. key length : %lu, value length : %lu\n",
        k.size(), v.size());
  }
  
  uint16_t offset = 0;
  uint16_t fileNo = 0;
  RetCode ret = store_.Append(v, &fileNo, &offset);
  if (ret == kSucc) {

#ifdef USE_HASH_TABLE
    ret = plate_.AddOrUpdate(k, fileNo, offset);
#else
    bplus_tree_put(tree, strToLong(key.data()), wrap(offset, fileNo));
#endif

  }
  
  if (writeCounter == 0) {
	  fprintf(stderr, "[EngineRace] : writing first data finished. key length : %lu, value length : %lu\n",
        k.size(), v.size());
  }
  writeCounter ++;
  if (writeCounter % 390000 == 0) {
    fprintf(stderr, "[EngineRace] : have writing 390000 data\n");
  }
  pthread_mutex_unlock(&mu_);
  return ret;
}

RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  pthread_mutex_lock(&mu_);
  const std::string& k = key.ToString();
  if (readCounter == 0) {
	  fprintf(stderr, "[EngineRace] : reading first data, key : %lu, and get %lu value\n",
		k.size(), value->size());
  }
  
  uint16_t fileNo = 0;
  uint16_t offset = 0;

  RetCode ret = kSucc;
#ifdef USE_HASH_TABLE
  ret = plate_.Find(k, &fileNo, &offset);
#else
  long info = bplus_tree_get(tree, strToLong(key.data()));
  if (info < 0) ret = kNotFound;
  else {
    offset = unwrapOffset(info);
    fileNo = unwrapFileNo(info);
  }
#endif

  if (ret == kSucc) {
    value->clear();
    ret = store_.Read(fileNo, offset, value);
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
  return ret;
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  pthread_mutex_lock(&mu_);

#ifdef USE_HASH_TABLE
  ;
#else
  long long low = strToLong(lower.data());
  long long high = strToLong(upper.data());
  bplus_tree_get_range(tree, low, high, visitor, store_);
#endif

  pthread_mutex_unlock(&mu_);
  return kSucc;
}

}  // namespace polar_race

