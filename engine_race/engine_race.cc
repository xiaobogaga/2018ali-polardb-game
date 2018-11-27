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
  ;
#endif

  if (ret != kSucc) {
    delete engine_race;
    fprintf(stderr, "[EngineRace] : init plate failed \n");
    return ret;
  }

  for (int i = 0; i < engine_race->parties; i++) {
    engine_race->store_[i].setParty(i);
    ret = engine_race->store_[i].Init();
    if (ret != kSucc) {
      fprintf(stderr, "[EngineRace] : init store failed \n");
      delete engine_race;
      return ret;
    }
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
//  for (int i = 0; i < this->parties; i++) {
//    bplus_tree_deinit(this->tree[i]);
//  }
;
#endif
  if (db_lock_) {
    UnlockFile(db_lock_);
  }
  // delete[] this->mutexes;
  delete[] this->store_;
  delete[] this->indexStore_;
}

RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  const std::string& v = value.ToString();
  long long k = strToLong(key.data());
  int party = partition(k);
 // this->mutexes[party].lock();
  pthread_mutex_lock(&mu_);
  uint16_t offset = 0;
  uint16_t fileNo = 0;
  RetCode ret = store_[party].Append(v, &fileNo, &offset);
  uint32_t info = wrap(offset, fileNo);
  if (ret == kSucc) {

#ifdef USE_HASH_TABLE
    ret = plate_.AddOrUpdate(k, fileNo, offset);
#else
    ;
    // bplus_tree_put(tree[party], k, );
#endif
  //  fprintf(stderr, "[EngineRace] : writing data. key : %lld, party : %d, offset : %d, fileNo : %d, info : %ld\n",
  //          k, party, offset, fileNo, wrap(offset, fileNo));

    if (writeCounter == 0) {
    //  time(&write_timer);
    }
    this->indexStore_[party].add(key, info);
  }

  writeCounter ++;
 // if (writeCounter % 300000 == 0) {
   // time_t current_time = time(NULL);
   //  fprintf(stderr, "[EngineRace] : have writing 300000 data, and spend %f s\n", difftime(current_time, write_timer));
   // write_timer = current_time;
 // }
//  this->mutexes[party].unlock();
  pthread_mutex_unlock(&mu_);

  return ret;
}

RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  long long k = strToLong(key.data());
  int party = partition(k);
  uint16_t fileNo = -1;
  uint16_t offset = -1;
  uint32_t ans = 0;
  pthread_mutex_lock(&mu_);
  // this->mutexes[party].lock();

  RetCode ret = kSucc;
#ifdef USE_HASH_TABLE
  ret = plate_.Find(k, &fileNo, &offset);
#else
  this->indexStore_[party].get(key, &ans);
  if (ans == 0) ret = kNotFound;
  else {
    offset = unwrapOffset(ans);
    fileNo = unwrapFileNo(ans);
    if (readCounter == 0) {
      time(&read_timer);
  //    fprintf(stderr, "[EngineRace] : reading first... offset : %d, fileNo : %d, info : %d\n",
  //            offset, fileNo, wrap(offset, fileNo));
    }
  //  fprintf(stderr, "[EngineRace] : reading data. key : %lld, party : %d, offset : %d, fileNo : %d, info : %ld\n",
  //            k, party, offset, fileNo, wrap(offset, fileNo));
  }
#endif

  if (ret == kSucc) {
    value->clear();
    ret = store_[party].Read(fileNo, offset, value);
  } 
  
 // if (readCounter == 0) {
 //	  fprintf(stderr, "[EngineRace] : reading first data finished, key : %lu, and get %lu value\n",
 //		key.size(), value->size());
 // }
  readCounter ++;
  if (readCounter % 300000 == 0) {
      time_t current_time = time(NULL);
	  fprintf(stderr, "[EngineRace] : have read 300000 data and spend %f s\n", difftime(current_time, read_timer));
	  read_timer = current_time;
  }

  // this->mutexes[party].unlock();
    pthread_mutex_unlock(&mu_);
    return ret;
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {

  pthread_mutex_lock(&mu_);

#ifdef USE_HASH_TABLE
  ;
#else
  long long low = lower.size() == 0 ? INT64_MIN : strToLong(lower.data());
  long long high = upper.size() == 0 ? INT64_MAX : strToLong(upper.data());
  if (rangeCounter == 0) {
    time(&range_timer);
  }
  // long size = bplus_tree_get_range(tree[0], low, high, visitor, store_[0]);

  // do range query.
    long size = 0;

    for (int i = 0; i < parties; i++) {
        size += this->indexStore_[i].rangeSearch(lower, upper, &visitor, &this->store_[i]);
  }

  if (rangeCounter == 0) {
      fprintf(stderr, "[EngineRace] : range read. [%lld, %lld) with %ld data. spend %f s\n",
              low, high, size, difftime(time(NULL), range_timer));
  }
  rangeCounter ++;
#endif

  pthread_mutex_unlock(&mu_);
  return kSucc;

}

}  // namespace polar_race

