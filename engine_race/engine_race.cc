// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <map>
#include <thread>
#include <assert.h>
#include "util.h"
#include "engine_race.h"

namespace polar_race {

static const char kLockFile[] = "LOCK";

static bool timerStop = false;

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}

// sleepint for 500s.
void startTimer() {
  double sleepTime = 1000.1;
  time_t  timer;
  time(&timer);
  while (difftime(time(NULL), timer) <= sleepTime && !timerStop) {
    sleep(1);
  }
  if (!timerStop) {
      fprintf(stderr, "[Timer] : exceed time and exist\n");
      exit(0);
  }
}

Engine::~Engine() {
}

RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
  fprintf(stderr, "[EngineRace] : open db\n");
  *eptr = NULL;
  EngineRace *engine_race = new EngineRace(name);
  engine_race->resetCounter();
  RetCode ret = kSucc;

  if (0 != LockFile(name + "/" + kLockFile, &(engine_race->db_lock_))) {
    fprintf(stderr, "[EngineRace] : lock file failed\n");
    delete engine_race;
    return kIOError;
  }

  // start a timer task
  timerStop = false;
  engine_race->timerTask = new std::thread (startTimer);

  *eptr = engine_race;
  return kSucc;
}


EngineRace::~EngineRace() {
  fprintf(stderr, "[EngineRace] : closing db\n");
  if (db_lock_) {
    UnlockFile(db_lock_);
  }
  // delete[] this->mutexes;
  delete[] this->store_;
  delete[] this->indexStore_;
  if (this->timerTask != NULL) {
      timerStop = true;
      this->timerTask->join();
      delete this->timerTask;
      this->timerTask = NULL;
  }
  for (int i = 0; i < this->parties; i++)
      pthread_mutex_destroy(&this->mutexes[i]);
}

RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  const std::string& v = value.ToString();
  long long k = strToLong(key.data());
  int party = partition(k);
  pthread_mutex_lock(&this->mutexes[party]);
  uint16_t offset = 0;
  uint16_t fileNo = 0;
  RetCode ret = store_[party].Append(v, &fileNo, &offset);
  uint32_t info = wrap(offset, fileNo);
  if (ret == kSucc) {
 // fprintf(stderr, "[EngineRace] : writing data. key : %lld, party : %d, offset : %d, fileNo : %d, info : %ld\n",
 //          k, party, offset, fileNo, wrap(offset, fileNo));

    if (writeCounter == 0) {
      time(&write_timer);
        fprintf(stderr, "[EngineRace] : writing data. key : %lld, party : %d, offset : %d, fileNo : %d, info : %ld\n",
                  k, party, offset, fileNo, wrap(offset, fileNo));
    }
    this->indexStore_[party].add(key, info);
  }

  writeCounter ++;
  if (writeCounter % 300000 == 0) {
    time_t current_time = time(NULL);
     fprintf(stderr, "[EngineRace] : have writing 300000 data, and spend %f s\n", difftime(current_time, write_timer));
    write_timer = current_time;
  }

  assert(this->writeCounter < 6400000);

  pthread_mutex_unlock(&this->mutexes[party]);
  return ret;
}

RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  int c = readCounter.load();
  readCounter ++;
  long long k = strToLong(key.data());
  int party = partition(k);
  uint16_t fileNo = -1;
  uint16_t offset = -1;
  uint32_t ans = 0;
  pthread_mutex_lock(&this->mutexes[party]);
  RetCode ret = kSucc;
  this->indexStore_[party].get(k, &ans);
  if (ans == 0) ret = kNotFound;
  else {
    offset = unwrapOffset(ans);
    fileNo = unwrapFileNo(ans);
//    if (c == 0) {
//      time(&read_timer);
//      fprintf(stderr, "[EngineRace] : reading first key : %lld... offset : %d, fileNo : %d, info : %ld\n",
//              k, offset, fileNo, wrap(offset, fileNo));
//    }
   // fprintf(stderr, "[EngineRace] : reading data. key : %lld, party : %d, offset : %d, fileNo : %d, info : %ld\n",
   //           k, party, offset, fileNo, wrap(offset, fileNo));
  }

  if (ret == kSucc) {
    value->clear();
    ret = store_[party].Read(fileNo, offset, value);
  } 
  
//  if (c == 0) {
// 	  fprintf(stderr, "[EngineRace] : reading first data finished, key : %lld, and get %lu value\n",
// 		k, value->size());
//  }

//  if (c % 300000 == 0) {
//      time_t current_time = time(NULL);
//	  fprintf(stderr, "[EngineRace] : have read 300000 data and spend %f s\n", difftime(current_time, read_timer));
//	  read_timer = current_time;
//  }
//    if (readCounter.load() % 300000) {
//        fprintf(stderr, "[EngineRace] : have read 300000 data\n");
//    }
    pthread_mutex_unlock(&this->mutexes[party]);
    return ret;
}

RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {

  // pthread_mutex_lock(&mu_);
  // 这里我们等所有线程都到达了以后，一次IO读取所有.

  this->visitors[this->idx++] = &visitor;

  // sleep 2 s
  while (readCounter.load() % 64 != 0) {
    std::this_thread::sleep_for(std::chrono::microseconds(500));
  }

  // waiting all visitors to join.
  std::this_thread::sleep_for(std::chrono::microseconds(1000));

  pthread_mutex_lock(&mu_);

  if (!this->finished) {

#ifdef USE_HASH_TABLE
    ;
#else
    long long low = lower.size() == 0 ? INT64_MIN : strToLong(lower.data());
    long long high = upper.size() == 0 ? INT64_MAX : strToLong(upper.data());
    fprintf(stderr , "[EngineRace] : range search for [%lld, %lld] with %d visitors\n",
            low, high, this->idx.load());
    if (rangeCounter == 0) {
      time(&range_timer);
    }
    // long size = bplus_tree_get_range(tree[0], low, high, visitor, store_[0]);

    // do range query.
    long size = 0;

    for (int i = 0; i < parties; i++) {
      size += this->indexStore_[i].rangeSearch(lower, upper, this->visitors, this->idx, &this->store_[i]);
    }

    if (rangeCounter == 0) {
      fprintf(stderr, "[EngineRace] : range read. [%lld, %lld) with %ld data. spend %f s\n",
              low, high, size, difftime(time(NULL), range_timer));
    }
    rangeCounter++;
#endif
  }

  this->finished = true;
  pthread_mutex_unlock(&mu_);
  return kSucc;

}

}  // namespace polar_race

