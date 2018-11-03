// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <pthread.h>
#include <string>
#include "include/engine.h"
#include "util.h"
#include <map>

namespace polar_race {

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

  explicit EngineRace(const std::string& dir)
    : path(dir), mu_(PTHREAD_MUTEX_INITIALIZER), keyWriteFile(-1), valueWriteFile(-1),
    keyOffsetMaps(NULL), keyFileMaps(NULL), fds(NULL), fdSize(0), totalSize(0) {
    }

  ~EngineRace();

  RetCode Write(const PolarString& key,
      const PolarString& value) override;

  RetCode Read(const PolarString& key,
      std::string* value) override;

  void initFile();

  void initMaps();

  int size() override;

  /*
   * NOTICE: Implement 'Range' in quarter-final,
   *         you can skip it in preliminary.
   */
  RetCode Range(const PolarString& lower,
      const PolarString& upper,
      Visitor &visitor) override;

 private: 
  std::string path;
  pthread_mutex_t mu_;
  int keyWriteFile;
  int valueWriteFile;
  std::map<std::string, uint64_t>* keyOffsetMaps;
  std::map<std::string, uint32_t>* keyFileMaps;
  int* fds;
  int fdSize;
  long long totalSize;
};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
