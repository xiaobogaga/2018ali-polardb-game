// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_DATA_STORE_H_
#define ENGINE_RACE_DATA_STORE_H_
#include <string.h>
#include <unistd.h>
#include <string>
#include "include/engine.h"

namespace polar_race {

struct Location {
  Location() : file_no(0), offset(0) {
  }
  uint32_t file_no;
  uint32_t offset;
};

class DataStore  {
 public:
  explicit DataStore(const std::string dir)
    : fd_(-1), dir_(dir) {}

  ~DataStore() {
    fprintf(stderr, "[DataStore] : finalized\n");
    if (fd_ > 0) {
      close(fd_);
    }
  }

  RetCode Init();
  RetCode Read(const Location& l, std::string* value);
  RetCode Append(const std::string& value, Location* location);

 private:
  int fd_;
  std::string dir_;
  Location next_location_;

  RetCode OpenCurFile();
};

}  // namespace polar_race
#endif  // ENGINE_SIMPLE_DATA_STORE_H_