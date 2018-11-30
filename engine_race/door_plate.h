// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_DOOR_PLATE_H_
#define ENGINE_RACE_DOOR_PLATE_H_
#include <string.h>
#include <stdint.h>
#include <map>
#include <string>
#include "../include/engine.h"
#include "data_store.h"

namespace polar_race {

static const uint32_t kMaxKeyLen = 8;

struct Item {
  Item()  {
  }
  char key[kMaxKeyLen];
  uint32_t info;
};

// Hash index for key
class DoorPlate  {
 public:
    explicit DoorPlate(const std::string& path);
    ~DoorPlate();
    DoorPlate() {
      this->fd_ = -1;
      this->items_ = NULL;
    }

    void setDir(const std::string& path) {
      this->dir_ = path;
    }

    RetCode Init();

    RetCode AddOrUpdate(const std::string& key, uint16_t fileNo, uint16_t offset);

    RetCode Find(const std::string& key, uint16_t * fileNo, uint16_t * offset);

    RetCode GetRangeLocation(const std::string& lower, const std::string& upper);

 private:
    std::string dir_;
    int fd_;
    Item *items_;

    int CalcIndex(const std::string& key);
};

}  // namespace polar_race

#endif  // ENGINE_EXAMPLE_DOOR_PLATE_H_
