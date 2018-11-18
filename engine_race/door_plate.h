// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_DOOR_PLATE_H_
#define ENGINE_RACE_DOOR_PLATE_H_
#include <string.h>
#include <stdint.h>
#include <map>
#include <string>
#include "include/engine.h"
#include "data_store.h"

namespace polar_race {

static const uint32_t kMaxKeyLen = 8;

struct Item {
  Item()  {
  }
  char key[kMaxKeyLen];
  uint64_t fileNo;
  uint32_t offset;
  uint32_t vLen;
};

// Hash index for key
class DoorPlate  {
 public:
    explicit DoorPlate(const std::string& path);
    ~DoorPlate();

    RetCode Init();

    RetCode AddOrUpdate(const std::string& key, uint64_t fileNo, 
		uint32_t offset, uint32_t vLen);

    RetCode Find(const std::string& key, uint64_t* fileNo, uint32_t* offset, uint32_t* vLen);

    RetCode GetRangeLocation(const std::string& lower, const std::string& upper);

 private:
    std::string dir_;
    int fd_;
    Item *items_;

    int CalcIndex(const std::string& key);
};

}  // namespace polar_race

#endif  // ENGINE_EXAMPLE_DOOR_PLATE_H_
