// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <map>
#include <cstring>
#include <iostream>
#include <utility>
#include "util.h"
#include "door_plate.h"

namespace polar_race {

static const uint32_t kMaxDoorCnt = 1024 * 1024 * 32;
static const std::string metaFilePath("/meta");
static const std::string kMetaFileName("META");
static const int kMaxRangeBufCount = kMaxDoorCnt;

static bool ItemKeyMatch(const Item &item, const std::string& target) {
	
  if (target.size() != 8
      || memcmp(item.key, target.data(), 8) != 0) {
    // Conflict
    return false;
  }
  return true;
  
}

static bool ItemTryPlace(const Item &item, const std::string& target) {
  if (item.fileNo <=  0) {
    return true;
  }
  return ItemKeyMatch(item, target);
}

DoorPlate::DoorPlate(const std::string& path)
  : dir_(path),
  fd_(-1),
  items_(NULL) {
  }

RetCode DoorPlate::Init() {
  fprintf(stderr, "[DoorPlate] : init DoorPlate\n");
  bool new_create = false;
  const int map_size = kMaxDoorCnt * sizeof(Item);

  if (!FileExists(dir_)
      && 0 != mkdir(dir_.c_str(), 0755)) {
    fprintf(stderr, "[DoorPlate] : mkdir failed %s\n", dir_.c_str());
    return kIOError;
  }

  std::string metaPath = dir_ + metaFilePath;

  if (!FileExists(metaPath) && 
      0 != mkdir(metaPath.c_str(), 0755)) {
    fprintf(stderr, "[DoorPlate] : mkdir failed %s\n", metaPath.c_str());
    return kIOError;
  }

  std::string f = metaPath + "/" + kMetaFileName;
  int fd = open(f.c_str(), O_RDWR, 0644);
  if (fd < 0 && errno == ENOENT) {
    // not exist, then create
    fd = open(f.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd >= 0) {
      new_create = true;
      if (posix_fallocate(fd, 0, map_size) != 0) {
        fprintf(stderr, "[DoorPlate] : posix_fallocate failed\n");
        close(fd);
        return kIOError;
      }
    }
  }
  if (fd < 0) {
    fprintf(stderr, "[DoorPlate] : file %s open failed\n", f.c_str());
    return kIOError;
  }
  fd_ = fd;

  void* ptr = mmap(NULL, map_size, PROT_READ | PROT_WRITE,
      MAP_SHARED, fd_, 0);
  if (ptr == MAP_FAILED) {
    fprintf(stderr, "[DoorPlate] : MAP_FAILED\n");
    close(fd);
    return kIOError;
  }
  if (new_create) {
	fprintf(stderr, "[DoorPlate] : create a new mmap \n");
    memset(ptr, 0, map_size);
  }

  items_ = reinterpret_cast<Item*>(ptr);
  return kSucc;
}

DoorPlate::~DoorPlate() {
  fprintf(stderr, "[DoorPlate] : finalize door plate\n");
  if (fd_ >= 0) {
    const int map_size = kMaxDoorCnt * sizeof(Item);
    munmap(items_, map_size);
    close(fd_);
  }
}

// Very easy hash table, which deal conflict only by try the next one
int DoorPlate::CalcIndex(const std::string& key) {
  uint32_t jcnt = 0;
  int index = StrHash(key.data(), 8) % kMaxDoorCnt;
  while (!ItemTryPlace(*(items_ + index), key)
      && ++jcnt < kMaxDoorCnt) {
    index = (index + 1) % kMaxDoorCnt;
  }

  if (jcnt == kMaxDoorCnt) {
	fprintf(stderr, "[DoorPlate] : puting failed since full\n");
    return -1;
  }
  return index;
}

RetCode DoorPlate::AddOrUpdate(const std::string& key, uint64_t fileNo, uint32_t offset, 
	uint32_t vLen) {

  int index = CalcIndex(key);
  if (index < 0) {
    fprintf(stderr, "[DoorPlate] : kfull\n");
    return kFull;
  }

  Item* iptr = items_ + index;
  memcpy(iptr->key, key.data(), 8);
  iptr->fileNo = fileNo;
  iptr->offset = offset;
  iptr->vLen = vLen;
  return kSucc;
}

RetCode DoorPlate::Find(const std::string& key, uint64_t* fileNo, 
	uint32_t* offset, uint32_t* vLen) {
  int index = CalcIndex(key);
  if (index < 0
      || !ItemKeyMatch(*(items_ + index), key)) {
	fprintf(stderr, "[DoorPlate] : key not found\n");
    return kNotFound;
  }
  Item* i = items_ + index;
  (*fileNo) = i->fileNo;
  (*offset) = i->offset;
  (*vLen) = i->vLen;
  return kSucc;
}

RetCode DoorPlate::GetRangeLocation(const std::string& lower,
    const std::string& upper) {
  return kSucc;
}

}  // namespace polar_race
