// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <map>
#include <utility>
#include <cstring>
#include <iostream>
#include "util.h"
#include "engine_race.h"

namespace polar_race {

static const std::string keyFilePath("/key");

static const std::string valueFilePath("/value");

static const uint32_t keySize = 8;

static const uint32_t valueSize = 4096;

struct Item {
  char key[keySize];
};

static std::string FileName(const std::string& dir, const std::string& prefix, uint32_t fileno) {
  return dir + prefix + "/" + std::to_string(fileno);
}

static void readOffset(int fd, uint64_t offset, std::string* value) {
  lseek(fd, offset, SEEK_SET);

  char* buf = new char[valueSize]();
  char* pos = buf;
  uint32_t value_len = valueSize;

  while (value_len > 0) {
    ssize_t r = read(fd, pos, value_len);
    if (r < 0) {
      if (errno == EINTR) {
        continue;  // Retry
      }
      printf("[EngineRace] : read file error\n");
      close(fd);
      return;
    }
    pos += r;
    value_len -= r;
  }
  *value = std::string(buf, valueSize);

  delete buf;
}
	
RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

/*
 * Complete the functions below to implement you own engine
 */

// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
	printf("[EngineRace] : open engine and create an enginerace instance\n");
  *eptr = NULL;
  EngineRace *engine_race = new EngineRace(name);
  *eptr = engine_race;
  return kSucc;
}

// 2. Close engine
EngineRace::~EngineRace() {
  printf("[EngineRace] : closing db\n");
  if (keyWriteFile > 0) {
    close(keyWriteFile);
    close(valueWriteFile);
  }
  if (keyOffsetMaps != NULL) {
    delete keyOffsetMaps;
    delete keyFileMaps;
  }
  if (fdSize > 0) {
      while (fdSize > 0) {
        close(*fds);
        fds ++;
        fdSize --;
      }
  }
}

void EngineRace::initFile() {
  if (!FileExists(path)) mkdir(path.c_str(), 0755);
  uint32_t keyFileSize = getSubFileSize(path + keyFilePath);
  uint32_t valueFileSize = getSubFileSize(path + valueFilePath);
  printf("[EngineRace] : key files : %d and value files : %d\n", keyFileSize, valueFileSize);
  std::string keyFileName(FileName(path, keyFilePath, keyFileSize));
  std::string valueFileName(FileName(path, valueFilePath, valueFileSize));
  int fd = open(keyFileName.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
  if (fd < 0) {
    printf("[EngineRace] : create key file failed\n");
    return ;
  }
  keyWriteFile = fd;
  fd = open(valueFileName.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
  if (fd < 0) {
    printf("[EngineRace] : create value file failed\n");
    return;
  }
  valueWriteFile = fd;
  printf("[EngineRace] : initFile finished\n");
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
    pthread_mutex_lock(&mu_);
  if (keyWriteFile < 0) {
    initFile();
  }
  FileAppend(keyWriteFile, key.ToString());
  FileAppend(valueWriteFile, value.ToString());
  totalSize ++;
  // printf("writing data : %ld\n", totalSize);
  pthread_mutex_unlock(&mu_);
  return kSucc;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  pthread_mutex_lock(&mu_);
  if (keyOffsetMaps == NULL) initMaps();
  RetCode code;
  std::string key2(key.ToString());
  std::map<std::string, uint64_t>::iterator ite = keyOffsetMaps->find(key2);
  if (ite == keyOffsetMaps->end()) {
    printf("[EngineRace] : oops, key not found\n");
    code = kNotFound;
  } else {
    std::map<std::string, uint32_t>::iterator ite2 = keyFileMaps->find(key2);
    readOffset(fds[ite2->second], ite->second,value);
    code = kSucc;
  }
  pthread_mutex_unlock(&mu_);
  return code;
}


void EngineRace::initMaps() {
  printf("[EngineRace] : initMaps\n");
  keyOffsetMaps = new std::map<std::string, uint64_t> ();
  keyFileMaps = new std::map<std::string, uint32_t> ();
  uint32_t length = getSubFileSize(path + keyFilePath);
  fds = new int[length];
  fdSize = length;
  int counter = 0;
  uint64_t offset = 0;
  for (uint32_t i = 0; i < length; i++) {
    offset = 0;
    long long fileLength = GetFileLength(FileName(path, keyFilePath, i));
    int fd = open(FileName(path, keyFilePath, i).c_str(), O_RDONLY, 0644);
    void* ptr = mmap(NULL, fileLength, PROT_READ, MAP_SHARED, fd, 0);
    Item* items_ = reinterpret_cast<Item*>(ptr);
    counter = (int) (fileLength / keySize);
    while (counter > 0) {
      std::string t(items_->key, keySize);
      (*keyOffsetMaps)[t] = offset;
      (*keyFileMaps)[t] = i;
      offset += valueSize;
      items_ ++;
      counter --;
    }
    fds[i] = open(FileName(path, valueFilePath, i).c_str(), O_RDONLY, 0644);
    munmap(items_, fileLength);
    close(fd);
  }
  totalSize = keyOffsetMaps->size();
  printf("[EngineRace] : initMaps finished\n");
}

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  // not supported now.
  return kSucc;
}

int EngineRace::size() {
  return totalSize;
}

}  // namespace polar_race
