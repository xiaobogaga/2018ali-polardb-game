// Copyright [2018] Alibaba Cloud All rights reserved
#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <stdio.h>
#include "util.h"

namespace polar_race {

static const int kA = 54059;  // a prime
static const int kB = 76963;  // another prime
static const int kFinish = 37;  // also prime
uint32_t StrHash(const char* s, int size) {
  uint32_t h = kFinish;
  while (size > 0) {
    h = (h * kA) ^ (s[0] * kB);
    s++;
    size--;
  }
  return h;
}

int GetDirFiles(const std::string& dir, std::vector<std::string>* result) {
  int res = 0;
  result->clear();
  DIR* d = opendir(dir.c_str());
  if (d == NULL) {
	  printf("open dir failed, getdirfiles\n");
    return errno;
  }
  struct dirent* entry;
  while ((entry = readdir(d)) != NULL) {
    if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
      continue;
    }
    res ++;
    result->push_back(entry->d_name);
  }
  closedir(d);
  return res;
}

uint32_t getSubFileSize(const std::string& path) {
  if (!FileExists(path) && 0 != mkdir(path.c_str(), 0755)) {
    printf("[EngineRace] : make dir %s failed\n", path.c_str());
    return 0;
  }
  DIR* d = opendir(path.c_str());
  if (d == NULL) {
      printf("Open dir failed\n");
      return -1;
  }
  uint32_t size = 0;
  struct dirent* entry;
  while ((entry = readdir(d)) != NULL) {
    if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
      continue;
    }
    // result->push_back(entry->d_name);
    size ++;
  }
  closedir(d);
  return size;
}

long long GetFileLength(const std::string& file) {
  struct stat stat_buf;
  long long rc = stat(file.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

int FileAppend(int fd, const std::string& value) {
  if (fd < 0) {
    return -1;
  }
  size_t value_len = value.size();
  const char* pos = value.data();
  while (value_len > 0) {
    ssize_t r = write(fd, pos, value_len);
    if (r < 0) {
      if (errno == EINTR) {
        continue;  // Retry
      }
	     printf("write data failed, fileappend\n");
      return -1;
    }
    pos += r;
    value_len -= r;
  }
  
  // flush(fd);
  return 0;
}

bool FileExists(const std::string& path) {
  return access(path.c_str(), F_OK) == 0;
}


}  // namespace polar_race
