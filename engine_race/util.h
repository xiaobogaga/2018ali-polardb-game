// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_UTIL_H_
#define ENGINE_RACE_UTIL_H_
#include <stdint.h>
#include <pthread.h>
#include <string>
#include <vector>

namespace polar_race {

// Hash
uint32_t StrHash(const char* s, int size);

// Env
int GetDirFiles(const std::string& dir, std::vector<std::string>* result);
long long GetFileLength(const std::string& file);
int FileAppend(int fd, const std::string& value);
bool FileExists(const std::string& path);
uint32_t getSubFileSize(const std::string& path);

}  // namespace polar_race

#endif  // ENGINE_SIMPLE_UTIL_H_
