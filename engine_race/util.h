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

int partition(long long key);

long long strToLong(const char* key);
uint32_t wrap(uint16_t offset, uint16_t fileNo);
uint16_t unwrapOffset(uint32_t wrapper);
uint16_t unwrapFileNo(uint32_t wrapper);
void longToStr(long long key, char* ans);
void printlonglong(long long value);

// Env
int GetDirFiles(const std::string& dir, std::vector<std::string>* result, bool deleteFile);
long long GetFileLength(const std::string& file);
long long GetFileLength(char* file);
int FileAppend(int fd, const std::string& value, uint32_t vLen);
bool FileExists(const std::string& path);
uint32_t getSubFileSize(const std::string& path);

// FileLock
class FileLock  {
 public:
    FileLock() {}
    virtual ~FileLock() {}

    int fd_;
    std::string name_;

 private:
    // No copying allowed
    FileLock(const FileLock&);
    void operator=(const FileLock&);
};

int LockFile(const std::string& f, FileLock** l);
int UnlockFile(FileLock* l);


}  // namespace polar_race

#endif  // ENGINE_SIMPLE_UTIL_H_
