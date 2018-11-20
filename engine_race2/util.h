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

long long strToLong(char* key) {
    long long ans = 0;
    for (int i = 0; i < 64; i++) {
        ans |= ((long) ( (unsigned char) (key[i / 8] >> (i % 8)) & 1)) << i;
    }
    return ans;
}


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