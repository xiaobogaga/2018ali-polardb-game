// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_UTIL_H_
#define ENGINE_RACE_UTIL_H_

#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <stdint.h>
#include <pthread.h>
#include <vector>

namespace polar_race {

    static inline long long strToLong(const char *key) {
        long long ans = 0;
        for (int i = 0; i < 64; i++) {
            ans |= (((long long) ((key[i / 8]) >> (7 - i % 8)) & 1) << (63 - i));
        }
        return ans;
    }

    static inline void longToStr(long long key, char *ans) {
        memset(ans, 0, sizeof(char) * 8);
        for (int i = 0; i < 64; i++) {
            char c = (char) (((key >> i) & 1) << (i % 8));
            ans[7 - i / 8] |= c;
        }
    }

    static inline uint32_t wrap(uint16_t offset, uint16_t fileNo) {
        uint32_t ans = 0;
        for (int i = 0; i < 16; i++) {
            ans |= (((uint32_t) ((offset >> i) & 1)) << i);
            ans |= (((uint32_t) ((fileNo >> i) & 1)) << (16 + i));
        }
        return ans;
    }

    static inline uint16_t unwrapOffset(uint32_t wrapper) {
        uint16_t ans = 0;
        for (int i = 0; i < 16; i++) {
            ans |= (((wrapper >> i) & 1) << i);
        }
        return ans;
    }

    static inline uint16_t unwrapFileNo(uint32_t wrapper) {
        uint16_t ans = 0;
        for (int i = 0; i < 16; i++) {
            ans |= (((wrapper >> (i + 16)) & 1) << i);
        }
        return ans;
    }

// Env
    int GetDirFiles(const std::string &dir, std::vector<std::string> *result, bool deleteFile);

    long long GetFileLength(const std::string &file);

    long long GetFileLength(char *file);

    int FileAppend(int fd, const std::string &value, uint32_t vLen);

    bool FileExists(const std::string &path);

    uint32_t getSubFileSize(const std::string &path);

// FileLock
    class FileLock {
    public:
        FileLock() {}

        virtual ~FileLock() {}

        int fd_;
        std::string name_;

    private:
        // No copying allowed
        FileLock(const FileLock &);

        void operator=(const FileLock &);
    };

    int LockFile(const std::string &f, FileLock **l);

    int UnlockFile(FileLock *l);


}  // namespace polar_race

#endif  // ENGINE_SIMPLE_UTIL_H_
