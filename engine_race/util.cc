// Copyright [2018] Alibaba Cloud All rights reserved
#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>

#include "util.h"

namespace polar_race {

static const int kA = 54059;  // a prime
static const int kB = 76963;  // another prime
static const int kFinish = 37;  // also prime
static const char buf2[4096] = "";

uint32_t StrHash(const char* s, int size) {
  uint32_t h = kFinish;
  while (size > 0) {
    h = (h * kA) ^ (s[0] * kB);
    s++;
    size--;
  }
  return h;
}

//int partition(long long key) {
//  return key & (1024 - 1);
////  return 0;
//}

// 64;
int partition(long long key) {
  long long int sep = 288230376151711743;
  int party = (key / sep - INT64_MIN / sep);
  return party == 64 ? 63 : party;
}

//int partition2(long long key) {
//  return key & (64 - 1);
//}

//long long strToLong(const char* key) {
//      long long ans = 0;
//      for (int i = 0; i < 64; i++) {
//        ans |= (((long long) ((key[i / 8]) >> (i % 8)) & 1) << i);
//        fprintf(stderr, "%d : %lld\n", i, ans);
//      }
//      return ans;
//}
//
//void longToStr(long long key, char* ans) {
//  for (int i = 0; i < 64; i++) {
//    ans[i / 8] |= (((key >> i) & 1) << (i % 8));
//  }
//}

    void printlonglong(long long value) {
      for (int i = 63; i >= 0; i --)
        fprintf(stderr, "%d", ((value >> i) & 1) );
      fprintf(stderr, "\n");
  }

    long long strToLong(const char* key) {
      long long ans = 0;
      for (int i = 0; i < 64; i++) {
        ans |= (((long long) ((key[i / 8]) >> (7 - i % 8)) & 1) << (63 - i));
     //   printlonglong(ans);
      }
      return ans;
    }

    void longToStr(long long key, char* ans) {
      memset(ans, 0, sizeof(char) * 8);
      for (int i = 0; i < 64; i++) {
      //  if (i % 8 == 0) fprintf(stderr, "\n");
        char c = (char) (((key >> i) & 1) << (i % 8));
       // fprintf(stderr, "%d", c);
        ans[7 - i / 8] |= c;
      }
      // fprintf(stderr, "\n");
    }

uint32_t wrap(uint16_t offset, uint16_t fileNo) {
      uint32_t ans = 0;
      for (int i = 0; i < 16; i++) {
        ans |= ( ((uint32_t) ((offset >> i) & 1)) << i);
        ans |= ( ((uint32_t) ((fileNo >> i) & 1)) << (16 + i));
      }
      return ans;
    }

uint16_t unwrapOffset(uint32_t wrapper) {
      uint16_t ans = 0;
      for (int i = 0; i < 16; i++) {
        ans |= (((wrapper >> i) & 1) << i);
      }
      return ans;
}

uint16_t unwrapFileNo(uint32_t wrapper) {
      uint16_t ans = 0;
      for (int i = 0; i < 16; i++) {
        ans |= (((wrapper >> (i + 16) ) & 1) << i);
      }
      return ans;
}


int GetDirFiles(const std::string& dir, std::vector<std::string>* result, bool deleteFile) {
  int res = 0;
  result->clear();
  DIR* d = opendir(dir.c_str());
  if (d == NULL) {
	  fprintf(stderr, "[Util] : open dir %s failed\n", dir.c_str());
    return errno;
  }
  struct dirent* entry;
  struct stat st;
  while ((entry = readdir(d)) != NULL) {
    stat(entry->d_name, &st);
    if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
      continue;
    }
    if (deleteFile && !S_ISDIR(st.st_mode)) remove(entry->d_name);
    res ++;
    result->push_back(entry->d_name);
  }
  closedir(d);
  return deleteFile ? 0 : res;
}

uint32_t getSubFileSize(const std::string& path) {
  if (!FileExists(path)) mkdir(path.c_str(), 0755);
  DIR* d = opendir(path.c_str());
  if (d == NULL) {
      fprintf(stderr, "[Util] : Open dir failed\n");
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

long long GetFileLength(char* file) {
  struct stat stat_buf;
  long long rc = stat(file, &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

int FileAppend(int fd, const std::string& value, uint32_t vLen) {
  if (fd < 0) {
    return -1;
  }
  size_t value_len = vLen;
  const char* pos = value.data();
  while (value_len > 0) {
    ssize_t r = write(fd, pos, value_len);
    if (r < 0) {
      if (errno == EINTR) {
        continue;  // Retry
      }
	  fprintf(stderr, "[Util] : write data failed, fileappend\n");
      return -1;
    }
    pos += r;
    value_len -= r;
  }
  /* padding.
  const char* pos2 = buf2;
  value_len = 4096 - vLen;
  while (value_len > 0) {
    ssize_t r = write(fd, pos2, value_len);
    if (r < 0) {
      if (errno == EINTR) {
        continue;  // Retry
      }
	  fprintf(stderr, "[Util] : write data failed, fileappend\n");
      return -1;
    }
    pos2 += r;
    value_len -= r;
  }
  */
  return 0;
}

bool FileExists(const std::string& path) {
  return access(path.c_str(), F_OK) == 0;
}

static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f);
}

int LockFile(const std::string& fname, FileLock** lock) {
  *lock = NULL;
  int result = 0;
  int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    result = errno;
  } else if (LockOrUnlock(fd, true) == -1) {
    result = errno;
    close(fd);
  } else {
    FileLock* my_lock = new FileLock;
    my_lock->fd_ = fd;
    my_lock->name_ = fname;
    *lock = my_lock;
  }
  return result;
}

int UnlockFile(FileLock* lock) {
  int result = 0;
  if (LockOrUnlock(lock->fd_, false) == -1) {
    result = errno;
  }
  close(lock->fd_);
  delete lock;
  return result;
}


}  // namespace polar_race
