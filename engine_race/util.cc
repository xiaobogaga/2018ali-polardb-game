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
