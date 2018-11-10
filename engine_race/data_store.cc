// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include "util.h"
#include "data_store.h"

namespace polar_race {

static const std::string dataFilePath("/data");
static const char kDataFilePrefix[] = "DATA_";
static const int kDataFilePrefixLen = 5;
static const int kSingleFileSize = 1024 * 1024 * 256;
static const int keysize = 8;
static const int valuesize = 4096;

static std::string FileName(const std::string &dir, uint32_t fileno) {
  return dir + "/" + kDataFilePrefix + std::to_string(fileno);
}

RetCode DataStore::Init() {
  fprintf(stderr, "[DataStore] : init data_store\n");
  if (!FileExists(dir_)
      && 0 != mkdir(dir_.c_str(), 0755)) {
    fprintf(stderr, "[DataStore] : %s mkdir failed\n", dir_.c_str());
    return kIOError;
  }

  std::string dataPath = dir_ + dataFilePath;

  if (!FileExists(dataPath) && 
      0 != mkdir( dataPath.c_str(), 0755)) {
      fprintf(stderr, "[DataStore] : %s mkdir failed\n", dataPath.c_str());
      return kIOError;
  }
}

void DataStore::initFD() {
  fprintf(stderr, "[DataStore] : initFD\n");
  std::string dataPath = dir_ + dataFilePath;
  std::vector<std::string> files;
  GetDirFiles(dataPath, &files, false);

  uint32_t last_no = 0;
  uint32_t cur_offset = 0;

  // Get the last data file no
  std::string sindex;
  std::vector<std::string>::iterator it;
  for (it = files.begin(); it != files.end(); ++it) {
    if ((*it).compare(0, kDataFilePrefixLen, kDataFilePrefix) != 0) {
      continue;
    }
    sindex = (*it).substr(kDataFilePrefixLen);
    // fprintf(stderr, "[DataStore] : file : %s and index: %d for index %s \n", 
      (*it).c_str(), std::stoi(sindex), sindex.c_str());
    if (std::stoul(sindex) > last_no) {
      last_no = std::stoi(sindex);
    }
  }

  // Get last data file offset
  int len = (int) GetFileLength(FileName(dataPath, last_no));
  fprintf(stderr, "[DataStore] : open current file %s and length : %d\n", 
	FileName(dataPath, last_no).c_str(), len);
  if (len > 0) {
    cur_offset = len;
  }

  next_location_.file_no = last_no;
  next_location_.offset = cur_offset;

  // Open file
  return OpenCurFile();
}



RetCode DataStore::Append(const std::string& value, Location* location) {
  if (fd_ < 0) {
	  initFD();
  }
	
  if (value.size() > kSingleFileSize) {
    return kInvalidArgument;
  }

  if (next_location_.offset + value.size() > kSingleFileSize) {
    // Swtich to new file
    close(fd_);
    next_location_.file_no += 1;
    next_location_.offset = 0;
    OpenCurFile();
  }

  // Append write
  if (0 != FileAppend(fd_, value)) {
    fprintf(stderr, "[DataStore] : append to file failed\n");
    return kIOError;
  }
  location->file_no = next_location_.file_no;
  location->offset = next_location_.offset;

  next_location_.offset += location->len;
  return kSucc;
}

RetCode DataStore::Read(const Location& l, std::string* value) {
  int fd = -1;
  if (readFiles.count(l.fileNo) <= 0) {
	fd = open(FileName(dir_ + dataFilePath, l.file_no).c_str(), O_RDONLY, 0644);
	if (fd < 0) {
		fprintf(stderr, "[DataStore] : open file for read failed\n");
		return kIOError;
	}
	readFiles.insert(std::pair<int, int> (l.fileNo, fd));
  } else fd = readFiles.find(l.fileNo)->second;
 
  lseek(fd, l.offset, SEEK_SET);
  char* buf = NULL;
  int tid = gettid();
  if (threadBuffer.count(tid) <= 0) {
	buf = new char[valuesize]();
	threadBuffer.insert(std::pair<int, char*> (tid, buf));
  } else buf = threadBuffer.find(tid)->second;
  char* pos = buf;
  uint32_t value_len = valuesize;
  while (value_len > 0) {
    ssize_t r = read(fd, pos, value_len);
    if (r < 0) {
      if (errno == EINTR) {
        continue;  // Retry
      }
      close(fd);
	  fprintf(stderr, "[DataStore] : read file failed\n");
      return kIOError;
    }
    pos += r;
    value_len -= r;
  }
  *value = std::string(buf, valuesize);
  return kSucc;
}

RetCode DataStore::OpenCurFile() {
  std::string file_name = FileName(dir_ + dataFilePath, next_location_.file_no);
  int fd = open(file_name.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
  fprintf(stderr, "[DataStore] : open a new file %s\n",file_name.c_str());
  if (fd < 0) {
    fprintf(stderr, "[DataStore] : create file failed\n");
    return kIOError;
  }
  fd_ = fd;
  return kSucc;
}

}  // namespace polar_race
