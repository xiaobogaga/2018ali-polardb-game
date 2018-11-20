// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pthread.h>
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
static char buf[valuesize];

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
  return kSucc;
}

RetCode DataStore::initFD() {
  fprintf(stderr, "[DataStore] : initFD\n");
  std::string dataPath = dir_ + dataFilePath;
  std::vector<std::string> files;
  GetDirFiles(dataPath, &files, false);

  cur_offset = 0;

  // Get the last data file no
  std::string sindex;
  std::vector<std::string>::iterator it;
  for (it = files.begin(); it != files.end(); ++it) {
    if ((*it).compare(0, kDataFilePrefixLen, kDataFilePrefix) != 0) {
      continue;
    }
    sindex = (*it).substr(kDataFilePrefixLen);
    // fprintf(stderr, "[DataStore] : file : %s and index: %d for index %s \n", 
    //  (*it).c_str(), std::stoi(sindex), sindex.c_str());
    if (std::stoul(sindex) > cur_fileNo) {
      cur_fileNo = (uint16_t) std::stoi(sindex);
    }
  }

  cur_fileNo = (cur_fileNo == 0) ? 1 : cur_fileNo;
  // Get last data file offset
  int len = (int) GetFileLength(FileName(dataPath, cur_fileNo));
  fprintf(stderr, "[DataStore] : open current file %s and length : %d\n", 
	FileName(dataPath, cur_fileNo).c_str(), len);
  if (len > 0) {
    cur_offset = (uint16_t) len;
  }

  // Open file
  return OpenCurFile();
}



RetCode DataStore::Append(const std::string& value, uint16_t* fileNo, uint16_t* offset) {
  if (fd_ < 0) {
	  initFD();
  }
	
  if (value.size() > kSingleFileSize) {
	fprintf(stderr, "[DataStore] : value size not correct\n");
    return kInvalidArgument;
  }

  if (cur_offset + valuesize > kSingleFileSize) {
    // Swtich to new file
    close(fd_);
    cur_fileNo += 1;
    cur_offset = 0;
    OpenCurFile();
  }

  // Append write
  if (0 != FileAppend(fd_, value, valuesize)) {
    fprintf(stderr, "[DataStore] : append to file failed\n");
    return kIOError;
  }
  (*fileNo) = cur_fileNo;
  (*offset) = cur_offset;
  cur_offset += 1;
  return kSucc;
}

RetCode DataStore::Read(uint16_t fileNo, uint16_t offset, std::string* value) {
  int fd = -1;
  if (readFiles == NULL) readFiles = new std::map<int, int>();
  if (readFiles->count(fileNo) <= 0) {
	fd = open(FileName(dir_ + dataFilePath, fileNo).c_str(), O_RDONLY, 0644);
	if (fd < 0) {
		fprintf(stderr, "[DataStore] : open file for read failed\n");
		return kIOError;
	}
	readFiles->insert(std::pair<int, int> (fileNo, fd));
  } else fd = readFiles->find(fileNo)->second;
 
  if (fd < 0) fprintf(stderr, "[DataStore] : error read file\n");
  
  lseek(fd, ((uint32_t) offset) * valuesize, SEEK_SET);
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
  // todo
  value->assign(buf, valuesize);
  return kSucc;
}

RetCode DataStore::OpenCurFile() {
  std::string file_name = FileName(dir_ + dataFilePath, cur_fileNo);
  int fd = open(file_name.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
  fprintf(stderr, "[DataStore] : open a new file %s and cur_fileNo : %d\n",file_name.c_str(), cur_fileNo);
  if (fd < 0) {
    fprintf(stderr, "[DataStore] : create file failed\n");
    return kIOError;
  }
  fd_ = fd;
  return kSucc;
}

}  // namespace polar_race
