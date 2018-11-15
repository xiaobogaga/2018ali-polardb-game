// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_DATA_STORE_H_
#define ENGINE_RACE_DATA_STORE_H_
#include <string.h>
#include <unistd.h>
#include <string>
#include <pthread.h>
#include <map>
#include "include/engine.h"

namespace polar_race {

class DataStore  {
 public:
  explicit DataStore(const std::string dir)
    :  {
        fd = -1;
        dir_ = dir;
        cur_fileNo = 0;
        cur_offset = 0;
        readFiles = NULL;
    }

  ~DataStore() {
    fprintf(stderr, "[DataStore] : finalized\n");
    if (fd_ > 0) {
      close(fd_);
    }
	if (!readFiles.empty()) {
		  for (std::map<int,int>::iterator it=readFiles.begin(); it != readFiles.end(); ++it)
			close(it->second);
	}
	if (!threadBuffer.empty()) {
		for (std::map<pthread_t,char*>::iterator it=threadBuffer.begin(); it != threadBuffer.end(); ++it)
			delete[] it->second;
	}

  }

  RetCode Init();
  RetCode Read(const Location& l, std::string* value);
  RetCode Append(const std::string& value, uint32_t* fileNo, uint32_t* offset);
  RetCode initFD();

 private:
  int fd_;
  std::string dir_;
  uint32_t cur_fileNo;
  uint32_t cur_offset;
  std::map<int, int>* readFiles;
  RetCode OpenCurFile();
};

}  // namespace polar_race
#endif  // ENGINE_SIMPLE_DATA_STORE_H_
