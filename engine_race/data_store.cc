// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pthread.h>
#include <vector>
#include "util.h"
#include "data_store.h"

namespace polar_race {

    static const std::string My_dataPath_("/data/");
    static const char My_kDataFilePrefix_[] = "DATA_";
    static const int My_kDataFilePrefixLen_ = 5;

    static std::string FileName(const std::string &dir, uint32_t fileno) {
        return dir + "/" + My_kDataFilePrefix_ + std::to_string(fileno);
    }

    RetCode DataStore::Init() {
        if (!FileExists(dir_)
            && 0 != mkdir(dir_.c_str(), 0755)) {
            printInfo(stderr, "[DataStore] : ERROR. %s mkdir failed\n", dir_.c_str());
            return kIOError;
        }

        this->dataFilePath = dir_ + My_dataPath_;

        if (!FileExists(this->dataFilePath) &&
            0 != mkdir(this->dataFilePath.c_str(), 0755)) {
            printInfo(stderr, "[DataStore] : ERROR. %s mkdir failed\n", this->dataFilePath.c_str());
            return kIOError;
        }

        this->dataFilePath += std::to_string(this->party) + "/";

        if (!FileExists(this->dataFilePath) &&
            0 != mkdir(this->dataFilePath.c_str(), 0755)) {
            printInfo(stderr, "[DataStore] : ERROR. %s mkdir failed\n", this->dataFilePath.c_str());
            return kIOError;
        }

        return kSucc;
    }

    int DataStore::initFD() {
        // printInfo(stderr, "[DataStore] : initFD\n");
        std::vector<std::string> files;
        GetDirFiles(this->dataFilePath, &files, false);

        cur_offset = 0;

        int fileSize = 0;
        std::string sindex;
        std::vector<std::string>::iterator it;
        for (it = files.begin(); it != files.end(); ++it) {
            if ((*it).compare(0, My_kDataFilePrefixLen_, My_kDataFilePrefix_) != 0) {
                continue;
            }
            sindex = (*it).substr(My_kDataFilePrefixLen_);
            if (std::stoul(sindex) > cur_fileNo) {
                cur_fileNo = (uint16_t) std::stoi(sindex);
            }
        }
        if (cur_fileNo != 0) fileSize = cur_fileNo;
        cur_fileNo = (cur_fileNo == 0) ? 1 : cur_fileNo;
        int len = (int) GetFileLength(FileName(this->dataFilePath, cur_fileNo));
        if (len > 0) {
            cur_offset = (uint16_t) len / (My_valuesize_);
        }
        OpenCurFile();
        return fileSize;
    }


    RetCode DataStore::Append(const std::string &value, uint16_t *fileNo, uint16_t *offset) {
        if (fd_ < 0) {
            initFD();
        }

        if (cur_offset * My_valuesize_ + My_valuesize_ > My_kSingleFileSize_) {
            // Swtich to new file
            close(fd_);
            cur_fileNo += 1;
            cur_offset = 0;
            OpenCurFile();
        }

        // Append write
        if (0 != FileAppend(fd_, value, My_valuesize_)) {
            printInfo(stderr, "[DataStore] : ERROR. append to file failed\n");
            return kIOError;
        }
        (*fileNo) = cur_fileNo;
        (*offset) = cur_offset;
        cur_offset += 1;
        return kSucc;
    }

    RetCode DataStore::Read(uint16_t fileNo, uint16_t offset, std::string *value) {
        int fd = -1;
        if (readFiles == NULL) readFiles = new std::map<uint16_t, int>();
        if (readFiles->count(fileNo) <= 0) {
            // open with o_direct to avoid system pagecache.
            // todo.
            fd = open(FileName(this->dataFilePath, fileNo).c_str(), O_RDONLY, 0644);
            if (fd < 0) {
                printInfo(stderr, "[DataStore] : ERROR. open file for read failed\n");
                return kIOError;
            }
            readFiles->insert(std::pair<uint16_t, int>(fileNo, fd));
        } else fd = readFiles->find(fileNo)->second;

        lseek(fd, ((uint32_t) offset) * My_valuesize_, SEEK_SET);
        char *pos = buf;
        uint32_t value_len = My_valuesize_;
        while (value_len > 0) {
            ssize_t r = read(fd, pos, value_len);
            if (r < 0) {
                if (errno == EINTR) {
                    continue;  // Retry
                }
                close(fd);
                printInfo(stderr, "[DataStore] : ERROR. read file failed\n");
                return kIOError;
            }
            pos += r;
            value_len -= r;
        }
        // todo
        value->assign(buf, My_valuesize_); // this make concurrent write failed.
        return kSucc;
    }

    RetCode DataStore::OpenCurFile() {
        std::string file_name = FileName(this->dataFilePath, cur_fileNo);
        int fd = open(file_name.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
        if (fd < 0) {
            printInfo(stderr, "[DataStore] : ERROR. create file failed\n");
            return kIOError;
        }
        fd_ = fd;
        return kSucc;
    }

}  // namespace polar_race
