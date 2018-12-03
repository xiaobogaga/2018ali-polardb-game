// Copyright [2018] Alibaba Cloud All rights reserved

#ifndef ENGINE_RACE_DATA_STORE_H_
#define ENGINE_RACE_DATA_STORE_H_

#include <string.h>
#include <unistd.h>
#include <string>
#include <pthread.h>
#include <map>
#include "../include/engine.h"
#include "config.h"

namespace polar_race {

    class DataStore {
    public:
        explicit DataStore(const std::string dir) {
            fd_ = -1;
            dir_ = dir;
            cur_fileNo = 0;
            cur_offset = 0;
            readFiles = NULL;
        }

        DataStore() {
            fd_ = -1;
            cur_fileNo = 0;
            cur_offset = 0;
            readFiles = NULL;
        }

        void setDir(const std::string dir) {
            this->dir_ = dir;
        }

        ~DataStore() {
            // printInfo(stderr, "[DataStore-%d] : finilized\n", party);
            if (fd_ >= 0) {
                close(fd_);
            }
            if (readFiles != NULL && !readFiles->empty()) {
                for (std::map<uint16_t, int>::iterator it = readFiles->begin(); it != readFiles->end(); ++it)
                    close(it->second);
                delete readFiles;
            }
        }

        void setParty(int party) {
            this->party = party;
        }

        RetCode Init();

        RetCode Read(uint16_t fileNo, uint16_t offset, std::string *value);

        RetCode Append(const std::string &value, uint16_t *fileNo, uint16_t *offset);

        std::string getDataFilePath() {
            return this->dataFilePath;
        }

        int initFD();

    private:
        int fd_;
        std::string dir_;
        std::string dataFilePath;
        uint16_t cur_fileNo;
        uint16_t cur_offset;
        std::map<uint16_t, int> *readFiles;
        RetCode OpenCurFile();
        int party;
        // todo
        char buf[My_valuesize_]; // for concurrent read error.
    };

}  // namespace polar_race
#endif  // ENGINE_SIMPLE_DATA_STORE_H_
