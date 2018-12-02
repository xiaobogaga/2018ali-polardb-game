//
// Created by tomzhu on 18-11-24.
//

#ifndef MY_INDEX_STORE_
#define MY_INDEX_STORE_

#include <stdio.h>
#include "util.h"
#include "../include/engine.h"
#include "../include/polar_string.h"
#include "MyHashTable.h"
#include "bloom_filter.hpp"
#include "config.h"

namespace polar_race {

    class IndexStore {

    public:

        IndexStore(std::string &dir) : dir_(dir), party_(-1), fd_(-1), items_(NULL), head_(NULL),
                                       size(0), infos(NULL), total(infoArraySize), newMapSize(0), start(0), sep(0),
                                       table(NULL), bf(NULL), bfparameters(NULL) {}

        IndexStore() : party_(-1), fd_(-1), items_(NULL), head_(NULL), size(0), infos(NULL), total(infoArraySize),
                       newMapSize(0), start(0), sep(0), table(NULL), bf(NULL), bfparameters(NULL) {}

        RetCode init(const std::string &dir, int party);

        void setParty(int party) {
            this->party_ = party;
        }

        void add(const PolarString &key, uint32_t info);

        void get(long long key, uint32_t *ans);

        int getInfoAt(int i, long long* k, uint32_t* info);

        void initMaps();

        void initMaps2();

        void reAllocate();

        void finalize();

        void initInfos();

        void get2(long long key, uint32_t *ans);

        struct Info* getInfo() {
            return this->infos;
        }

        uint32_t getSize() {
            return this->size;
        }

        ~IndexStore();

    private:
        std::string dir_;
        std::string indexPath_;
        std::string fileName_;
        int party_;
        int fd_;
        struct Item *items_;
        struct Item *head_;
        uint32_t size;
        struct Info *infos;
        uint32_t total;
        size_t newMapSize;
        size_t start;
        size_t sep;
        MyHashTable *table;
        bloom_filter *bf;
        bloom_parameters *bfparameters;
    };

}

#endif