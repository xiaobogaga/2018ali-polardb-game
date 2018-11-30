//
// Created by tomzhu on 18-11-24.
//

#ifndef ENGINE_RACE_INDEXSTORE_H
#define ENGINE_RACE_INDEXSTORE_H

#include <stdio.h>
#include <unordered_map>
#include "util.h"
#include "../include/engine.h"
#include "../include/polar_string.h"
#include "data_store.h"
#include "LRUCache.cc"
#include "MyHashTable.h"
#include "bloom_filter.hpp"

static std::string indexPrefix("/index/");

struct Item {
    uint32_t info;
    char key[8];
};

class IndexStore {

public:

    IndexStore(std::string& dir) : dir_(dir), party_(-1), fd_(-1), items_(NULL), head_(NULL),
        size(0), infos(NULL), total(100000), newMapSize(0), start(0), table(NULL), bf(NULL), bfparameters(NULL) { }

    IndexStore() : party_(-1), fd_(-1), items_(NULL), head_(NULL), size(0), infos(NULL), total(100000), newMapSize(0)
    , start(0), table(NULL), bf(NULL), bfparameters(NULL) {}

    polar_race::RetCode init(const std::string& dir, int party);

    void setParty(int party) {
        this->party_ = party;
    }

    void add(const polar_race::PolarString& key, uint32_t info);

    void get(long long key, uint32_t* ans);

    int rangeSearch(const polar_race::PolarString& lower, const polar_race::PolarString& upper,
            polar_race::Visitor** visitor, int vSize, polar_race::DataStore* store);

    void initMaps();

    void initMaps2();

    void reAllocate();

    void finalize();

    void initInfos();

    void get2(long long key, uint32_t* ans);

    ~IndexStore();

private:
    std::string dir_;
    std::string indexPath_;
    std::string fileName_;
 //   radix_tree<std::string, long>* tree_;
    int party_;
    int fd_;
    struct Item* items_;
    struct Item* head_;
    uint32_t size;
    struct Info* infos;
 //   std::map<std::string, uint32_t >* maps;
    // art_tree* tree;
    uint32_t total;
    size_t newMapSize;
    size_t start;
    size_t sep;
    MyHashTable* table;
    // bf::basic_bloom_filter* bf;
    bloom_filter* bf;
    bloom_parameters* bfparameters;
};

#endif //ENGINE_RACE_INDEXSTORE_H
