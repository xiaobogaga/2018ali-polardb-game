//
// Created by tomzhu on 18-11-24.
//

#ifndef ENGINE_RACE_INDEXSTORE_H
#define ENGINE_RACE_INDEXSTORE_H

#include <stdio.h>
#include "util.h"
#include "../include/engine.h"
#include "../include/polar_string.h"
#include "radix_tree.hpp"
#include "data_store.h"

static std::string indexPrefix("/index/");

struct Item {
    uint32_t info;
    char key[8];
};

class IndexStore {

public:

    IndexStore(std::string& dir) : dir_(dir), party_(-1), fd_(-1), items_(NULL), size(0) { }

    IndexStore() : party_(-1), fd_(-1), items_(NULL), size(0) {}

    polar_race::RetCode init(const std::string& dir, int party);

    void setParty(int party) {
        this->party_ = party;
    }

    void add(const polar_race::PolarString& key, uint32_t info);

    void get(const polar_race::PolarString& key, uint32_t* ans);

    void rangeSearch(const polar_race::PolarString& lower, const polar_race::PolarString& upper,
            polar_race::Visitor& visitor, polar_race::DataStore& store);

    void initMaps();

    void finalize();

    ~IndexStore();

private:
    std::string dir_;
    std::string indexPath_;
    std::string fileName_;
 //   radix_tree<std::string, long>* tree_;
    int party_;
    int fd_;
    struct Item* items_;
    int size;
    std::map<std::string, long> maps;
};

#endif //ENGINE_RACE_INDEXSTORE_H
