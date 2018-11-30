//
// Created by tomzhu on 18-11-28.
//

#ifndef MY_HASH_TABLE
#define MY_HASH_TABLE

#include <stdio.h>
#include "util.h"
#include "../include/engine.h"
#include "../include/polar_string.h"
#include "bloom_filter.hpp"

struct Info {
    long long key;
    uint32_t info;
};

class MyHashTable {
public:

    MyHashTable() : datas(NULL), total(1048576), bf(NULL), bfparameters(NULL) {
        this->datas = (struct Info*) malloc(sizeof(struct Info) * total);
    }

    ~MyHashTable() {
        if (this->datas != NULL) {
            free(this->datas);
            this->datas = NULL;
        }
        if (this->bf != NULL) {
            delete this->bf;
            this->bf = NULL;
            delete this->bfparameters;
            this->bfparameters = NULL;
        }
    }

    inline int hash(long long key) {
        return key ^ (key >> 32);
    }

    void add(long long key, uint32_t info) {
        if (bf == NULL) {
            this->bfparameters = new bloom_parameters();
            this->bfparameters->projected_element_count = 1000000;
            this->bfparameters->false_positive_probability = 0.0001; // 1 in 10000
            this->bfparameters->random_seed = 0xA5A5A5A5;
            if (!this->bfparameters)
            {
                fprintf(stderr, "[MyHashTable] : Invalid set of bloom filter parameters!\n");
                return;
            }
            this->bfparameters->compute_optimal_parameters();
            this->bf = new bloom_filter(*this->bfparameters);
        }
        int loc = hash(key) & (total - 1);
        while (this->datas[loc].info != 0 && this->datas[loc].key != key) {
            loc = (loc + 1) & (total - 1);
        }
        if (this->datas[loc].key == key) {
            this->datas[loc].info = info; // replacing.
            if (key == 0) bf->insert(key);
        } else {
            this->datas[loc].info = info;
            this->datas[loc].key = key;
            bf->insert(key);
        }
    }

    uint32_t get(long long key) {
        if (!bf->contains(key)) return 0;
        else {
            int loc = hash(key) & (total - 1);
            while (this->datas[loc].info != 0) {
                if (this->datas[loc].key == key) return this->datas[loc].info;
                loc = (loc + 1) & (total - 1);
            }
            // false, positive.
            return 0;
        }
    }


private:

    struct Info* datas;
    int total;
    bloom_filter* bf;
    bloom_parameters* bfparameters;
};

#endif
