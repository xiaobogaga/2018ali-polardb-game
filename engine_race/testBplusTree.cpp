//
// Created by tomzhu on 18-11-19.
// testing the correctness of bplustree and its performance
//

#include <iostream>
#include <map>
#include "bplustree.h"
#include <chrono>
#include <random>
#include <vector>

using namespace std;

int main() {
    char fileName[] = "/tmp/btree.bin";
    int block_size = 4096;
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::minstd_rand0 generator (seed);
    struct bplus_tree* tree = bplus_tree_init(fileName, block_size);
    std::map<long long, long> maps;
    std::vector<long long> vectors;
    long long min = INT64_MIN;
    long long max = INT64_MAX;
    int size = 2;
    for (int i = 0; i < size;) {
        long value = generator();
        long long temp = value;
        temp *= temp;
        if ((i & 1) == 0 && temp > 0) temp = -temp;
        if ((i & 1) == 0 && value > 0) value = -value;
        if (maps.count(temp) > 0) continue;
        maps.insert(std::pair<long long, long> (temp, value));
        bplus_tree_put(tree, temp, value);
        vectors.push_back(temp);
        i ++;
    }

    for (std::map<long long, long>::iterator it = maps.begin(); it != maps.end(); ++it) {
        long long key = it->first;
        long value = it->second;
        long ans = bplus_tree_get(tree, key);
        if (ans != value) {
            fprintf(stderr, "[Step-1] : find un matching key : %ld\n", key);
        }
    }

    // do range search here.

    bplus_tree_get_range(tree, min, max);
    fprintf(stderr, "[Step-1] : finished, total : %d elements\n", maps.size());

    std::vector<int>::size_type sz = vectors.size();
    // testing updating and adding
    for (int i = 0; i < size; ) {
        if ((i & 1) == 0) {
            // updating.
            long value = maps.find(vectors[i % sz])->second;
            long long key = maps.find(vectors[i % sz])->first;
            maps.erase(key);
            maps.insert(std::pair<long long, long>(key, value + 1));
            bplus_tree_put(tree, key, value + 1);
            i ++;
        } else {
            long value = generator();
            long long key = value;
            key = key * key * (i - key);
            if (maps.count(key) > 0) continue;
            maps.insert(std::pair<long long, long> (key, value));
            bplus_tree_put(tree, key, value);
            vectors.push_back(key);
            i ++;
        }
    }

    for (std::map<long long, long>::iterator it = maps.begin(); it != maps.end(); ++it) {
        long long key = it->first;
        long value = it->second;
        long ans = bplus_tree_get(tree, key);
        if (ans != value) {
            fprintf(stderr, "[Step-2] : find un matching key : %ld\n", key);
        }
    }

    bplus_tree_get_range(tree, min, max);
    fprintf(stderr, "[Step-2] : finished, total : %d elements\n", maps.size());

    // now testing closing.
    tree = bplus_tree_init(fileName, block_size);
    for (std::map<long long, long>::iterator it = maps.begin(); it != maps.end(); ++it) {
        long long key = it->first;
        long value = it->second;
        long ans = bplus_tree_get(tree, key);
        if (ans != value) {
            fprintf(stderr, "[Step-3] : find un matching key : %ld\n", key);
        }
    }

    bplus_tree_get_range(tree, min, max);
    fprintf(stderr, "[Step-3] : finished, total : %d elements\n", maps.size());

    sz = vectors.size();

    // testing updating and adding
    for (int i = 0; i < size; ) {
        if ((i & 1) == 0) {
            // updating.
            long value = maps.find(vectors[i % sz])->second;
            long long key = maps.find(vectors[i % sz])->first;
            maps.erase(key);
            maps.insert(std::pair<long long, long>(key, value + 1));
            bplus_tree_put(tree, key, value + 1);
            i ++;
        } else {
            long value = generator();
            long long key = value;
            key = key * key * (i - key);
            if (maps.count(key) > 0) continue;
            maps.insert(std::pair<long long, long> (key, value));
            bplus_tree_put(tree, key, value);
            vectors.push_back(key);
            i ++;
        }
    }

    for (std::map<long long, long>::iterator it = maps.begin(); it != maps.end(); ++it) {
        long long key = it->first;
        long value = it->second;
        long ans = bplus_tree_get(tree, key);
        if (ans != value) {
            fprintf(stderr, "[Step-4] : find un matching key : %ld\n", key);
        }
    }

    bplus_tree_get_range(tree, min, max);
    fprintf(stderr, "[Step-4] : finished, total : %d elements\n", maps.size());

   // bplus_tree_deinit(tree);
  //  flush_index(tree);
    tree = bplus_tree_init(fileName, block_size);

    for (std::map<long long, long>::iterator it = maps.begin(); it != maps.end(); ++it) {
        long long key = it->first;
        long value = it->second;
        long ans = bplus_tree_get(tree, key);
        if (ans != value) {
            fprintf(stderr, "[Step-5] : find un matching key : %ld\n", key);
        }
    }

    bplus_tree_get_range(tree, min, max);
    fprintf(stderr, "[Step-5] : finished, total : %d elements\n", maps.size());

    sz = vectors.size();

    // testing updating and adding
    for (int i = 0; i < size; ) {
        if ((i & 1) == 0) {
            // updating.
            long value = maps.find(vectors[i % sz])->second;
            long long key = maps.find(vectors[i % sz])->first;
            maps.erase(key);
            maps.insert(std::pair<long long, long>(key, value + 1));
            bplus_tree_put(tree, key, value + 1);
            i ++;
        } else {
            long value = generator();
            long long key = value;
            key = key * key * (i - key);
            if (maps.count(key) > 0) continue;
            maps.insert(std::pair<long long, long> (key, value));
            bplus_tree_put(tree, key, value);
            vectors.push_back(key);
            i ++;
        }
    }

    for (std::map<long long, long>::iterator it = maps.begin(); it != maps.end(); ++it) {
        long long key = it->first;
        long value = it->second;
        long ans = bplus_tree_get(tree, key);
        if (ans != value) {
            fprintf(stderr, "[Step-6] : find un matching key : %ld\n", key);
        }
    }

    bplus_tree_get_range(tree, min, max);
    fprintf(stderr, "[Step-6] : finished, total : %d elements\n", maps.size());

    return 0;
}

int main2() {
    char fileName[] = "/tmp/btree.bin";
    int block_size = 4096;
    struct bplus_tree* tree = bplus_tree_init(fileName, block_size);
    long long a1 = -1028409;
    long long a2 = 278943279;
    long long key = (a1) * (a2);
    fprintf(stderr, "puting %lld\n", key);
    bplus_tree_put(tree, key, 1);
    fprintf(stderr, "getting %lld\n", bplus_tree_get(tree, key));
}


