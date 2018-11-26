//
// Created by tomzhu on 18-11-24.
//


//
// Created by tomzhu on 18-11-19.
// testing the correctness of bplustree and its performance
//

#include <iostream>
#include <map>
#include <chrono>
#include <random>
#include <vector>
#include "bplustree.h"

using namespace std;

int main() {
    char fileName[] = "/tmp/btree.bin";
    system("rm -rf /tmp/btree*");
    int block_size = 4096;
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::minstd_rand0 generator (seed);
    struct bplus_tree* tree = bplus_tree_init(fileName, block_size);
    std::map<long long, long> maps;
    std::vector<long long> vectors;
    long long min = INT64_MIN;
    long long max = INT64_MAX;
    int size = 10000000;
    fprintf(stderr, "[BplusTreeTest] : testing adding\n");
    time_t t;
    time(&t);
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
        if (i % 1000000 == 0) {
            fprintf(stderr, "[BplusTreeTest] : have writing 1000000 data, spend %f time\n", difftime(time(NULL), t));
            time(&t);
        }
    }

    fprintf(stderr, "[BplusTreeTest] : testing reading\n");
    time(&t);
    int i = 0;
    for (std::map<long long, long>::iterator it = maps.begin(); it != maps.end(); ++it) {
        long long key = it->first;
        long value = it->second;
        long ans = bplus_tree_get(tree, key);
        if (ans != value) {
            fprintf(stderr, "[Step-1] : find un matching key : %ld\n", key);
        }
        i ++;
        if (i % 1000000 == 0) {
            fprintf(stderr, "[BplusTreeTest] : have reading 1000000 data, spend %f time\n", difftime(time(NULL), t));
            time(&t);
        }
    }

    // do range search here.
    fprintf(stderr, "[BplusTreeTest] : testing range read\n");
    time(&t);
    long ans = bplus_tree_get_range(tree, min, max);
    fprintf(stderr, "[BplusTreeTest] : finish range read, spend %f time\n", difftime(time(NULL), t));
    fprintf(stderr, "[Step-1] : finished, total : %d elements == ret %ld elements\n", maps.size(), ans);
    bplus_tree_deinit(tree);

    return 0;
}