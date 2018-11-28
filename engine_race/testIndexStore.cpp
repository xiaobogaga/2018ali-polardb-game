//
// Created by tomzhu on 18-11-26.
//

#include <stdio.h>
#include "indexstore.h"
#include "engine.h"
#include "polar_string.h"
#include "util.h"
#include <random>
#include <chrono>
#include <map>


using polar_race::PolarString;

PolarString generateAKey(std::default_random_engine* random) {
    char* buf = new char[8];
    for (int i = 0; i < 8; i++) {
        buf[i] = (*random)() % 256;
    }
    return PolarString(buf, 8);
}

PolarString generateValue(std::default_random_engine* random) {
    size_t size = 4096;
    char* buf = new char[size];
    for (size_t i = 0; i < size; i++) {
        buf[i] = (*random)() % 256;
    }
    return PolarString(buf, size);
}

int main() {
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine random;
    random.seed(seed);
    std::string path("/tmp/test_dump/");
    system("rm -rf /tmp/test_dump/*");
    int parties = 64;
    time_t timer;
    time(&timer);
    IndexStore indexStore[parties];
    for (int i = 0; i < parties; i++) indexStore[i].init(path, i);
    // test how many times it would consume to save 64000000 items and fetch almost 64000000 items.
    long long total = 4;
    char buf[8];
    std::map<long long, long> maps;
    std::vector<long long> keys;
    for (int i = 0; i < total; ) {
        // setting duplicated key.
        long long key;
        if ((i & 1) == 1) {
            key = keys.at(random() % keys.size());
            long value = random();
            if (value == 0) value += 1;
            maps.erase(key);
            polar_race::longToStr(key, buf);
            int party = polar_race::partition(key);
            indexStore[party].add(PolarString(buf, 8), value);
            maps.insert(std::pair<long long, long>(key, value));
        } else {
            key = random() * random();
            long value = random();
            if (value == 0) value ++;
            polar_race::longToStr(key, buf);
            int party = polar_race::partition(key);
            indexStore[party].add(PolarString(buf, 8), value);
            if (maps.count(key) > 0) maps.erase(key);
            maps.insert(std::pair<long long, long>(key, value));
        }
        keys.push_back(key);
        i ++;
        if (i % 1000000 == 0) fprintf(stderr, "[RadixTreeTest] : have saving 1000000 data\n");
    }

    fprintf(stderr, "[RadixTreeTest] : save 64000000 items and consume %f s\n",
            difftime(time(NULL), timer));

    for (int i = 0;  i < parties; i++) indexStore[i].finalize();
    // now test reading.

    IndexStore indexStore2[parties];
    for (int i = 0; i < parties; i++) indexStore2[i].init(path , i);
    time(&timer);
    for (long long i = 0; i < total; ) {
        long long key = 0;
        if (i % 6 == 0) {
            // testing un exist key.
            key = random() * random();
        } else {
            key = keys.at(random() % keys.size());
        }
        int party = polar_race::partition(key);
        polar_race::longToStr(key, buf);
        uint16_t fileNo = 0;
        uint16_t offset = 0;
        uint32_t ans = 0;
        indexStore2[party].get(key, &ans);
        if (ans == 0) {
            if (maps.count(key) > 0) {
                fprintf(stderr, "[RadixTreeTest] : reading error. doesn't find key : %lld\n", key);
                exit(0);
            }
        } else {
            if (maps.count(key) <= 0) {
                fprintf(stderr, "[RadixTreeTest] : find un existing key : %lld\n", key);
                exit(0);
            }
            fileNo = polar_race::unwrapFileNo(ans);
            offset = polar_race::unwrapOffset(ans);
            uint32_t t = polar_race::wrap(offset, fileNo);
            uint32_t p = maps.find(key)->second;
            if (t != p) {
                fprintf(stderr, "[RadixTreeTest] : reading error for key : %lld t : %ld != p : %lld \n", key, t, p);
                exit(0);
            }
        }
        i++;
        if (i % 1000000 == 0) fprintf(stderr, "[RadixTreeTest] : have reading 1000000 data \n");
    }
    fprintf(stderr, "[RadixTreeTest] : reading 64000000 finished and consume %f s\n",
            difftime(time(NULL), timer));

    return 0;
}

