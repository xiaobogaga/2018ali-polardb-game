//
// Created by tomzhu on 18-11-23.
//

#include <cstdint>
#include <cstdio>
#include "util.h"
#include <stdio.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include "engine.h"
#include "polar_string.h"

int main2() {

    long long min = INT64_MIN;
    long long max = INT64_MAX;
    long long sep = max / 64 - (min / 64);

    long long start = min;
    int size = 64;
    for (int i = 0; i < size; i++) {
        fprintf(stderr, "%d : [%lld - %lld] , right / sep = %lld \n", i, start, start + sep, ((unsigned long long ) (start + sep - min) / sep));

        start += sep;
    }

    fprintf(stderr, "min : %lld, max : %lld, sep : %lld\n", min, max, sep);

    for (long long t = min; t < min + sep; t++)
        fprintf(stderr, "%d\n", polar_race::partition(t));

    return 0;
}

struct It {
    int data;
};

int testRead();

int main() {

    testRead();

    system("rm -rf /tmp/test_dump/*");
    std::string fileName("/tmp/test_dump/test");
    int fd = open(fileName.c_str(), O_RDWR, 0644);
    size_t fileLength;
    size_t map_size = 4096;
    size_t newMapSize = map_size;
    bool new_create = false;
    if (fd < 0 && errno == ENOENT) {
        // not exist, then create
        fd = open(fileName.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd >= 0) {
            new_create = true;
            if (posix_fallocate(fd, 0, map_size) != 0) {
                fprintf(stderr, "[IndexStore-%d] : posix_fallocate failed\n");
                close(fd);
                return polar_race::kIOError;
            }
        }
    }

    if (fd < 0) {
        fprintf(stderr, "[IndexStore] : file %s open failed\n", fileName.c_str());
        return polar_race::kIOError;
    } else {
        fileLength = polar_race::GetFileLength(fileName);
        if (fileLength > newMapSize) newMapSize = fileLength;
    }
    int fd_ = fd;

    void* ptr = mmap(NULL, newMapSize, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd_, 0);
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "[IndexStore] : MAP_FAILED\n");
        close(fd);
        return polar_race::kIOError;
    }
    if (new_create) {
        //   fprintf(stderr, "[IndexStore] : create a new mmap \n");
        memset(ptr, 0, newMapSize);
    }

    fprintf(stderr, "%ld" , sysconf(_SC_PAGE_SIZE));

    struct It* items_ = reinterpret_cast<It *>(ptr);
    struct It* head_ = items_;
    size_t start = newMapSize;
    for (int i = 0; i < 4096; i++) {
        if (items_ - head_ >= (newMapSize) / sizeof(struct It)) {
            // reallocate.
            // needs reallocate.
            fprintf(stderr, "reallocate\n");
            int ret = munmap(head_, newMapSize);
            if (ret == -1) fprintf(stderr, "unmap failed\n");
            // if (lseek(fd, newMapSize, SEEK_END) == -1) fprintf(stderr, "seek failed\n");
            if (posix_fallocate(fd, start, newMapSize) != 0) {
                fprintf(stderr, "[IndexStore-%d] : posix_fallocate failed\n");
                close(fd);
                return polar_race::kIOError;
            }
            void* ptr = mmap(NULL, newMapSize, PROT_READ | PROT_WRITE,
                             MAP_SHARED, fd_, start);
            if (ptr == MAP_FAILED) {
                fprintf(stderr, "[IndexStore] : MAP_FAILED\n");
                close(fd);
                return polar_race::kIOError;
            }
            items_ = reinterpret_cast<It *>(ptr);
            head_ = items_;
            start += newMapSize;
        }
        items_->data = i;
        items_ ++;
    }


    if (fd_ >= 0) {
        // items_ = NULL;
        munmap(head_, newMapSize);
        items_ = NULL;
        head_ = NULL;
        close(fd_);
        fd_ = -1;
    }

}

int testRead() {
    std::string fileName("/tmp/test_dump/test");
    int fd = open(fileName.c_str(), O_RDWR, 0644);
    size_t fileLength;
    size_t map_size = 4096;
    size_t newMapSize = map_size;
    bool new_create = false;
    if (fd < 0 && errno == ENOENT) {
        // not exist, then create
        fd = open(fileName.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd >= 0) {
            new_create = true;
            if (posix_fallocate(fd, 0, map_size) != 0) {
                fprintf(stderr, "[IndexStore-%d] : posix_fallocate failed\n");
                close(fd);
                return polar_race::kIOError;
            }
        }
    }

    if (fd < 0) {
        fprintf(stderr, "[IndexStore] : file %s open failed\n", fileName.c_str());
        return polar_race::kIOError;
    } else {
        fileLength = polar_race::GetFileLength(fileName);
        if (fileLength > newMapSize) newMapSize = fileLength;
    }
    int fd_ = fd;

    void* ptr = mmap(NULL, newMapSize, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd_, 0);
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "[IndexStore] : MAP_FAILED\n");
        close(fd);
        return polar_race::kIOError;
    }
    if (new_create) {
        //   fprintf(stderr, "[IndexStore] : create a new mmap \n");
        memset(ptr, 0, newMapSize);
    }

    // fprintf(stderr, "%ld" , sysconf(_SC_PAGE_SIZE));

    struct It* items_ = reinterpret_cast<It *>(ptr);
    struct It* head_ = items_;

    for (int i = 0; i < 4096; i++) {
        fprintf(stderr, "%d\n", items_->data);
        items_++;
    }

    exit(0);

    if (fd_ >= 0) {
        // items_ = NULL;
        munmap(head_, newMapSize);
        items_ = NULL;
        head_ = NULL;
        close(fd_);
        fd_ = -1;
    }

}


/*
 *
0 : [-9223372036854775808 - -8935141660703064065]
1 : [-8935141660703064064 - -8646911284551352321]
2 : [-8646911284551352320 - -8358680908399640577]
3 : [-8358680908399640576 - -8070450532247928833]
4 : [-8070450532247928832 - -7782220156096217089]
5 : [-7782220156096217088 - -7493989779944505345]
6 : [-7493989779944505344 - -7205759403792793601]
7 : [-7205759403792793600 - -6917529027641081857]
8 : [-6917529027641081856 - -6629298651489370113]
9 : [-6629298651489370112 - -6341068275337658369]
10 : [-6341068275337658368 - -6052837899185946625]
11 : [-6052837899185946624 - -5764607523034234881]
12 : [-5764607523034234880 - -5476377146882523137]
13 : [-5476377146882523136 - -5188146770730811393]
14 : [-5188146770730811392 - -4899916394579099649]
15 : [-4899916394579099648 - -4611686018427387905]
16 : [-4611686018427387904 - -4323455642275676161]
17 : [-4323455642275676160 - -4035225266123964417]
18 : [-4035225266123964416 - -3746994889972252673]
19 : [-3746994889972252672 - -3458764513820540929]
20 : [-3458764513820540928 - -3170534137668829185]
21 : [-3170534137668829184 - -2882303761517117441]
22 : [-2882303761517117440 - -2594073385365405697]
23 : [-2594073385365405696 - -2305843009213693953]
24 : [-2305843009213693952 - -2017612633061982209]
25 : [-2017612633061982208 - -1729382256910270465]
26 : [-1729382256910270464 - -1441151880758558721]
27 : [-1441151880758558720 - -1152921504606846977]
28 : [-1152921504606846976 - -864691128455135233]
29 : [-864691128455135232 - -576460752303423489]
30 : [-576460752303423488 - -288230376151711745]
31 : [-288230376151711744 - -1]
32 : [0 - 288230376151711743]
33 : [288230376151711744 - 576460752303423487]
34 : [576460752303423488 - 864691128455135231]
35 : [864691128455135232 - 1152921504606846975]
36 : [1152921504606846976 - 1441151880758558719]
37 : [1441151880758558720 - 1729382256910270463]
38 : [1729382256910270464 - 2017612633061982207]
39 : [2017612633061982208 - 2305843009213693951]
40 : [2305843009213693952 - 2594073385365405695]
41 : [2594073385365405696 - 2882303761517117439]
42 : [2882303761517117440 - 3170534137668829183]
43 : [3170534137668829184 - 3458764513820540927]
44 : [3458764513820540928 - 3746994889972252671]
45 : [3746994889972252672 - 4035225266123964415]
46 : [4035225266123964416 - 4323455642275676159]
47 : [4323455642275676160 - 4611686018427387903]
48 : [4611686018427387904 - 4899916394579099647]
49 : [4899916394579099648 - 5188146770730811391]
50 : [5188146770730811392 - 5476377146882523135]
51 : [5476377146882523136 - 5764607523034234879]
52 : [5764607523034234880 - 6052837899185946623]
53 : [6052837899185946624 - 6341068275337658367]
54 : [6341068275337658368 - 6629298651489370111]
55 : [6629298651489370112 - 6917529027641081855]
56 : [6917529027641081856 - 7205759403792793599]
57 : [7205759403792793600 - 7493989779944505343]
58 : [7493989779944505344 - 7782220156096217087]
59 : [7782220156096217088 - 8070450532247928831]
60 : [8070450532247928832 - 8358680908399640575]
61 : [8358680908399640576 - 8646911284551352319]
62 : [8646911284551352320 - 8935141660703064063]
63 : [8935141660703064064 - 9223372036854775807]
 *
 */


