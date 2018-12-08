//
// Created by tomzhu on 18-12-8.
//

#include <cstdio>
#include <cstdint>

/**
 * used to test config parameters here.
 *
 * @return
 */

int main() {

    int My_queueSize_ = 2;
    int My_kSingleFileSize_ = 1024 * 1024 * 128;
    int My_parties_ = 512;
    int My_map_size_ = 1024 * 512 * 3;
    int My_infoArraySize_ = 1024 * 128;
    int My_queueCapacity_ = 1024 * 128;


    // lets calc here.

    int newQueueSize = 16;
    int times = newQueueSize / My_queueSize_;
    My_kSingleFileSize_ = My_kSingleFileSize_ / (times);
    My_parties_ = My_parties_ * times;
    My_map_size_ = My_map_size_ / times;
    My_infoArraySize_ = My_infoArraySize_ / times;
    My_queueCapacity_ = My_queueCapacity_ / times;


    long long min = INT64_MIN;
    long long max = INT64_MAX;
    unsigned long long sep = ((unsigned long long) (max - min)) / My_parties_;


    fprintf(stderr, "static const int My_queueSize_ = %d;\n"
                    "static const int My_kSingleFileSize_ = %d;\n"
                    "static const int My_parties_ = %d;\n"
                    "static const int My_map_size_ = %d;\n"
                    "static const int My_infoArraySize_ = %d;\n"
                    "static const int My_queueCapacity_ = %d;\n"
                    , newQueueSize, My_kSingleFileSize_, My_parties_, My_map_size_,
                    My_infoArraySize_, My_queueCapacity_);

    fprintf(stderr, "sep : %llu", sep);

    return 0;
}
