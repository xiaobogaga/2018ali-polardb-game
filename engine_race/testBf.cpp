//
// Created by tomzhu on 18-11-28.
//

#include <iostream>
#include <stdio.h>
#include "util.h"
#include "../include/engine.h"
#include "../include/polar_string.h"
#include "bloom_filter.hpp"

size_t m(double fp, size_t capacity) {
    auto ln2 = std::log(2);
    return std::ceil(-(capacity * std::log(fp) / ln2 / ln2));
}

int main() {

    bloom_parameters parameters;

    // How many elements roughly do we expect to insert?
    parameters.projected_element_count = 1024 * 128;

    // Maximum tolerable false positive probability? (0,1)
    parameters.false_positive_probability = 0.0001; // 1 in 10000

    // Simple randomizer (optional)
    parameters.random_seed = 0xA5A5A5A5;

    if (!parameters)
    {
        std::cout << "Error - Invalid set of bloom filter parameters!" << std::endl;
        return 1;
    }

    parameters.compute_optimal_parameters();

    //Instantiate Bloom Filter
    bloom_filter filter(parameters);

    fprintf(stderr, "%lld\n" , filter.size());

}
