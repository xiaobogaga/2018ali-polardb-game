//
// Created by tomzhu on 18-11-19.
// testing the correctness of bplustree and its performance
//

#include <iostream>
#include "bplustree.h"

using namespace std;

int main() {
    char fileName[] = "/tmp/btree.bin";
    int block_size = 4096;
    long key = 10000000000000;
    long key2 = 100000937120534;
    struct bplus_tree* tree = bplus_tree_init(fileName, block_size);
    bplus_tree_put(tree, key, 9);
   // bplus_tree_put(tree, key2, 4);
    long ans = bplus_tree_get(tree,  key);
    long ans2 = bplus_tree_get(tree, key2);
    printf("%d : %ld \n",  key, ans);
    printf("%d : %ld \n", key2, ans2);
    return 0;

}


