//
// Created by tomzhu on 18-11-23.
//

#include <cstdint>
#include <cstdio>

int main() {

    char a = -127;

    uint8_t t = a;

    fprintf(stderr, "%d\n", a);

    fprintf(stderr, "%d\n", t);

    return 0;
}


