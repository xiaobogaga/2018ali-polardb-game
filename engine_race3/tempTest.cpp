//
// Created by tomzhu on 18-11-23.
//

#include <cstdint>
#include <cstdio>
#include "util.h"

int main() {

    char a = -127;

    uint8_t t = a;

    fprintf(stderr, "%d\n", a);

    fprintf(stderr, "%d\n", t);

    char buf[8];
    for (int i = 0; i < 8; i++) buf[i] = -128;
    for (int i = 0; i < 8; i++) fprintf(stderr, ", %d , ", buf[i]);
    fprintf(stderr, "\n");
    long long ans = polar_race::strToLong(buf);
    fprintf(stderr, "%lld\n", ans);
    char buf2[8];
    polar_race::longToStr(ans, buf2);
    for (int i = 0; i < 8; i++) fprintf(stderr, ", %d , ", buf2[i]);
    fprintf(stderr, "\n");
    return 0;
}


