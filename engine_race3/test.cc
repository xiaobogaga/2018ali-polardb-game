#include <assert.h>
#include <stdio.h>
#include <string>
#include <random>
#include <chrono>
#include "../include/engine.h"
#include "engine_race.h"

using polar_race::EngineRace;
using polar_race::Engine;
using polar_race::PolarString;
using polar_race::RetCode;
using polar_race::Visitor;

static const char kEnginePath[] = "/tmp/test_engine";
static const char kDumpPath[] = "/tmp/test_dump";

class DumpVisitor : public Visitor {
public:
    DumpVisitor(int *kcnt)
            : key_cnt_(kcnt) {}

    ~DumpVisitor() {}

    void Visit(const PolarString &key, const PolarString &value) {
        printf("Visit %s --> %s\n", key.data(), value.data());
        (*key_cnt_)++;
    }

private:
    int *key_cnt_;
};

PolarString generateAKey(std::default_random_engine &random, char buf[]) {
    for (int i = 0; i < 8; i++) {
        buf[i] = random() % 256;
    }
    return PolarString(buf, 8);
}

PolarString generateValue(std::default_random_engine &random, char buf[]) {
    size_t size = 4096;
    for (size_t i = 0; i < size; i++) {
        buf[i] = random() % 256;
    }
    return PolarString(buf, size);
}

int main() {
    system("rm -rf /tmp/test_dump/*");
    Engine *engine = NULL;
    std::string path(kDumpPath);
    RetCode ret = Engine::Open(path, &engine);
    assert (ret == RetCode::kSucc);

    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine random;
    random.seed(seed);

    char key1[8];
    char value1[4096];
    generateAKey(random, key1);
    generateValue(random, value1);
    ret = engine->Write(key1, PolarString(value1, 4096));
    assert (ret == RetCode::kSucc);
    std::string strValue1;
    ret = engine->Read(PolarString(key1, 8), &strValue1);
    assert (ret == RetCode::kSucc);
    if (strValue1.compare(std::string(value1, 4096)) != 0) {
        fprintf(stderr, "[Test-1] : find an unmatching key\n");
    }

    char value2[4096];
    generateValue(random, value2);
    ret = engine->Write(key1, PolarString(value2, 4096));
    assert (ret == RetCode::kSucc);
    std::string strValue2;
    ret = engine->Read(PolarString(key1, 8), &strValue2);
    assert (ret == RetCode::kSucc);
    if (strValue2.compare(std::string(value2, 4096)) != 0) {
        fprintf(stderr, "[Test-2] : find an unmatching key\n");
    }

    char key3[8];
    char value3[4096];
    generateAKey(random, key3);
    generateValue(random, value3);
    ret = engine->Write(key3, PolarString(value3, 4096));
    assert (ret == RetCode::kSucc);
    std::string strValue3;
    ret = engine->Read(PolarString(key3, 8), &strValue3);
    assert (ret == RetCode::kSucc);
    if (strValue3.compare(std::string(value3, 4096)) != 0) {
        fprintf(stderr, "[Test-3] : find an unmatching key\n");
    }

    char value4[4096];
    generateValue(random, value4);
    ret = engine->Write(key3, PolarString(value4, 4096));
    assert (ret == RetCode::kSucc);
    std::string strValue4;
    ret = engine->Read(PolarString(key3, 8), &strValue4);
    assert (ret == RetCode::kSucc);
    if (strValue4.compare(std::string(value4, 4096)) != 0) {
        fprintf(stderr, "[Test-4] : find an unmatching key\n");
    }
    delete engine;

    ret = Engine::Open(path, &engine);
//    assert (ret == RetCode::kSucc);
//    ret = engine->Read(PolarString(key1, 8), &strValue1);
//    assert (ret == RetCode::kSucc);
//    if (strValue1.compare(std::string(value2, 4096)) != 0) {
//        fprintf(stderr, "[Test-5] : find an unmatching key\n");
//    }

    generateAKey(random, key1);
    generateValue(random, value1);
    ret = engine->Write(key1, PolarString(value1, 4096));
    assert (ret == RetCode::kSucc);
    ret = engine->Read(PolarString(key1, 8), &strValue1);
    assert (ret == RetCode::kSucc);
    if (strValue1.compare(std::string(value1, 4096)) != 0) {
        fprintf(stderr, "[Test-1] : find an unmatching key\n");
    }

    generateValue(random, value2);
    ret = engine->Write(key1, PolarString(value2, 4096));
    assert (ret == RetCode::kSucc);
    ret = engine->Read(PolarString(key1, 8), &strValue2);
    assert (ret == RetCode::kSucc);
    if (strValue2.compare(std::string(value2, 4096)) != 0) {
        fprintf(stderr, "[Test-2] : find an unmatching key\n");
    }

    generateAKey(random, key3);
    generateValue(random, value3);
    ret = engine->Write(key3, PolarString(value3, 4096));
    assert (ret == RetCode::kSucc);
    ret = engine->Read(PolarString(key3, 8), &strValue3);
    assert (ret == RetCode::kSucc);
    if (strValue3.compare(std::string(value3, 4096)) != 0) {
        fprintf(stderr, "[Test-3] : find an unmatching key\n");
    }

    generateValue(random, value4);
    ret = engine->Write(key3, PolarString(value4, 4096));
    assert (ret == RetCode::kSucc);
    ret = engine->Read(PolarString(key3, 8), &strValue4);
    assert (ret == RetCode::kSucc);
    if (strValue4.compare(std::string(value4, 4096)) != 0) {
        fprintf(stderr, "[Test-4] : find an unmatching key\n");
    }

    return 0;
}
