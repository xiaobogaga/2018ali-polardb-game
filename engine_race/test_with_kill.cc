#include <assert.h>
#include <stdio.h>
#include <string>
#include "../include/engine.h"
#include "engine_race.h"
#include <thread>
#include <mutex>
#include <chrono>
#include <random>
#include <iostream>
#include <map>
#include <vector>
#include "util.h"

using polar_race::EngineRace;
using polar_race::Engine;
using polar_race::PolarString;
using polar_race::RetCode;

struct PolarStringComparator {
    bool operator()(const PolarString &x, const PolarString &y) const {
        return x.compare(y) < 0;
    }
};

namespace polar_race {

    class MyVisitor : public Visitor {

    public:
        void Visit(const PolarString &key, const PolarString &value) override {
            long long k = polar_race::strToLong(key.data());
        //    fprintf(stderr, "[Visitor] : visiting %lld key \n", k);
            if (tempMaps.count(k) > 0) {
                fprintf(stderr, "[Visitor-%d] : error! iterator duplicate keys %lld\n", party, k);
                exit(1);
            }
            tempMaps.insert( std::pair<long long, int> (k, 1) );
            std::map<PolarString, PolarString>::iterator ite = maps->find(key);
            if (ite != maps->end()) {
                if (ite->second.compare(PolarString(value)) != 0) {
                    fprintf(stderr, "[Visitor-%d] : error! find an unmatching key. %lld\n", party, k);
                }
            } else {
                fprintf(stderr, "[Visitor-%d] : error! visit an unexist key %lld\n", party, k);
            }
        }

        MyVisitor(int party, std::map<PolarString, PolarString, PolarStringComparator>* maps) {
            this->party = party;
            this->maps = maps;
        }

        void checkSizeEqual() {
            if (maps->size() != tempMaps.size())
                fprintf(stderr, "[Visitor-%d] : elements size error. ite %d elements but real %d elements\n",
                        party, tempMaps.size(), maps->size());
        }

    private:
        int party;
        std::map<long long, int> tempMaps;
        std::map<PolarString, PolarString, PolarStringComparator>* maps;
    };

}

static const char kEnginePath[] = "/tmp/test_engine";
static const char kDumpPath[] = "/tmp/test_dump";
static std::mutex mutex;
static volatile bool shutdown = false;


void writeAValue(EngineRace* engine, PolarString& key,
                 PolarString& value, std::map<PolarString, PolarString, PolarStringComparator>* maps,
                 std::vector<PolarString>* keys);

PolarString generateAKey(std::default_random_engine* random);

PolarString generateValue(std::default_random_engine* random);

void writeTask(EngineRace* engine, std::default_random_engine* random,
               int writeTimes, std::map<PolarString, PolarString, PolarStringComparator>* maps,
               std::vector<PolarString>* keys);

void writeTask2(EngineRace* engine, std::default_random_engine* random,
                int writeTimes, std::map<PolarString, PolarString, PolarStringComparator>* maps,
                std::vector<PolarString>* keys);

void testReader(EngineRace* engine, std::map<PolarString, PolarString, PolarStringComparator>* maps,
                std::vector<PolarString>* keys, int readerTime,
                std::default_random_engine* random);

void testRange(int party, EngineRace* engine, std::map<PolarString, PolarString, PolarStringComparator>* maps,
               std::vector<PolarString>* keys, int readerTime, std::default_random_engine* random);

class WriterTask {

public:
    WriterTask(EngineRace* engine) {
        this->engine = engine;
        this->groups = NULL;
        unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
        this->random.seed(seed);
    }

    void startKillableWriter(int threadSize, int writeTimes) {
        this->threadSize = threadSize;
        this->groups = new std::thread*[threadSize];
        // fprintf(stderr, "start writing\n");
        for (int i = 0; i < threadSize; i++) {
            groups[i] = new std::thread(writeTask, engine,
                                        &this->random, writeTimes, &this->maps,
                                        &this->keys);
        }
    }

    void startPerformanceWrite(int threadSize, int writeTimes) {
        this->threadSize = threadSize;
        this->groups = new std::thread*[threadSize];
        // fprintf(stderr, "start writing\n");
        for (int i = 0; i < threadSize; i++) {
            groups[i] = new std::thread(writeTask2, engine,
                                        &this->random, writeTimes, &this->maps, &this->keys);
        }
        for (int i = 0; i < threadSize; i++) {
            groups[i]->join();
        }
        // fprintf(stderr, "end writing\n");
        for (int i = 0; i < threadSize; i++)
            delete this->groups[i];
        delete[] this->groups;
        this->groups = NULL;
    }

    void waitThreadEnd() {
        for (int i = 0; i < threadSize; i++) {
            groups[i]->join();
        }
        for (int i = 0; i < threadSize; i++)
            delete this->groups[i];
        delete[] this->groups;
        this->groups = NULL;
        // fprintf(stderr, "end writing \n");
    }

    std::map<PolarString, PolarString, PolarStringComparator>* getMaps() {
        return &this->maps;
    }

    std::vector<PolarString>* getKeys() {
        return &this->keys;
    }

    ~WriterTask() {
    }

    std::map<PolarString, PolarString, PolarStringComparator> maps;
    std::vector<PolarString> keys;

private:
    int threadSize;
    std::thread** groups;
    EngineRace* engine;
    std::default_random_engine random;
};


void writeAValue(EngineRace* engine, PolarString& key,
                 PolarString& value, std::map<PolarString, PolarString, PolarStringComparator>* maps,
                 std::vector<PolarString>* keys) {

    keys->push_back(key);
    if (maps->count(key) > 0) maps->erase(key);
    maps->insert( std::pair<PolarString, PolarString> (key, value));
    engine->Write(key, value);

}

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

void writeTask(EngineRace* engine, std::default_random_engine* random,
               int writeTimes, std::map<PolarString, PolarString, PolarStringComparator>* maps,
               std::vector<PolarString>* keys) {
    for (int i = 0; i < writeTimes && !shutdown; i++) {
        mutex.lock();
        if (i % 6 == 0 && i != 0) {
            PolarString value = generateValue(random);
            int size = keys->size();
            int loc = (*random)() % size;
            PolarString key = keys->at(loc);
            writeAValue(engine, key,value, maps, keys);
        } else {
            PolarString key = generateAKey(random);
            PolarString value = generateValue(random);
            writeAValue(engine, key, value, maps, keys);
        }
        mutex.unlock();
    }
}

void writeTask2(EngineRace* engine, std::default_random_engine* random,
                int writeTimes, std::map<PolarString, PolarString, PolarStringComparator>* maps,
                std::vector<PolarString>* keys) {
    for (int i = 0; i < writeTimes; i++) {
        mutex.lock();
        if (i % 6 == 0 && i != 0) {
            PolarString value = generateValue(random);
            int size = keys->size();
            int loc = (*random)() % size;
            PolarString key = keys->at(loc);
            writeAValue(engine, key, value, maps, keys);
        } else {
            PolarString key = generateAKey(random);
            PolarString value = generateValue(random);
            writeAValue(engine, key, value, maps, keys);
        }
        mutex.unlock();
    }
}



void testReader(EngineRace* engine, std::map<PolarString, PolarString, PolarStringComparator>* maps,
                std::vector<PolarString>* keys, int readerTime, std::default_random_engine* random) {
    mutex.lock();
    for (int i = 0; i < readerTime; i++) {
        std::string value;
        RetCode code = RetCode::kSucc;
        PolarString key;

        if (i % 2 == 0) {
            int loc = (*random)() % keys->size();
            key = keys->at(loc);
            code = engine->Read(key, &value);
        } else {
            key = generateAKey(random);
            code = engine->Read(key, &value);
        }

        if (code == RetCode::kSucc) {
            std::map<PolarString, PolarString>::iterator ite = maps->find(key);
            if (ite != maps->end()) {
                if (ite->second.compare(PolarString(value)) != 0) {
                    fprintf(stderr, "[Reader] : error! find an unmatching key. %lld\n", polar_race::strToLong(key.data()));
                    break;
                }
            } else {
                fprintf(stderr, "[Reader] : error! find a doesn't exist key. %lld\n", polar_race::strToLong(key.data()));
                break;
            }
        } else {
            if (maps->count(key) > 0) {
                fprintf(stderr, "[Reader] : error! error couldn't find key. %lld\n", polar_race::strToLong(key.data()));
                break;
            }
        }

    }
    mutex.unlock();
}

void testRange(int party, EngineRace* engine, std::map<PolarString, PolarString, PolarStringComparator>* maps,
                std::vector<PolarString>* keys, int readerTime, std::default_random_engine* random) {
    // fprintf(stderr, "[Reader] : start testing range query\n");
    polar_race::MyVisitor visit(party, maps);
    engine->MyRange(PolarString(std::string("")),
                  PolarString(std::string("")), visit);
    visit.checkSizeEqual();
}

class ReaderPro {

public :
    ReaderPro (EngineRace* engine, std::map<PolarString, PolarString, PolarStringComparator>* maps,
               std::vector<PolarString>* keys) {
        this->engine = engine;
        this->maps = maps;
        unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
        this->random.seed(seed);
        this->keys = keys;
    }

    ~ReaderPro() {}

    void startReader(int threadSize, int readerTime) {
        this->groups = new std::thread*[threadSize];
        // fprintf(stderr, "start reading\n");
        for (int i = 0; i < threadSize; i++) {
            this->groups[i] = new std::thread(testReader, this->engine,
                                              this->maps, this->keys, readerTime, &this->random);
        }
        for (int i = 0; i < threadSize; i++) {
            this->groups[i]->join();
        }
        //fprintf(stderr, "end reading\n");
        for (int i = 0; i < threadSize; i++)
            delete this->groups[i];
        delete[] this->groups;
        this->groups = NULL;
    }


private:
    EngineRace* engine;
    std::map<PolarString, PolarString, PolarStringComparator>* maps;
    std::thread** groups;
    std::default_random_engine random;
    std::vector<PolarString>* keys;
};

class RangePro {
public:
    RangePro (EngineRace* engine, std::map<PolarString, PolarString, PolarStringComparator>* maps,
            std::vector<PolarString>* keys) {
        this->engine = engine;
        this->maps = maps;
        unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
        this->random.seed(seed);
        this->keys = keys;
    }

    ~RangePro() {}

    void startRange(int threadSize, int readerTime) {
        this->groups = new std::thread*[threadSize];
        // fprintf(stderr, "start range reading\n");
        for (int i = 0; i < threadSize; i++) {
            this->groups[i] = new std::thread(testRange, i, this->engine,
                                              this->maps, this->keys, readerTime, &this->random);
        }
        for (int i = 0; i < threadSize; i++) {
            this->groups[i]->join();
        }
        // fprintf(stderr, "end range reading\n");
        for (int i = 0; i < threadSize; i++)
            delete this->groups[i];
        delete[] this->groups;
        this->groups = NULL;
    }


private:
    EngineRace* engine;
    std::map<PolarString, PolarString, PolarStringComparator>* maps;
    std::thread** groups;
    std::default_random_engine random;
    std::vector<PolarString>* keys;
};


int main(int argc, char** argv) {
    // test_with_kill threadSize writing_time
    system("rm -rf /tmp/test_dump/*");
    fprintf(stderr, "Correctness Test\n");
    int threadSize = 64;
    int writingTime = 1000;
    if (argc <= 1) {
        ;
    } else {
        threadSize = atoi(argv[1]);
        writingTime = atoi(argv[2]);
    }
    std::string path(kDumpPath);
    Engine* engine = NULL;
 //   Engine::Open(path, &engine);
 //   delete engine; // finilize.
    EngineRace::Open(path, &engine);
    WriterTask writeTask((EngineRace*) engine);
    fprintf(stderr , "[Tester] : start writer\n");
    writeTask.startKillableWriter(threadSize, writingTime);
    // std::this_thread::sleep_for (std::chrono::seconds(5)); // sleeping for ten seconds.
    // shutdown = true;
    writeTask.waitThreadEnd();
    fprintf(stderr , "[Tester] : end writer\n");
    // Engine::Open(path, &engine);
    ReaderPro readerPro((EngineRace*) engine, writeTask.getMaps(), writeTask.getKeys());
    fprintf(stderr , "[Tester] : start reader\n");
    readerPro.startReader(threadSize, writingTime);
    fprintf(stderr , "[Tester] : end reader\n");
    fprintf(stderr , "[Tester] : start range read\n");

    RangePro rangePro((EngineRace*) engine, writeTask.getMaps(), writeTask.getKeys());
    rangePro.startRange(threadSize, writingTime);

    fprintf(stderr , "[Tester] : end range read\n");
    delete engine; // finilize.

    // exit(0);

    // after do finilize, then we can do performance test.
    fprintf(stderr, "Performance Test\n");
    // first clear dirs.
    system("rm -rf /tmp/test_dump/*");
    EngineRace::Open(path, &engine);
    WriterTask performanceWriterTask((EngineRace*) engine);
    threadSize = 64;
    writingTime = 1000;
    fprintf(stderr , "[Tester] : start writer\n");
    performanceWriterTask.startPerformanceWrite(threadSize, writingTime);
    fprintf(stderr , "[Tester] : end writer\n");
    delete engine;
    EngineRace::Open(path, &engine);
    fprintf(stderr , "[Tester] : start read\n");
    ReaderPro performanceReaderTask((EngineRace*) engine, performanceWriterTask.getMaps(), performanceWriterTask.getKeys());
    performanceReaderTask.startReader(threadSize, writingTime);
    fprintf(stderr , "[Tester] : end read\n");
    fprintf(stderr , "[Tester] : start range read\n");
    delete engine;
    Engine::Open(path, &engine);
    RangePro performanceRangeTask((EngineRace*) engine, performanceWriterTask.getMaps(), performanceWriterTask.getKeys());
    performanceRangeTask.startRange(threadSize, writingTime);
    fprintf(stderr , "[Tester] : end range read\n");
    delete engine; // finilize.
    system("rm -rf /tmp/test_dump/*");
    return 0;
}
