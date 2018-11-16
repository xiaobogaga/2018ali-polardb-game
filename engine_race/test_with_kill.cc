#include <assert.h>
#include <stdio.h>
#include <string>
#include "include/engine.h"
#include <thread>
#include <mutex>
#include <chrono>
#include <random>
#include <iostream>
#include <map>
#include <vector>

using namespace polar_race;

static const char kEnginePath[] = "/tmp/test_engine";
static const char kDumpPath[] = "/tmp/test_dump";
static const std::mutex mutex;
static volatile bool shutdown = false;

void writeAValue(EngineRace* engine, const PolarString& key, 
	const PolarString& value, const std::map<PolarString, PolarString>& maps, 
	const std::vector<PolarString>& keys);

PolarString generateAKey(const default_random_engine& random);

PolarString generateValue(const default_random_engine& random);

void writeTask(EngineRace* engine, const default_random_engine& random, 
	int writeTimes, const std::map<PolarString, PolarString>& maps, 
	const std::vector<PolarString>& keys);
	
void writeTask2(EngineRace* engine, const default_random_engine& random, 
	int writeTimes, const std::map<PolarString, PolarString>& maps, 
	const std::vector<PolarString>& keys);

void testReader(EngineRace* engine, std::map<PolarString, PolarString> *maps, 
	std::vector<PolarString> keys, int readerTime, const default_random_engine& random);
	
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
		this->groups = new std::thread[threadSize];
		fprintf(stderr, "start writing\n"):
		for (int i = 0; i < threadSize; i++) {
			groups[i] = new std::thread(writeTask, engine, 
				this->random, writeTimes);
		}
		fprintf(stderr, "end writing\n");
	}
	
	void startPerformanceWrite(int threadSize, int writeTime) {
		this->threadSize = threadSize;
		this->groups = new std::thread[threadSize];
		fprintf(stderr, "start writing\n");
		for (int i = 0; i < threadSize; i++) {
			groups[i] = new std::thread(writeTask2, engine, 
				this->random, writeTimes);
		}
		for (int i = 0; i < threadSize; i++) {
			groups[i]->join();
		}
		fprintf(stderr, "end writing\n");
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
	}
	
	~WriterTask() {
	}
	
	std::map<PolarString, PolarString> maps;

private:
	int threadSize;
	std::thread* groups;
	EngineRace* engine;
	default_random_engine random;
	std::vector<PolarString> keys;
}


void writeAValue(EngineRace* engine, const PolarString& key, 
	const PolarString& value, const std::map<PolarString, PolarString>& maps, 
	const std::vector<PolarString>& keys) {
	mutex.lock();
	
	keys.push_back(key);
	maps.insert( std::pair<PolarString, PolarString> (key, value));
	engine.Write(key, value);
	
	mutex.unlock();
}

PolarString generateAKey(const default_random_engine& random) {
	char buf[8];
	for (int i = 0; i < 8; i++) {
		buf[i] = random() % 256;
	}
	return PolarString(buf, 8);
}

PolarString generateValue(const default_random_engine& random) {
	size_t size = e() % 4096;
	char buf[size];
	for (int i = 0; i < size; i++) {
		buf[i] = random() % 256;
	}
	return PolarString(buf, size);
}

void writeTask(EngineRace* engine, const default_random_engine& random, 
	int writeTimes, const std::map<PolarString, PolarString>& maps, 
	const std::vector<PolarString>& keys) {
	for (int i = 0; i < writeTimes && !shutdown; i++) {
		if (i % 6 != 0) {
			const PolarString& value = generateValue(random);
			writeAValue(engine, keys.at(random() % keys.size()),value, keys);
		} else {	
			const PolarString& key = generateAKey(random);
			const PolarString& value = generateValue(random);
			writeAValue(engine, key, value, maps, keys);
		}
	}
}

void writeTask2(EngineRace* engine, const default_random_engine& random, 
	int writeTimes, const std::map<PolarString, PolarString>& maps, 
	const std::vector<PolarString>& keys) {
	for (int i = 0; i < writeTimes; i++) {
		if (i % 6 != 0) {
			const PolarString& value = generateValue(random);
			writeAValue(engine, keys.at(random() % keys.size()), value, keys);
		} else {	
			const PolarString& key = generateAKey(random);
			const PolarString& value = generateValue(random);
			writeAValue(engine, key, value, maps, keys);
		}
	}
}



void testReader(EngineRace* engine, std::map<PolarString, PolarString> *maps, 
	std::vector<PolarString> keys, int readerTime, const default_random_engine& random) {
	for (int i = 0; i < readerTime; i++) {
		std::string value;
		Retcode code;
		PolarString& key;
		if (i % 2 == 0) {
			key = keys.at(i);
			code engine->Read(key, &value);
		} else {
			key = generateAKey(random);
			code = engine.Read(key, &value);
		}
		if (code == kSucc) {
			std::map<PolarString, PolarString>::iterator ite = maps->find(key);
			if (ite != maps.end()) {
				if (ite->second.compare(value) != 0) {
					fprintf(stderr, "find an unmatching key\n");
				}
			} else {
				fprintf(stderr, "find a doesn't exist key\n");
			}
		} else {
			if (maps.count(key) != 0) {
				fprintf(stderr, "error couldn't find key\n");
			}
		}
	}
}

class ReaderPro {
	
public :
	ReaderPro (EngineRace* engine, std::map<PolarString, PolarString>* maps, 
		std::vector<PolarString>* keys) {
		this->engine = engine;
		this->maps = maps;
		unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
		this->random.seed(seed);
		this->keys = keys;
	}
	
	~ReaderPro() {}
	
	void startReader(int threadSize, int readerTime) {
		this->threadSize = threadSize;
		this->groups = new std::thread[threadSize];
		fprintf(stderr, "start reading\n");
		for (int i = 0; i < threadSize; i++) {
			this->groups[i] = new std::thread(testReader, this->engine, 
				this->maps, readerTime, this->random);
		}
		for (int i = 0; i < threadSize; i++) {
			this->groups[i].join();
		}
		fprintf(stderr, "end reading\n");
		for (int i = 0; i < threadSize; i++)
			delete this->groups[i];
		delete[] this->groups;
		this->groups = NULL;
	}
	
	
private:
	int threadSize;
	EngineRace* engine;
	std::map<PolarString, PolarString>* maps;
	std::thead* groups;
	default_random_engine random;
	std::vector<PolarString>* keys;
}


int main(int argc, char** argv) {
	// test_with_kill threadSize writing_time
	fprintf(stderr, "Correctness Test\n");
	int threadSize = atoi(argv[1]);
	int writingTime = atoi(argv[2]);
	std::string path(kDumpPath);
	EngineRace* engine = NULL;
	EngineRace::Open(path, &engine);
	delete engine; // finilize.
	EngineRace::Open(path, &engine);
	WriterTask writeTask(engine);
	writeTask.startKillableWriter(threadSize, writing_time);
	std::this_thread::sleep_for (std::chrono::seconds(10)); // sleeping for ten seconds.
	shutdown = true;
	writeTask.waitThreadEnd();
	
	// todo
	delete engine; // finilize.
	return 0;
}