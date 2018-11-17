#include <assert.h>
#include <stdio.h>
#include <string>
#include "include/engine.h"
#include "engine_race.h"
#include <thread>
#include <mutex>
#include <chrono>
#include <random>
#include <iostream>
#include <map>
#include <vector>

using polar_race::EngineRace;
using polar_race::Engine;
using polar_race::PolarString;
using polar_race::RetCode;

static const char kEnginePath[] = "/tmp/test_engine";
static const char kDumpPath[] = "/tmp/test_dump";
static std::mutex mutex;
static volatile bool shutdown = false;

void writeAValue(Engine* engine, PolarString& key, 
	const PolarString& value, std::map<PolarString, PolarString>& maps, 
	std::vector<PolarString>& keys);

PolarString generateAKey(std::default_random_engine& random);

PolarString generateValue(std::default_random_engine& random);

void writeTask(Engine* engine, std::default_random_engine& random, 
	int writeTimes, std::map<PolarString, PolarString>& maps, 
	std::vector<PolarString>& keys);
	
void writeTask2(Engine* engine, std::default_random_engine& random, 
	int writeTimes, std::map<PolarString, PolarString>& maps, 
	std::vector<PolarString>& keys);

void testReader(Engine* engine, std::map<PolarString, PolarString>* maps, 
	std::vector<PolarString>* keys, int readerTime, std::default_random_engine& random);
	
class WriterTask {

public:	
	WriterTask(Engine* engine) {
		this->engine = engine;
		this->groups = NULL;
		unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
		this->random.seed(seed);
	}
	
	void startKillableWriter(int threadSize, int writeTimes) {
		this->threadSize = threadSize;
		this->groups = new std::thread*[threadSize];
		fprintf(stderr, "start writing\n");
		for (int i = 0; i < threadSize; i++) {
			groups[i] = new std::thread(writeTask, std::ref(engine), 
				std::ref(this->random), writeTimes, std::ref(this->maps), std::ref(this->keys));
		}
	}
	
	void startPerformanceWrite(int threadSize, int writeTimes) {
		this->threadSize = threadSize;
		this->groups = new std::thread*[threadSize];
		fprintf(stderr, "start writing\n");
		for (int i = 0; i < threadSize; i++) {
			groups[i] = new std::thread(writeTask, std::ref(engine), 
				std::ref(this->random), writeTimes, std::ref(this->maps), std::ref(this->keys));
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
		fprintf(stderr, "end writing \n");
	}
	
	~WriterTask() {
	}
	
	std::map<PolarString, PolarString> maps;
	std::vector<PolarString> keys;

private:
	int threadSize;
	std::thread** groups;
	Engine* engine;
	std::default_random_engine random;
};


void writeAValue(Engine* engine, PolarString& key, 
	PolarString& value, std::map<PolarString, PolarString>& maps, 
	std::vector<PolarString>& keys) {
	mutex.lock();
	
	keys.push_back(key);
	maps.insert( std::pair<PolarString, PolarString> (key, value));
	engine->Write(key, value);
	
	mutex.unlock();
}

PolarString generateAKey(std::default_random_engine& random) {
	char buf[8];
	for (int i = 0; i < 8; i++) {
		buf[i] = random() % 256;
	}
	return PolarString(buf, 8);
}

PolarString generateValue(std::default_random_engine& random) {
	size_t size = random() % 4096;
	char buf[size];
	for (size_t i = 0; i < size; i++) {
		buf[i] = random() % 256;
	}
	return PolarString(buf, size);
}

void writeTask(Engine* engine, std::default_random_engine& random, 
	int writeTimes, std::map<PolarString, PolarString>& maps, 
	std::vector<PolarString>& keys) {
	for (int i = 0; i < writeTimes && !shutdown; i++) {
		if (i % 6 != 0) {
			PolarString value = generateValue(random);
			writeAValue(engine, keys.at(random() % keys.size()),value, maps, keys);
		} else {	
			PolarString key = generateAKey(random);
			PolarString value = generateValue(random);
			writeAValue(engine, key, value, maps, keys);
		}
	}
}

void writeTask2(Engine* engine, std::default_random_engine& random, 
	int writeTimes, std::map<PolarString, PolarString>& maps, 
	std::vector<PolarString>& keys) {
	for (int i = 0; i < writeTimes; i++) {
		if (i % 6 != 0) {
			PolarString value = generateValue(random);
			writeAValue(engine, keys.at(random() % keys.size()), value, maps, keys);
		} else {	
			PolarString key = generateAKey(random);
			PolarString value = generateValue(random);
			writeAValue(engine, key, value, maps, keys);
		}
	}
}



void testReader(Engine* engine, std::map<PolarString, PolarString>* maps, 
	std::vector<PolarString>* keys, int readerTime, std::default_random_engine& random) {
	for (int i = 0; i < readerTime; i++) {
		std::string value;
		RetCode code = RetCode::kSucc;
		PolarString key;
		if (i % 2 == 0) {
			key = keys->at(i);
			code = engine->Read(key, &value);
		} else {
			key = generateAKey(random);
			code = engine->Read(key, &value);
		}
		if (code == 0) {
			std::map<PolarString, PolarString>::iterator ite = maps->find(key);
			if (ite != maps->end()) {
				if (ite->second.compare(value) != 0) {
					fprintf(stderr, "find an unmatching key\n");
				}
			} else {
				fprintf(stderr, "find a doesn't exist key\n");
			}
		} else {
			if (maps->count(key) != 0) {
				fprintf(stderr, "error couldn't find key\n");
			}
		}
	}
}

class ReaderPro {
	
public :
	ReaderPro (Engine* engine, std::map<PolarString, PolarString>* maps, 
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
		this->groups = new std::thread*[threadSize];
		fprintf(stderr, "start reading\n");
		for (int i = 0; i < threadSize; i++) {
			this->groups[i] = new std::thread(testReader, this->engine, 
				this->maps, this->keys, readerTime, std::ref(this->random));
		}
		for (int i = 0; i < threadSize; i++) {
			this->groups[i]->join();
		}
		fprintf(stderr, "end reading\n");
		for (int i = 0; i < threadSize; i++)
			delete this->groups[i];
		delete[] this->groups;
		this->groups = NULL;
	}
	
	
private:
	int threadSize;
	Engine* engine;
	std::map<PolarString, PolarString>* maps;
	std::thread** groups;
	std::default_random_engine random;
	std::vector<PolarString>* keys;
};


int main(int argc, char** argv) {
	// test_with_kill threadSize writing_time
	fprintf(stderr, "Correctness Test\n");
	int threadSize = atoi(argv[1]);
	int writingTime = atoi(argv[2]);
	std::string path(kDumpPath);
	Engine* engine = NULL;
	Engine::Open(path, &engine);
	delete engine; // finilize.
	EngineRace::Open(path, &engine);
	WriterTask writeTask(engine);
	writeTask.startKillableWriter(threadSize, writingTime);
	std::this_thread::sleep_for (std::chrono::seconds(10)); // sleeping for ten seconds.
	shutdown = true;
	writeTask.waitThreadEnd();
	ReaderPro readerPro(engine, &writeTask.maps, &writeTask.keys);
	readerPro.startReader(threadSize, writingTime);
	delete engine; // finilize.
	
	return 0;
}