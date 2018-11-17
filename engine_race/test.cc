#include <assert.h>
#include <stdio.h>
#include <string>
#include "include/engine.h"
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
  DumpVisitor(int* kcnt)
    : key_cnt_(kcnt) {}

  ~DumpVisitor() {}

  void Visit(const PolarString& key, const PolarString& value) {
    printf("Visit %s --> %s\n", key.data(), value.data());
    (*key_cnt_)++;
  }
  
private:
  int* key_cnt_;
};

int main() {
  Engine *engine = NULL;
  std::string path(kDumpPath);
  RetCode ret = Engine::Open(path, &engine);
  assert (ret == RetCode::kSucc);

  ret = engine->Write("aaaaaaaa", "aaaaaaaaaaa");
  assert (ret == RetCode::kSucc);
  ret = engine->Write("aaaaaaaa", "111111111111111111111111111111111111111111");
  ret = engine->Write("bbbbbbbb", "2222222");
  ret = engine->Write("cccccccc", "33333333333333333333");
  ret = engine->Write("dddddddd", "4");

  ret = engine->Write("dddddddd", "bbbbbbbbbbbb");
  assert (ret == RetCode::kSucc);

  ret = engine->Write("cccccccc", "cbbbbbbbbbbbb");
  assert (ret == RetCode::kSucc);
  
  std::string value;
  ret = engine->Read("aaaaaaaa", &value);
  fprintf(stderr, "Read aaaaaaaa value: %s\n", value.c_str());
  
  ret = engine->Read("bbbbbbbb", &value);
  assert (ret == RetCode::kSucc);
  fprintf(stderr, "Read bbbbbbbb value: %s\n", value.c_str());
  
  ret = engine->Read("cccccccc", &value);
  assert (ret == RetCode::kSucc);
  fprintf(stderr, "Read cccccccc value: %s\n", value.c_str());
  
  ret = engine->Read("dddddddd", &value);
  assert (ret == RetCode::kSucc);
  fprintf(stderr, "Read dddddddd value: %s\n", value.c_str());

  delete engine;

  return 0;
}
