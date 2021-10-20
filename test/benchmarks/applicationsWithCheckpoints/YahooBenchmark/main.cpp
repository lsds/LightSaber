#include <iostream>

#include "YSB.cpp"

// ./yahoo_benchmark_checkpoints --circular-size 16777216 --slots 128 --batch-size 524288 --bundle-size 524288 --checkpoint-duration 1000 --threads 1
int main(int argc, const char **argv) {
  std::unique_ptr<BenchmarkQuery> benchmarkQuery {};

  SystemConf::getInstance().QUERY_NUM = 1;
  BenchmarkQuery::parseCommandLineArguments(argc, argv);

  if (SystemConf::getInstance().QUERY_NUM == 1) {
    benchmarkQuery = std::make_unique<YSB>();
  } else {
    throw std::runtime_error("error: invalid benchmark query id");
  }

  return benchmarkQuery->runBenchmark();
}