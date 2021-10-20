#include <iostream>
#include <vector>

#include "SG1.cpp"
#include "SG2.cpp"
#include "SG3.cpp"

int main(int argc, const char **argv) {
  std::unique_ptr<BenchmarkQuery> benchmarkQuery {};

  SystemConf::getInstance().QUERY_NUM = 2;
  BenchmarkQuery::parseCommandLineArguments(argc, argv);

  if (SystemConf::getInstance().QUERY_NUM == 1) {
    benchmarkQuery = std::make_unique<SG1>();
  } else if (SystemConf::getInstance().QUERY_NUM == 2) {
    benchmarkQuery = std::make_unique<SG2>();
  } else if (SystemConf::getInstance().QUERY_NUM == 3) {
    benchmarkQuery = std::make_unique<SG3>();
  } else {
    throw std::runtime_error("error: invalid benchmark query id");
  }

  return benchmarkQuery->runBenchmark();
}