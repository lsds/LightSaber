#include <iostream>
#include <vector>

#include "CM1.cpp"
#include "CM2.cpp"

int main(int argc, const char **argv) {
  BenchmarkQuery *benchmarkQuery = nullptr;

  SystemConf::getInstance().QUERY_NUM = 1;
  BenchmarkQuery::parseCommandLineArguments(argc, argv);

  if (SystemConf::getInstance().QUERY_NUM == 1) {
    benchmarkQuery = new CM1();
  } else if (SystemConf::getInstance().QUERY_NUM == 2) {
    benchmarkQuery = new CM2();
  } else {
    throw std::runtime_error("error: invalid benchmark query id");
  }

  return benchmarkQuery->runBenchmark();
}