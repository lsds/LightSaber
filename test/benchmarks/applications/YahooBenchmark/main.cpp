#include <iostream>
#include <vector>

#include "YSB.cpp"

int main(int argc, const char **argv) {
  BenchmarkQuery *benchmarkQuery = nullptr;

  SystemConf::getInstance().QUERY_NUM = 1;
  BenchmarkQuery::parseCommandLineArguments(argc, argv);

  if (SystemConf::getInstance().QUERY_NUM == 1) {
    benchmarkQuery = new YSB();
  } else {
    throw std::runtime_error("error: invalid benchmark query id");
  }

  return benchmarkQuery->runBenchmark();
}