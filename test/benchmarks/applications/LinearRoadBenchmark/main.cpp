#include <iostream>
#include <vector>

#include "LRB1.cpp"
#include "LRB2.cpp"

int main(int argc, const char **argv) {
  BenchmarkQuery *benchmarkQuery = nullptr;

  SystemConf::getInstance().QUERY_NUM = 2;
  BenchmarkQuery::parseCommandLineArguments(argc, argv);

  if (SystemConf::getInstance().QUERY_NUM == 1) {
    benchmarkQuery = new LRB1();
  } else if (SystemConf::getInstance().QUERY_NUM == 2) {
    benchmarkQuery = new LRB2();
  } else {
    throw std::runtime_error("error: invalid benchmark query id");
  }

  return benchmarkQuery->runBenchmark();
}