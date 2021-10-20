#include <iostream>
#include <vector>

#include "CM1.cpp"
#include "CM2.cpp"

// ./cluster_monitoring_checkpoints --circular-size 16777216 --unbounded-size 1048576 (524288) --batch-size 524288 --bundle-size 524288 --query 1 --checkpoint-duration 1000 --disk-block-size 65536 --threads 1
// ./cluster_monitoring_checkpoints --circular-size 16777216 --unbounded-size 1048576 (524288) --batch-size 524288 --bundle-size 524288 --query 2 --checkpoint-duration 1000 --disk-block-size 131072 --threads 1
int main(int argc, const char **argv) {
  std::unique_ptr<BenchmarkQuery> benchmarkQuery {};

  SystemConf::getInstance().QUERY_NUM = 1;
  BenchmarkQuery::parseCommandLineArguments(argc, argv);

  if (SystemConf::getInstance().QUERY_NUM == 1) {
    benchmarkQuery = std::make_unique<CM1>();
  } else if (SystemConf::getInstance().QUERY_NUM == 2) {
    benchmarkQuery = std::make_unique<CM2>();
  } else {
    throw std::runtime_error("error: invalid benchmark query id");
  }

  return benchmarkQuery->runBenchmark();
}