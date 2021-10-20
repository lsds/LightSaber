#include <iostream>
#include <vector>

#include "LRB1.cpp"
#include "LRB2.cpp"
#include "LRB3.cpp"

// --unbounded-size 8388608 --circular-size 16777216 --batch-size 524288 --bundle-size 524288 --query 1 --hashtable-size 256 --checkpoint-duration 1000 --disk-block-size 16777216 --create-merge true --parallel-merge true --threads 1
// ./linear_road_benchmark_checkpoints --unbounded-size 4194304 --circular-size 16777216 --batch-size 262144 --bundle-size 262144 --query 1 --hashtable-size 256 --checkpoint-duration 1000 --disk-block-size 16777216 --create-merge true --threads 1
// ./linear_road_benchmark_checkpoints --unbounded-size 16777216 --circular-size 16777216 --batch-size 262144 --bundle-size 262144 --query 2 --checkpoint-duration 1000 --disk-block-size 8388608 --create-merge true --parallel-merge true --threads 1
int main(int argc, const char **argv) {
  std::unique_ptr<BenchmarkQuery> benchmarkQuery {};

  auto t1 = std::chrono::high_resolution_clock::now();
  SystemConf::getInstance().QUERY_NUM = 2;
  BenchmarkQuery::parseCommandLineArguments(argc, argv);

  if (SystemConf::getInstance().QUERY_NUM == 1) {
    benchmarkQuery = std::make_unique<LRB1>();
  } else if (SystemConf::getInstance().QUERY_NUM == 2) {
    benchmarkQuery = std::make_unique<LRB2>();
  } else if (SystemConf::getInstance().QUERY_NUM == 3) {
    benchmarkQuery = std::make_unique<LRB3>();
  } else {
    throw std::runtime_error("error: invalid benchmark query id");
  }

  auto t2 = std::chrono::high_resolution_clock::now();
  auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
  std::cout << "Setup duration: " + std::to_string(time_span.count()) << std::endl;

  //if (!SystemConf::getInstance().FAILURE_ON) {
    return benchmarkQuery->runBenchmark();
  /*} else {
    try {
      benchmarkQuery->runBenchmark();
      //std::system("pkill -9 -f "
      //    "/home/george/LightSaber/cmake-build-debug/test/benchmarks/applicationsWithCheckpoints/linear_road_benchmark_checkpoints");
    } catch (std::exception& e) {
      std::cerr << "Exception caught : " << e.what() << std::endl;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds (500));
    SystemConf::getInstance().RECOVER = true;
    std::unique_ptr<BenchmarkQuery> recoverQuery {};
    if (SystemConf::getInstance().QUERY_NUM == 1) {
      recoverQuery = std::make_unique<LRB1>();
    } else if (SystemConf::getInstance().QUERY_NUM == 2) {
      recoverQuery = std::make_unique<LRB2>();
    } else if (SystemConf::getInstance().QUERY_NUM == 3) {
      recoverQuery = std::make_unique<LRB3>();
    } else {
      throw std::runtime_error("error: invalid benchmark query id");
    }
    return recoverQuery->runBenchmark();
  }*/
}