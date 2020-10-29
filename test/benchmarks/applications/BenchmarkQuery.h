#pragma once

#include <cstring>

#include "utils/TupleSchema.h"
#include "utils/QueryApplication.h"
#include "utils/SystemConf.h"

// ./cluster_monitoring --circular-size 8388608 --unbounded-size 1048576 --batch-size 524288 --bundle-size 524288 --query 1 --threads 1
// ./cluster_monitoring --circular-size 8388608 --unbounded-size 1048576 --batch-size 524288 --bundle-size 524288 --query 2 --threads 1
// ./smartgrid --query 1 --unbounded-size 262144 --batch-size 524288 --circular-size 16777216 --bundle-size 524288 --slots 128 --threads 1
// ./smartgrid --query 2 --hashtable-size 512 --unbounded-size 1048576 --circular-size 16777216 --bundle-size 524288  --slots 128 --batch-size 524288 --unbounded-size 4194304 --threads 1
// ./linear_road_benchmark --unbounded-size 4194304 --circular-size 16777216 --batch-size 524288 --bundle-size 524288 --query 1 --hashtable-size 256 --threads 1
// ./linear_road_benchmark --unbounded-size 16777216 --circular-size 16777216 --batch-size 262144 --bundle-size 262144 --query 2 --threads 1
// ./yahoo_benchmark --circular-size 8388608 --slots 128 --batch-size 524288 --bundle-size 524288 --threads 1
// ./manufacturing_equipment --query 1 --unbounded-size 4096 --batch-size 262144 --circular-size 16777216 --bundle-size 262144 --slots 128 --threads 1

class BenchmarkQuery {
 protected:
  std::string m_name;
  long m_timestampReference = 0;
 private:
  const long m_duration = 60 * 10;
 public:
  std::string getApplicationName() {
    return m_name;
  }
  virtual QueryApplication *getApplication() = 0;
  virtual TupleSchema *getSchema() = 0;
  virtual std::vector<char> *getInMemoryData() = 0;
  virtual std::vector<char> *getStaticData() = 0;
  static void parseCommandLineArguments(int argc, const char **argv) {
    int i, j;
    for (i = 1; i < argc;) {
      if ((j = i + 1) == argc) {
        throw std::runtime_error("error: wrong number of arguments");
      }
      if (strcmp(argv[i], "--threads") == 0) {
        SystemConf::getInstance().WORKER_THREADS = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--batch-size") == 0) {
        SystemConf::getInstance().BATCH_SIZE = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--bundle-size") == 0) {
        SystemConf::getInstance().BUNDLE_SIZE = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--input-size") == 0) {
        SystemConf::getInstance().INPUT_SIZE = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--query") == 0) {
        SystemConf::getInstance().QUERY_NUM = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--circular-size") == 0) {
        SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = std::stoul(argv[j]);
      } else if (strcmp(argv[i], "--unbounded-size") == 0) {
        SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE = std::stoul(argv[j]);
      } else if (strcmp(argv[i], "--hashtable-size") == 0) {
        SystemConf::getInstance().HASH_TABLE_SIZE = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--slots") == 0) {
        SystemConf::getInstance().SLOTS = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--partial-windows") == 0) {
        SystemConf::getInstance().PARTIAL_WINDOWS = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--parallel-merge") == 0) {
        SystemConf::getInstance().PARALLEL_MERGE_ON = (strcasecmp(argv[j], "true") == 0 ||
            std::atoi(argv[j]) != 0);
      } else if (strcmp(argv[i], "--latency") == 0) {
        SystemConf::getInstance().LATENCY_ON = (strcasecmp(argv[j], "true") == 0 ||
            std::atoi(argv[j]) != 0);
      } else {
        std::string argument(argv[i]);
        throw std::runtime_error("error: unknown argument " + argument);
      }
      i = j + 1;
    }
  }

  int runBenchmark(bool terminate = true) {
    auto t1 = std::chrono::high_resolution_clock::now();
    auto inputBuffer = getInMemoryData();
    auto application = getApplication();
    if (SystemConf::getInstance().LATENCY_ON) {
      SystemConf::getInstance().DURATION = m_duration - 3;
    }
    long systemTimestamp = -1;
    std::cout << "Start running " + getApplicationName() + " ..." << std::endl;
    try {
      while (true) {
        if (terminate) {
          auto t2 = std::chrono::high_resolution_clock::now();
          auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
          if (time_span.count() >= (double) m_duration) {
            std::cout << "Stop running " + getApplicationName() + " ..." << std::endl;
            return 0;
          }
        }
        if (SystemConf::getInstance().LATENCY_ON) {
          auto currentTime = std::chrono::high_resolution_clock::now();
          auto currentTimeNano =
              std::chrono::duration_cast<std::chrono::nanoseconds>(currentTime.time_since_epoch()).count();
          systemTimestamp = (long) ((currentTimeNano - m_timestampReference) / 1000L);
        }
        application->processData(*inputBuffer, systemTimestamp);
      }
    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
      exit(1);
    }
  }
};