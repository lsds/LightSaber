#include <cstdlib>
#include <cstring>

#include "Kafka.h"
#include "benchmarks/applications/ClusterMonitoring/CM1.cpp"
#include "benchmarks/applications/ClusterMonitoring/CM2.cpp"
#include "benchmarks/applications/LinearRoadBenchmark/LRB1.cpp"
#include "benchmarks/applications/LinearRoadBenchmark/LRB2.cpp"
#include "benchmarks/applications/ManufacturingEquipment/ME1.cpp"
#include "benchmarks/applications/Nexmark/NBQ5.cpp"
#include "benchmarks/applications/SmartGrid/SG1.cpp"
#include "benchmarks/applications/SmartGrid/SG2.cpp"
#include "benchmarks/applications/YahooBenchmark/YSB.cpp"
#include "utils/SystemConf.h"
#include "utils/TupleSchema.h"

const bool terminate = true;
size_t m_bytes = 0;
size_t m_totalCnt = 0;
size_t m_totalBytes = 0;
size_t m_duration = 1 * 60;
long m_timestampReference = 0;

// kafka
// --bundle-size 62914560 --batch-size 62914560 --disk-block-size 4194304 --threads 16

static void parseCommandLineArguments(int argc, const char **argv) {
  int i, j;
  for (i = 1; i < argc;) {
    if ((j = i + 1) == argc) {
      throw std::runtime_error("error: wrong number of arguments");
    }
    if (strcmp(argv[i], "--threads") == 0) {
      SystemConf::getInstance().WORKER_THREADS = std::stoi(argv[j]);
      if (SystemConf::getInstance().WORKER_THREADS > 1) {
        SystemConf::getInstance().BATCH_SIZE = (SystemConf::getInstance().WORKER_THREADS - 1) * SystemConf::getInstance().BLOCK_SIZE;
        SystemConf::getInstance().BUNDLE_SIZE = SystemConf::getInstance().BATCH_SIZE;
      }
      std::cout << "BATCH_SIZE: " << SystemConf::getInstance().BATCH_SIZE << " / BUNDLE_SIZE: " << SystemConf::getInstance().BUNDLE_SIZE << std::endl;
    } else if (strcmp(argv[i], "--batch-size") == 0) {
      SystemConf::getInstance().BATCH_SIZE = std::stoi(argv[j]);
      SystemConf::getInstance().BUNDLE_SIZE = SystemConf::getInstance().BATCH_SIZE;
    } else if (strcmp(argv[i], "--bundle-size") == 0) {
      SystemConf::getInstance().BUNDLE_SIZE = std::stoi(argv[j]);
      SystemConf::getInstance().BATCH_SIZE = SystemConf::getInstance().BUNDLE_SIZE;
    } else if (strcmp(argv[i], "--disk-block-size") == 0) {
      SystemConf::getInstance().BLOCK_SIZE = std::stoi(argv[j]);
    } else if (strcmp(argv[i], "--input-size") == 0) {
      SystemConf::getInstance().INPUT_SIZE = std::stoi(argv[j]);
    } else if (strcmp(argv[i], "--query") == 0) {
      SystemConf::getInstance().QUERY_NUM = std::stoi(argv[j]);
    } else if (strcmp(argv[i], "--latency") == 0) {
      SystemConf::getInstance().LATENCY_ON =
          (strcasecmp(argv[j], "true") == 0 || std::atoi(argv[j]) != 0);
    } else if (strcmp(argv[i], "--use-flink") == 0) {
      SystemConf::getInstance().USE_FLINK =
          (strcasecmp(argv[j], "false") == 0 || std::atoi(argv[j]) == 0);
    } else if (strcmp(argv[i], "--use-kafka") == 0) {
      SystemConf::getInstance().USE_KAFKA =
          (strcasecmp(argv[j], "false") == 0 || std::atoi(argv[j]) == 0);
    } else if (strcmp(argv[i], "--disk-buffer") == 0) {
      SystemConf::getInstance().DISK_BUFFER = std::stoi(argv[j]);
      m_batchSize = SystemConf::getInstance().DISK_BUFFER;
    } else {
      std::string argument(argv[i]);
      throw std::runtime_error("error: unknown argument " + argument);
    }
    i = j + 1;
  }
}

template <typename query, typename InputSchema, typename hash>
void run() {
  auto benchmark = std::make_unique<query>(true, false);
  auto buffer = benchmark->getInMemoryData();

  std::vector<char> input (buffer->size());
  std::memcpy(input.data(), buffer->data(), buffer->size());

  // this is used for latency measurements
  m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();
  long latencyMark = -1;

  autoConsume = true;
  // initialize Kafka
  auto kafka = std::make_unique<Kafka<InputSchema, hash>>(SystemConf::getInstance().WORKER_THREADS, m_timestampReference, autoConsume);
  m_kafkaDuration = m_duration - 1;

  kafka->startWorkers();

  // run benchmark
  auto t1 = std::chrono::high_resolution_clock::now();
  auto _t1 = std::chrono::high_resolution_clock::now();

  std::cout << "Start running benchmark..." << std::endl;
  try {
    while (true) {
      if (terminate) {
        auto t2 = std::chrono::high_resolution_clock::now();
        auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - _t1);
        if (time_span.count() >= 1) {
          auto thr = (m_bytes / (1024 * 1024)) / time_span.count();
          m_totalBytes += thr;
          m_totalCnt++;
          m_bytes = 0;
          _t1 = t2;
          std::cout << "[DBG] " + std::to_string(thr) + " MB/s" +
              " [AVG: " + std::to_string(m_totalBytes/m_totalCnt) +  " MB/s " +
              std::to_string((m_totalBytes/sizeof(InputSchema) * 1024 * 1024)/m_totalCnt) + " tuples/s]" << std::endl;
        }
        time_span = std::chrono::duration_cast<std::chrono::duration<double>>(
            t2 - t1);
        if (time_span.count() >= (double)m_duration + 2) {
          kafka->measureLatency();
          std::cout << "Stop running the kafka benchmark..." << std::endl;
          return;
        }
      }

      if (SystemConf::getInstance().LATENCY_ON) {
        auto currentTime = std::chrono::high_resolution_clock::now();
        auto currentTimeNano =
            std::chrono::duration_cast<std::chrono::nanoseconds>(currentTime.time_since_epoch()).count();
        latencyMark = (long)((currentTimeNano - m_timestampReference) / 1000L);
      }

      // send data
      kafka->processPartitionedData(input, latencyMark);

      m_bytes += input.size();
    }
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
    exit(1);
  }

}

int main(int argc, const char **argv) {
  SystemConf::getInstance().QUERY_NUM = 0;
  parseCommandLineArguments(argc, argv);

  switch (SystemConf::getInstance().QUERY_NUM) {
    case 0:
      run<CM1,  CM1Query::InputSchema, CM1Query::hash> ();
      return 0;
    case 1:
      run<CM2,  CM2Query::InputSchema, CM2Query::hash> ();
      return 0;
    case 2:
      run<SG1, SG1Query::InputSchema, SG1Query::hash> ();
      return 0;
    case 3:
    case 4:
      run<SG2, SG2Query::InputSchema, SG2Query::hash> ();
      return 0;
    case 5:
      run<LRB1, LRB1Query::InputSchema, LRB1Query::hash> ();
      return 0;
    case 6:
    case 7:
      run<LRB2, LRB2Query::InputSchema, LRB2Query::hash> ();
      return 0;
    case 8:
      run<YSB, YSBQuery::InputSchema, YSBQuery::hash> ();
      return 0;
    case 9:
      run<ME1, ME1Query::InputSchema, ME1Query::hash> ();
      return 0;
    case 10:
      run<NBQ5, NBQ5Query::InputSchema, NBQ5Query::hash> ();
      return 0;
    default:
      throw std::runtime_error("error: wrong query number");
  }

}