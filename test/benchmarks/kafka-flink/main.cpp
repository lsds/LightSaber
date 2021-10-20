#include <cstdlib>
#include <cstring>

#include "Kafka.h"
#include "Flink.h"
#include "benchmarks/applications/BenchmarkQuery.h"
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
    } else if (strcmp(argv[i], "--bundle-size") == 0) {
      SystemConf::getInstance().BUNDLE_SIZE = std::stoi(argv[j]);
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
      m_flinkBatchSize = SystemConf::getInstance().DISK_BUFFER;
    } else if (strcmp(argv[i], "--use-checkpoints") == 0) {
      useCheckpoints = (strcasecmp(argv[j], "true") == 0 || std::atoi(argv[j]) == 1);
    } else {
      std::string argument(argv[i]);
      throw std::runtime_error("error: unknown argument " + argument);
    }
    i = j + 1;
  }
}

int main(int argc, const char **argv) {
  parseCommandLineArguments(argc, argv);

  auto benchmark = std::make_unique<YSB>(true, false);
  auto buffer = benchmark->getInMemoryData();
  YSBQuery::m_staticData[0] = benchmark->getStaticData();
  YSBQuery::m_timestampOffset = benchmark->getEndTimestamp();
  for (int w = 1; w < SystemConf::WORKER_THREADS; w++) {
    YSBQuery::m_staticData[w] = new std::vector<char>(YSBQuery::m_staticData[0]->size());
    std::memcpy(YSBQuery::m_staticData[w]->data(), YSBQuery::m_staticData[0]->data(), YSBQuery::m_staticData[0]->size());
  }
  std::vector<char> input (buffer->size());
  std::memcpy(input.data(), buffer->data(), buffer->size());

  // this is used for latency measurements
  m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();
  long latencyMark = -1;

  autoConsume = !SystemConf::getInstance().USE_KAFKA;
  // initialize Kafka
  auto kafka = std::make_unique<Kafka<YSBQuery::InputSchema, YSBQuery::hash>>(SystemConf::getInstance().WORKER_THREADS, m_timestampReference, false);
  m_kafkaDuration = m_duration - 1;

  // initialize Flink
  std::unique_ptr<FlinkYSB> flink = std::make_unique<FlinkYSB>(SystemConf::getInstance().WORKER_THREADS, m_timestampReference, autoConsume);
  m_flinkDuration = m_duration - 1;

  if (SystemConf::getInstance().USE_KAFKA) {
    flink->connect(kafka.get());
    kafka->startWorkers();
  }
  flink->startWorkers();

  // run benchmark
  auto t1 = std::chrono::high_resolution_clock::now();
  auto _t1 = std::chrono::high_resolution_clock::now();

  std::cout << "Start running Yahoo Benchmark..." << std::endl;
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
              std::to_string((m_totalBytes/sizeof(YSBQuery::InputSchema) * 1024 * 1024)/m_totalCnt) + " tuples/s]" << std::endl;
        }
        time_span = std::chrono::duration_cast<std::chrono::duration<double>>(
            t2 - t1);
        if (time_span.count() >= (double)m_duration + 2) {
          flink->measureLatency();
          std::cout << "Stop running the kafka-flink benchmark..." << std::endl;
          return 0;
        }
      }

      if (SystemConf::getInstance().LATENCY_ON) {
        auto currentTime = std::chrono::high_resolution_clock::now();
        auto currentTimeNano =
            std::chrono::duration_cast<std::chrono::nanoseconds>(currentTime.time_since_epoch()).count();
        latencyMark = (long)((currentTimeNano - m_timestampReference) / 1000L);
      }

      // send data
      if (SystemConf::getInstance().USE_KAFKA) {
        if (useCheckpoints) {
          kafka->processPartitionedDataWithCheckpoints(input, latencyMark);
        } else {
          kafka->processPartitionedData(input, latencyMark);
        }
      } else {
        flink->processPartitionedData(input, latencyMark);
      }
      m_bytes += input.size();
    }
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
    exit(1);
  }
}