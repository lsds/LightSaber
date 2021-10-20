#pragma once

#include <arpa/inet.h>
#include <sys/socket.h>

#include <cstring>

#include "BenchmarkQuery.h"
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


/*
 * This is a remote source that loads and replays the data from the
 * test/benchmarks/applications folder.
 *
 * */
class RemoteSource {
 protected:
  std::string m_name;
  long m_timestampReference = 0;
  long m_lastTimestamp = 0;
  int m_sock = 0;
  int m_sock2 = 0;

 private:
  std::unique_ptr<BenchmarkQuery> m_benchmarkQuery = nullptr;
  const long m_duration = 60 * 1;
  const long m_changeDuration = 10;
  long m_prevThrTime = 0, m_thrTime = 0;
  double m_Bytes{};

  struct DataSlot;
  std::vector<DataSlot> m_initialSlots;
  std::vector<DataSlot> m_slots;
  std::vector<std::thread *> m_workers;
  std::atomic<bool> m_stop = false;
  int m_nextSlot = 0;
  bool m_first = true;

  const bool m_debug = false;

 public:
  long getTimestampReference() { return m_timestampReference; }
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
      } else if (strcmp(argv[i], "--ingestion") == 0) {
        SystemConf::getInstance().MBs_INGESTED_PER_SEC = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--two-sources") == 0) {
        SystemConf::getInstance().HAS_TWO_SOURCES = (strcasecmp(argv[j], "true") == 0 ||
            std::atoi(argv[j]) != 0);
      } else if (strcmp(argv[i], "--send-second") == 0) {
      SystemConf::getInstance().SEND_TO_SECOND_WORKER = (strcasecmp(argv[j], "true") == 0 ||
          std::atoi(argv[j]) != 0);
      } else {
        std::string argument(argv[i]);
        throw std::runtime_error("error: unknown argument " + argument);
      }
      i = j + 1;
    }
  }

  std::unique_ptr<BenchmarkQuery> getBenchmark() {
    switch (SystemConf::getInstance().QUERY_NUM) {
      case 0:
        return std::make_unique<CM1>(true, false);
      case 1:
        return std::make_unique<CM2>(true, false);
      case 2:
        return std::make_unique<SG1>(true, false);
      case 3:
      case 4:
        return std::make_unique<SG2>(true, false);
      case 5:
        return std::make_unique<LRB1>(true, false);
      case 6:
      case 7:
        return std::make_unique<LRB2>(true, false);
      case 8:
        return std::make_unique<YSB>(true, false);
      case 9:
        return std::make_unique<ME1>(true, false);
      case 10:
        return std::make_unique<NBQ5>(true, false);
      default:
        throw std::runtime_error("error: wrong query number");
    }
  }

  int run(int argc, const char **argv, bool terminate = true) {
    SystemConf::getInstance().QUERY_NUM = 0;
    parseCommandLineArguments(argc, argv);
    m_benchmarkQuery = getBenchmark();
    auto inputBuffer = m_benchmarkQuery->getInMemoryData();

    // prepare workers
    setupWorkers(inputBuffer);

    // setup socket
    if (!SystemConf::getInstance().SEND_TO_SECOND_WORKER) {
      std::cout << "Setting up 1st socket" << std::endl;
      setupSocket(m_sock, SystemConf::getInstance().REMOTE_WORKER);
    }
    if (SystemConf::getInstance().HAS_TWO_SOURCES || SystemConf::getInstance().SEND_TO_SECOND_WORKER) {
      std::cout << "Setting up 2nd socket" << std::endl;
      setupSocket(m_sock2, SystemConf::getInstance().REMOTE_WORKER_2);
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    if (SystemConf::getInstance().LATENCY_ON) {
      SystemConf::getInstance().DURATION = m_duration - 5;
    }
    long systemTimestamp = -1;
    long restartReference = 0;
    double remainingTime = 0.;
    bool stopRecovery = false;
    if (SystemConf::getInstance().MBs_INGESTED_PER_SEC > 0) {
      auto t2 = std::chrono::high_resolution_clock::now();
      auto time_span =
          std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
      m_prevThrTime = time_span.count();
      m_thrTime = time_span.count();
    }

    std::cout << "Start running " + m_benchmarkQuery->getApplicationName() +
                     " ..."
              << std::endl;
    try {
      while (true) {
        if (terminate || SystemConf::getInstance().MBs_INGESTED_PER_SEC > 0) {
          auto t2 = std::chrono::high_resolution_clock::now();
          if (SystemConf::getInstance().MBs_INGESTED_PER_SEC > 0 &&
              !SystemConf::getInstance().BUFFERED_LATENCY) {
            // std::cout << "Start limiting the throughput..." << std::endl;
            auto time_span =
                std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
            m_thrTime = time_span.count();
            m_Bytes += (double)inputBuffer->size();
            // std::this_thread::sleep_for(std::chrono::microseconds (1600));
            if ((m_thrTime - m_prevThrTime < 1000) &&
                m_Bytes >= SystemConf::getInstance().MBs_INGESTED_PER_SEC *
                               1024 * 1024) {
              auto sleepTime = ((m_prevThrTime + 1000) - m_thrTime) + 350;
              std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
              // std::cout << "[dat] " << " " << inputBuffer->size() << " "
              //          << m_Bytes << " " <<
              //          SystemConf::getInstance().MBs_INGESTED_PER_SEC
              //          << " " << sleepTime << std::endl;
              m_prevThrTime = m_thrTime;
              m_Bytes = 0;
            } else if (m_thrTime - m_prevThrTime >= 1000) {
              m_prevThrTime = m_thrTime;
              m_Bytes = 0;
            }
          }
          auto time_span =
              std::chrono::duration_cast<std::chrono::duration<double>>(t2 -
                                                                        t1);
          if (terminate && time_span.count() >= (double)m_duration) {
            std::cout << "Stop running " +
                             m_benchmarkQuery->getApplicationName() + " ..."
                      << std::endl;
            return 0;
          }
        }
        if (SystemConf::getInstance().LATENCY_ON) {
          auto currentTime = std::chrono::high_resolution_clock::now();
          auto currentTimeNano =
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  currentTime.time_since_epoch())
                  .count();
          systemTimestamp =
              (long)((currentTimeNano - m_timestampReference) / 1000L);
        }

        // get next buffer
        inputBuffer = getNextSlot();

        // send data
        if (SystemConf::getInstance().HAS_TWO_SOURCES) {
          if (m_nextSlot % 2 == 0) {
            send(m_sock, inputBuffer->data(), inputBuffer->size(), 0);
          } else {
            send(m_sock2, inputBuffer->data(), inputBuffer->size(), 0);
          }
        } else {
          if (!SystemConf::getInstance().SEND_TO_SECOND_WORKER) {
            send(m_sock, inputBuffer->data(), inputBuffer->size(), 0);
          } else {
            send(m_sock2, inputBuffer->data(), inputBuffer->size(), 0);
          }
        }

        // return the buffer to the pool
        advanceSlot();
      }
    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
      exit(1);
    }
  }

 private:
  static void setupSocket(int &sock, const std::string &remoteIp) {
    struct sockaddr_in serv_addr {};
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
      throw std::runtime_error("error: Socket creation error");
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    // Convert IPv4 and IPv6 addresses from text to binary form
    if (inet_pton(AF_INET, remoteIp.c_str(), &serv_addr.sin_addr) <= 0) {
      throw std::runtime_error("error: Invalid address/ Address not supported");
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
      throw std::runtime_error("error: Connection Failed");
    }
  }

  void setupWorkers(std::vector<char> *buffer) {
    auto size = buffer->size();
    auto offset = m_benchmarkQuery->getEndTimestamp() -
                  m_benchmarkQuery->getStartTimestamp() + 1;
    if (offset <= 0) {
      std::cout << "warning: the starting offset is " << offset << std::endl;
    }
    auto curOffset = 0;
    int idx = 0;
    m_initialSlots.resize(SystemConf::getInstance().WORKER_THREADS * 2);
    m_slots.resize(SystemConf::getInstance().WORKER_THREADS * 2);
    auto finalOffset = offset * m_slots.size();
    for (auto &slot : m_slots) {
      slot.m_buffer.resize(size);
      std::memcpy(slot.m_buffer.data(), buffer->data(), size);
      auto tupleSize = m_benchmarkQuery->getSchema()->getTupleSize();
      auto startPos = 0;
      auto endPos = slot.m_buffer.size() / sizeof(long);
      auto step = tupleSize / sizeof(long);
      auto buf = (long *)slot.m_buffer.data();
      for (unsigned long i = startPos; i < endPos; i += step) {
        buf[i] += curOffset;
      }
      // the first data sent
      m_initialSlots[idx].m_buffer.resize(size);
      std::memcpy(m_initialSlots[idx].m_buffer.data(), slot.m_buffer.data(),
                  size);

      curOffset += offset;
      slot.m_offset = finalOffset;
      slot.m_state = new std::atomic<int>(0);
      idx++;
    }

    m_workers.resize(SystemConf::getInstance().WORKER_THREADS);
    for (int t = 0; t < m_workers.size(); t++) {
      m_workers[t] = new std::thread([&, t] {
        auto thread = t;
        auto idx = t;

        while (!m_slots[idx].m_state)
          ;

        while (!m_stop) {
          auto oldVal = 0;
          while (!m_slots[idx].m_state->compare_exchange_weak(oldVal, 1)) {
            if (m_debug) {
              // std::cout << "Worker " + std::to_string(thread) + " waiting for
              // " + std::to_string(idx) + " slot." << std::endl;
            }
            oldVal = 0;
            _mm_pause();
          }

          if (m_debug) {
            std::cout << "Worker " + std::to_string(thread) + " updating " +
                             std::to_string(idx) + " slot."
                      << std::endl;
          }

          auto tupleSize = m_benchmarkQuery->getSchema()->getTupleSize();
          auto startPos = 0;
          auto endPos = m_slots[idx].m_buffer.size() / sizeof(long);
          auto step = tupleSize / sizeof(long);
          auto buf = (long *)m_slots[idx].m_buffer.data();
          for (unsigned long i = startPos; i < endPos; i += step) {
            buf[i] += m_slots[idx].m_offset;
          }

          m_slots[idx].m_state->store(2);
          idx += SystemConf::getInstance().WORKER_THREADS;
          if (idx >= m_slots.size()) {
            idx = t;
          }
        }
      });
      auto core = (SystemConf::getInstance().SEND_TO_SECOND_WORKER) ? t+2 : t;
      Utils::bindProcess(*m_workers[t], core);
    }
  }

  std::vector<char> *getNextSlot() {
    if (m_first) {
      return &m_initialSlots[m_nextSlot].m_buffer;
    }
    auto oldVal = 2;
    while (!m_slots[m_nextSlot].m_state->compare_exchange_weak(oldVal, 3)) {
      if (m_debug) {
        // std::cout << "Waiting to get " + std::to_string(m_nextSlot) + "
        // slot"<< std::endl;
      }
      oldVal = 2;
      _mm_pause();
    }
    if (m_debug) {
      auto buf = (long *)m_slots[m_nextSlot].m_buffer.data();
      std::cout << "Sending " + std::to_string(m_nextSlot) + " slot with " +
                       std::to_string(buf[0]) + " starting timestamp"
                << std::endl;
    }
    return &m_slots[m_nextSlot].m_buffer;
  }

  void advanceSlot() {
    if (m_first) {
      m_nextSlot++;
      if (m_nextSlot >= m_slots.size()) {
        m_first = false;
        m_nextSlot = 0;
      }
      return;
    }
    if (m_debug) {
      std::cout << "Resetting " + std::to_string(m_nextSlot) + " slot"
                << std::endl;
    }
    m_slots[m_nextSlot].m_state->store(0);
    m_nextSlot++;
    if (m_nextSlot >= m_slots.size()) {
      m_nextSlot = 0;
    }
  }

  struct DataSlot {
    std::vector<char> m_buffer;
    long m_offset = 0;
    std::atomic<int> *m_state = nullptr;
  };
};

int main(int argc, const char **argv) {
  std::unique_ptr<RemoteSource> remoteSource = std::make_unique<RemoteSource>();
  remoteSource->run(argc, argv);
}