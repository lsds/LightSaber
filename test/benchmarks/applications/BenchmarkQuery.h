#pragma once

#include <buffers/UnboundedQueryBufferFactory.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#if defined(RDMA_INPUT)
#include "RDMA/infinity/infinity.h"
#include "buffers/RDMABufferPool.h"
infinity::core::Context *m_context;
infinity::queues::QueuePairFactory *m_qpFactory;
infinity::queues::QueuePair *m_qp;
infinity::memory::Buffer **m_receiveBuffers;
#endif

#include <cstring>
//#include <random>

#include "utils/TupleSchema.h"
#include "utils/QueryApplication.h"
#include "utils/SystemConf.h"

// ./cluster_monitoring --circular-size 8388608 --unbounded-size 1048576 (524288) --batch-size 524288 --bundle-size 524288 --query 1 --threads 1
// ./cluster_monitoring --circular-size 8388608 --unbounded-size 1048576 (524288) --batch-size 524288 --bundle-size 524288 --query 2 --threads 1
// ./smartgrid --query 1 --unbounded-size 262144 --batch-size 524288 --circular-size 16777216 --bundle-size 524288 --slots 128 --threads 1
// ./smartgrid --query 2 --hashtable-size 512 --unbounded-size 1048576 --circular-size 16777216 --bundle-size 524288  --slots 128 --batch-size 524288 --unbounded-size 4194304 --threads 1
// ./linear_road_benchmark --unbounded-size 4194304 --circular-size 16777216 --batch-size 524288 --bundle-size 524288 --query 1 --hashtable-size 256 --threads 1
// ./linear_road_benchmark --unbounded-size 16777216 --circular-size 16777216 --batch-size 262144 --bundle-size 262144 --query 2 --threads 1
// ./yahoo_benchmark --circular-size 8388608 --slots 128 --batch-size 524288 --bundle-size 524288 --threads 1
// ./manufacturing_equipment --query 1 --unbounded-size 4096 --batch-size 262144 --circular-size 16777216 --bundle-size 262144 --slots 128 --threads 1
// ./nexmark --query 1 --circular-size 33554432 --batch-size 1048576 --bundle-size 1048576 --unbounded-size 262144 --latency tru --parallel-merge true --threads 15

class BenchmarkQuery {
 protected:
  std::string m_name;
  long m_timestampReference = 0;
  long m_lastTimestamp = 0;
  long m_startTimestamp = 0;
  long m_endTimestamp = 0;
  int m_sock = 0;
  int m_server_fd;
 private:
  const long m_duration = 60 * 1; // in seconds, i.e. 60 = 60 secs
  const long m_changeDuration = 10;
  long  m_prevThrTime = 0, m_thrTime = 0;
  double m_Bytes;
 public:
  std::string getApplicationName() {
    return m_name;
  }
  virtual QueryApplication *getApplication() = 0;
  long getTimestampReference() { return m_timestampReference; }
  long getStartTimestamp() { return m_startTimestamp; }
  long getEndTimestamp() { return m_endTimestamp; }
  virtual TupleSchema *getSchema() = 0;
  virtual std::vector<char> *getInMemoryData() = 0;
  virtual std::vector<char> *getSecondInMemoryData() { throw std::runtime_error("error: the function is not implemented"); };
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
        SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--unbounded-size") == 0) {
        SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--output-size") == 0) {
        SystemConf::getInstance().OUTPUT_BUFFER_SIZE = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--hashtable-size") == 0) {
        SystemConf::getInstance().HASH_TABLE_SIZE = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--slots") == 0) {
        SystemConf::getInstance().SLOTS = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--partial-windows") == 0) {
        SystemConf::getInstance().PARTIAL_WINDOWS = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--parallel-merge") == 0) {
        SystemConf::getInstance().PARALLEL_MERGE_ON =
            (strcasecmp(argv[j], "true") == 0 || std::atoi(argv[j]) != 0);
      } else if (strcmp(argv[i], "--performance-monitor-interval") == 0) {
        SystemConf::getInstance().PERFORMANCE_MONITOR_INTERVAL =
            std::stoul(argv[j]);
      } else if (strcmp(argv[i], "--latency") == 0) {
        SystemConf::getInstance().LATENCY_ON =
            (strcasecmp(argv[j], "true") == 0 || std::atoi(argv[j]) != 0);
      }  else if (strcmp(argv[i], "--compression-monitor-interval") == 0) {
        SystemConf::getInstance().COMPRESSION_MONITOR_INTERVAL =
            std::stoul(argv[j]);
      } else if (strcmp(argv[i], "--checkpoint-duration") == 0) {
        SystemConf::getInstance().CHECKPOINT_INTERVAL = std::stoi(argv[j]);
        SystemConf::getInstance().CHECKPOINT_ON =
            SystemConf::getInstance().CHECKPOINT_INTERVAL > 0;
      } else if (strcmp(argv[i], "--failure") == 0) {
        SystemConf::getInstance().FAILURE_TIME = std::stoi(argv[j]);
        SystemConf::getInstance().FAILURE_ON =
            SystemConf::getInstance().FAILURE_TIME > 0;
      } else if (strcmp(argv[i], "--disk-block-size") == 0) {
        SystemConf::getInstance().BLOCK_SIZE = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--create-merge") == 0) {
        SystemConf::getInstance().CREATE_MERGE_WITH_CHECKPOINTS = (strcasecmp(argv[j], "true") == 0 ||
            std::atoi(argv[j]) != 0);
      } else if (strcmp(argv[i], "--checkpoint-compression") == 0) {
        SystemConf::getInstance().CHECKPOINT_COMPRESSION = (strcasecmp(argv[j], "true") == 0 ||
            std::atoi(argv[j]) != 0);
      } else if (strcmp(argv[i], "--persist-input") == 0) {
        SystemConf::getInstance().PERSIST_INPUT = (strcasecmp(argv[j], "true") == 0 ||
            std::atoi(argv[j]) != 0);
      } else if (strcmp(argv[i], "--lineage") == 0) {
        SystemConf::getInstance().LINEAGE_ON = (strcasecmp(argv[j], "true") == 0 ||
            std::atoi(argv[j]) != 0);
      } else if (strcmp(argv[i], "--recover") == 0) {
        SystemConf::getInstance().RECOVER = (strcasecmp(argv[j], "true") == 0 ||
            std::atoi(argv[j]) != 0);
      } else if (strcmp(argv[i], "--adaptive-compression") == 0) {
        SystemConf::getInstance().ADAPTIVE_COMPRESSION_ON = (strcasecmp(argv[j], "true") == 0 ||
            std::atoi(argv[j]) != 0);
      } else if (strcmp(argv[i], "--adaptive-interval") == 0) {
        SystemConf::getInstance().ADAPTIVE_COMPRESSION_INTERVAL = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--adaptive-data") == 0) {
        SystemConf::getInstance().ADAPTIVE_CHANGE_DATA = true;
      } else if (strcmp(argv[i], "--campaign-num") == 0) {
        SystemConf::getInstance().CAMPAIGNS_NUM = std::stoi(argv[j]);
      } else if (strcmp(argv[i], "--ingestion") == 0) {
        SystemConf::getInstance().MBs_INGESTED_PER_SEC = std::stoi(argv[j]);
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
    std::vector<char> * alterInputBuffer = nullptr;
    auto application = getApplication();
    if (SystemConf::getInstance().ADAPTIVE_CHANGE_DATA) {
      alterInputBuffer = getSecondInMemoryData();
      auto tempBuffer = inputBuffer;
      inputBuffer = alterInputBuffer;
      alterInputBuffer = tempBuffer;
    }
    if (SystemConf::getInstance().LATENCY_ON) {
      SystemConf::getInstance().DURATION = m_duration - 5;
      if (SystemConf::getInstance().RECOVER) {
        m_timestampReference = application->getTimestampReference();
        m_lastTimestamp = application->getLastTimestamp();
        m_lastTimestamp = m_lastTimestamp + (m_timestampReference/1000L); // in usec
        std::cout << "Last timestamp was " << m_lastTimestamp << " usec" << std::endl;
        SystemConf::getInstance().BUFFERED_LATENCY = true;
      }
    }
    long systemTimestamp = -1;
    long restartReference = 0;
    double remainingTime = 0.;
    bool stopRecovery = false;
    if (SystemConf::getInstance().MBs_INGESTED_PER_SEC > 0) {
      auto t2 = std::chrono::high_resolution_clock::now();
      auto time_span = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
      m_prevThrTime = time_span.count();
      m_thrTime = time_span.count();
    }
    std::cout << "Start running " + getApplicationName() + " ..." << std::endl;
    try {
      while (true) {
        if (terminate || SystemConf::getInstance().MBs_INGESTED_PER_SEC > 0) {
          auto t2 = std::chrono::high_resolution_clock::now();
          if (SystemConf::getInstance().MBs_INGESTED_PER_SEC > 0 && !SystemConf::getInstance().BUFFERED_LATENCY) {
            //std::cout << "Start limiting the throughput..." << std::endl;
            auto time_span = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
            m_thrTime = time_span.count();
            m_Bytes += (double) inputBuffer->size();
            //std::this_thread::sleep_for(std::chrono::microseconds (1600));
            if ((m_thrTime - m_prevThrTime < 1000) &&
                m_Bytes >= SystemConf::getInstance().MBs_INGESTED_PER_SEC * 1024 * 1024) {
              auto sleepTime = ((m_prevThrTime+1000) - m_thrTime) + 350;
              std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
              // std::cout << "[dat] " << " " << inputBuffer->size() << " "
              //          << m_Bytes << " " << SystemConf::getInstance().MBs_INGESTED_PER_SEC
              //          << " " << sleepTime << std::endl;
              m_prevThrTime = m_thrTime;
              m_Bytes = 0;
            } else if (m_thrTime - m_prevThrTime >= 1000) {
              m_prevThrTime = m_thrTime;
              m_Bytes = 0;
            }
          }
          auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
          if (terminate && time_span.count() >= (double) m_duration) {
            std::cout << "Stop running " + getApplicationName() + " ..." << std::endl;
            return 0;
          }
          if (SystemConf::getInstance().FAILURE_ON &&
              time_span.count() >= (double) SystemConf::getInstance().FAILURE_TIME) {
            std::cout << "Killing " + getApplicationName() + " ..." << std::endl;
            //application->~QueryApplication();
            return 0;
          }
        }
        if (SystemConf::getInstance().LATENCY_ON) {
          auto currentTime = std::chrono::high_resolution_clock::now();
          auto currentTimeNano =
              std::chrono::duration_cast<std::chrono::nanoseconds>(currentTime.time_since_epoch()).count();
          systemTimestamp = (long)((currentTimeNano - m_timestampReference) / 1000L);
          if (SystemConf::getInstance().BUFFERED_LATENCY) {
            if (restartReference == 0) {
              restartReference = currentTimeNano;
              auto dt = (restartReference / 1000L) - m_lastTimestamp; // in usec
              remainingTime = dt / 1000.; // in ms
              std::cout << "Ingesting buffered data for " + std::to_string(remainingTime) + " ms" << std::endl;
            }
            double diff = ((double)(currentTimeNano - restartReference) / 1000L) / 1000.; // in ms
            if (diff > remainingTime) {
              SystemConf::getInstance().BUFFERED_LATENCY = false;
              std::cout << "Finished ingesting buffered data" << std::endl;
            } else {
              auto currentTimeUsec = currentTimeNano/1000L + (long) (diff - remainingTime) * 1000L;
              systemTimestamp = (long)(currentTimeUsec - (m_timestampReference / 1000L));
            }
          }
        }
        if (SystemConf::getInstance().RECOVER && !stopRecovery) {
          application->recoverData();
          stopRecovery = true;
        }
        if (SystemConf::getInstance().ADAPTIVE_CHANGE_DATA) {
          auto t2 = std::chrono::high_resolution_clock::now();
          auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
          if (time_span.count() >= (double) m_changeDuration) {
            std::cout << "Changing data..." << std::endl;
            auto tempBuffer = inputBuffer;
            inputBuffer = alterInputBuffer;
            alterInputBuffer = tempBuffer;
            SystemConf::getInstance().ADAPTIVE_CHANGE_DATA = false;
          }
        }

#if defined(TCP_INPUT)
        // read data from a remote source
        auto buffer = getInputData();
        application->processData(buffer, systemTimestamp);
#elif defined(RDMA_INPUT)
        // read data from a remote source
        auto buffer = getRDMABuffer();
        application->processData((void *)buffer, inputBuffer->size(), systemTimestamp);
#else
        application->processData(*inputBuffer, systemTimestamp);
#endif
      }
    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
      exit(1);
    }
  }

  int runTwoStreamsBenchmark(bool terminate = true) {
    auto t1 = std::chrono::high_resolution_clock::now();
    auto inputBuffer1 = getInMemoryData();
    auto inputBuffer2 = getSecondInMemoryData();
    auto application = getApplication();
    if (SystemConf::getInstance().LATENCY_ON) {
      SystemConf::getInstance().DURATION = m_duration - 5;
    }
    long systemTimestamp = -1;
    std::cout << "Start running " + getApplicationName() + " ..." << std::endl;

    /*struct iSchema {
      long timestamp;
      int attr1;
      int attr2;
    };
    auto p1 = (iSchema*) inputBuffer1->data();
    auto p2 = (iSchema*) inputBuffer2->data();
    size_t len = inputBuffer1->size() / sizeof(iSchema);
    long lastTimestamp = p1[len - 1].timestamp;*/

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
        application->processFirstStream(*inputBuffer1, systemTimestamp);
        application->processSecondStream(*inputBuffer2, systemTimestamp);

        // update timestamps
        /*for (size_t i = 0; i < len/8; i+=4) {
          p1[i].attr2 += 1;
          p2[i].attr2 += 1;
        }*/
      }
    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
      exit(1);
    }
  }

 private:
  inline std::shared_ptr<UnboundedQueryBuffer> getInputData() {
#if defined(TCP_INPUT)
    //if (!buffer) {
    //  throw std::runtime_error("error: buffer in a nullptr");
    //}
    auto buffer = UnboundedQueryBufferFactory::getInstance().newNBInstance(0);
    if (m_sock == 0) {
      std::cout << "[DBG] setting up the tcp socket" << std::endl;
      setupSocket();
    }
    readBytes(m_sock, buffer->getBuffer().size(), buffer->getBuffer().data());

    /*struct _InputSchema_128 {
      long timestamp;
      long padding_0;
      __uint128_t user_id;
      __uint128_t page_id;
      __uint128_t ad_id;
      long ad_type;
      long event_type;
      __uint128_t ip_address;
      __uint128_t padding_1;
      __uint128_t padding_2;
    };
    auto d = (_InputSchema_128 *) buffer->getBuffer().data();
    for (auto ii = 0; ii < buffer->getBuffer().size()/128; ii++) {
      //std::cout << d[ii].timestamp << " "
      //    << (int) d[ii].ad_id << " "
      //    << (int) d[ii].ad_type << " "
      //    << (int) d[ii].event_type << " " << std::endl;
      if (ii == 0)
        break;
    }*/
    //std::cout << "end" << std::endl;
    return buffer;
#else
    // do nothing
    return nullptr;
#endif
  }

#if defined(RDMA_INPUT)
  inline infinity::core::receive_element_t *getRDMABuffer() {
    infinity::core::receive_element_t *receiveElement = new infinity::core::receive_element_t;
    if (!m_context) {
      auto device = (SystemConf::getInstance().REMOTE_WORKER == SystemConf::WALLABY_ib1) ? 1 : 0;
      m_context = new infinity::core::Context(device);
      m_qpFactory = new  infinity::queues::QueuePairFactory(m_context);

      std::cout <<"Creating buffers to receive a messages" << std::endl;
      m_receiveBuffers = new infinity::memory::Buffer *[BUFFER_COUNT];
      for (uint32_t i = 0; i < BUFFER_COUNT; ++i) {
        m_receiveBuffers[i] = new infinity::memory::Buffer(m_context, SystemConf::getInstance().BUNDLE_SIZE * sizeof(char));
        m_context->postReceiveBuffer(m_receiveBuffers[i]);
      }

      std::cout <<"Waiting for incoming connection" << std::endl;
      m_qpFactory->bindToPort(PORT);
      m_qp = m_qpFactory->acceptIncomingConnection();
      while (!m_context->receive(receiveElement));
      m_context->postReceiveBuffer(receiveElement->buffer);
      // setup rdma context
      RDMABufferPool::getInstance().setContext(m_context);
    }
    while (!m_context->receive(receiveElement));
    return receiveElement;
  }
#endif

  inline void setupSocket() {
    struct sockaddr_in address {};
    int opt = 1;
    int addrlen = sizeof(address);
    // Creating socket file descriptor
    if ((m_server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
      throw std::runtime_error("error: Socket file descriptor creation error");
    }

    // Forcefully attaching socket to the PORT
    if (setsockopt(m_server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                   sizeof(opt))) {
      throw std::runtime_error("error: setsockopt");
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    // Forcefully attaching socket to the PORT
    if (bind(m_server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
      throw std::runtime_error("error: bind failed");
    }
    if (listen(m_server_fd, 3) < 0) {
      throw std::runtime_error("error: listen");
    }

    std::cout << "[DBG] Waiting for a tcp connection" << std::endl;
    // todo: accept multiple connections
    if ((m_sock = accept(m_server_fd, (struct sockaddr *)&address,
                         (socklen_t *)&addrlen)) < 0) {
      throw std::runtime_error("error: accept");
    }
  }

  inline void readBytes(int socket, unsigned int length, void *buffer) {
    unsigned int bytesRead = 0;
    while (bytesRead < length) {
      auto valread =
          read(socket, (char *)buffer + bytesRead, length - bytesRead);
      assert(valread >= 0);
      bytesRead += valread;
    }
  }
};