#pragma once

#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include "RDMA/infinity/infinity.h"
#include "utils/SystemConf.h"
#include "utils/TupleSchema.h"
#include "utils/Utils.h"

static size_t sum = 0;
static std::unique_ptr<std::vector<char>> copyBuffer;

// --batch-size 1048576 --bundle-size 1048576
class RemoteRDMASink {
 protected:
  infinity::core::Context *m_context;
  infinity::queues::QueuePairFactory *m_qpFactory;
  infinity::queues::QueuePair *m_qp;

  infinity::memory::Buffer **m_receiveBuffers;

 private:
  const long m_duration = 60 * 1;
  const long m_changeDuration = 10;
  long m_prevThrTime = 0, m_thrTime = 0;
  double m_Bytes{};

 public:
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
      } else if (strcmp(argv[i], "--two-sources") == 0) {
        SystemConf::getInstance().HAS_TWO_SOURCES = (strcasecmp(argv[j], "true") == 0 ||
            std::atoi(argv[j]) != 0);
      } else {
        std::string argument(argv[i]);
        throw std::runtime_error("error: unknown argument " + argument);
      }
      i = j + 1;
    }
  }

  int run(int argc, const char **argv, bool terminate = true) {
    SystemConf::getInstance().QUERY_NUM = 0;
    parseCommandLineArguments(argc, argv);

    //auto core = (SystemConf::getInstance().REMOTE_CLIENT == SystemConf::KEA03_ib1 ||
    //              SystemConf::getInstance().REMOTE_CLIENT == SystemConf::KEA04_ib1) ? Utils::getFirstCoreFromSocket(1) : 2;
    auto core = 0;
    Utils::bindProcess(core);

    size_t len = SystemConf::getInstance().BUNDLE_SIZE;
    auto inputBuffer = new std::vector<char>(len);

    // setup socket
    setupSocket();

    copyBuffer = std::make_unique<std::vector<char>>(len);

    size_t idx = 0;
    auto t1 = std::chrono::high_resolution_clock::now();
    auto _t1 = std::chrono::high_resolution_clock::now();
    std::cout << "Start running the remote sink..." << std::endl;

    std::cout << "Waiting for first message (first message has additional setup costs)" << std::endl;
    infinity::core::receive_element_t receiveElement;
    while (!m_context->receive(&receiveElement));
    m_context->postReceiveBuffer(receiveElement.buffer);

    try {
      while (true) {
        if (terminate) {
          auto t2 = std::chrono::high_resolution_clock::now();
          auto time_span =
              std::chrono::duration_cast<std::chrono::duration<double>>(t2 -
                                                                        _t1);
          if (time_span.count() >= 1) {
            auto thr = (m_Bytes / (1024 * 1024)) / time_span.count();
            m_Bytes = 0;
            _t1 = t2;
            std::cout << "[DBG] " + std::to_string(thr) + " MB/s" << std::endl;
          }
          time_span = std::chrono::duration_cast<std::chrono::duration<double>>(
              t2 - t1);
          if (terminate && time_span.count() >= (double)m_duration) {
            std::cout << "Stop running the remote sink..." << " (" << sum << ")" << std::endl;
            return 0;
          }
        }

        long valread = 0;
        // receive data
        while (!m_context->receive(&receiveElement));
        // do something with data here
        /* std::cout << ((long*)receiveElement.buffer->getData())[0] << " " <<
            ((long*)receiveElement.buffer->getData())[131064]<< " timestamp" << std::endl;
        if(idx == 256){
          return 0;
        }
        idx++;*/
        /*auto dat = (long *)receiveElement.buffer->getData();
        idx = 0; size_t idx2 = 0;
        for (size_t ii = 0; ii < receiveElement.buffer->getSizeInBytes()/128; ii++) {
          if (dat[idx+9] == 0) {
            sum += dat[idx];
            std::memcpy(copyBuffer->data() + idx2, &dat[idx], 32);
            idx2 += 16;
          }
          idx += 16;
        }*/
        // return buffer
        m_context->postReceiveBuffer(receiveElement.buffer);
        valread = SystemConf::getInstance().BUNDLE_SIZE;

        m_Bytes += valread;
      }
    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
      exit(1);
    }
  }

 private:
  void setupSocket() {
    m_context = new infinity::core::Context(1);
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
  }
};

int main(int argc, const char **argv) {
  std::unique_ptr<RemoteRDMASink> remoteSink = std::make_unique<RemoteRDMASink>();
  remoteSink->run(argc, argv);
}