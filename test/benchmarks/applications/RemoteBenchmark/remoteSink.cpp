#pragma once

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include "utils/SystemConf.h"
#include "utils/TupleSchema.h"
#include "utils/Utils.h"

// --batch-size 1048576 --bundle-size 1048576 --two-sources true

class RemoteSink {
 protected:
  int m_sock = 0;
  int m_sock2 = 0;
  int m_server_fd = 0;
  int m_server_fd2 = 0;

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

    auto core = (SystemConf::getInstance().REMOTE_CLIENT == SystemConf::KEA03_ib1 ||
                  SystemConf::getInstance().REMOTE_CLIENT == SystemConf::KEA04_ib1) ? Utils::getFirstCoreFromSocket(1) : 2;
    Utils::bindProcess(core);

    size_t len = SystemConf::getInstance().BUNDLE_SIZE;
    auto inputBuffer = new std::vector<char>(len);

    // setup socket
    setupSocket();

    size_t idx = 0;
    auto t1 = std::chrono::high_resolution_clock::now();
    auto _t1 = std::chrono::high_resolution_clock::now();
    std::cout << "Start running the remote sink..." << std::endl;
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
            std::cout << "Stop running the remote sink..." << std::endl;
            return 0;
          }
        }

        long valread = 0;
        // receive data
        if (SystemConf::getInstance().HAS_TWO_SOURCES) {

          if (idx % 2 == 0) {
            valread = readBytes(m_sock, inputBuffer->size(), inputBuffer->data());
          } else {
            valread = readBytes(m_sock2, inputBuffer->size(), inputBuffer->data());
          }
          idx++;
        } else {
          valread = readBytes(m_sock, inputBuffer->size(), inputBuffer->data());
        }
        m_Bytes += valread;
      }
    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
      exit(1);
    }
  }

 private:
  void setupSocket() {
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

    // todo: accept multiple connections
    if ((m_sock = accept(m_server_fd, (struct sockaddr *)&address,
                         (socklen_t *)&addrlen)) < 0) {
      throw std::runtime_error("error: accept");
    }
    std::cout << "The remote sink established the 1st connection" << std::endl;
    if (SystemConf::getInstance().HAS_TWO_SOURCES) {
      if ((m_sock2 = accept(m_server_fd, (struct sockaddr *)&address,
                           (socklen_t *)&addrlen)) < 0) {
        throw std::runtime_error("error: accept");
      }
      std::cout << "The remote sink established the 2nd connection" << std::endl;
    }
  }

  static inline size_t readBytes(int socket, unsigned int length, void *buffer) {
    size_t bytesRead = 0;
    while (bytesRead < length) {
      auto valread =
          read(socket, (char *)buffer + bytesRead, length - bytesRead);
      assert(valread >= 0);
      bytesRead += valread;
    }
    return bytesRead;
  }
};

int main(int argc, const char **argv) {
  std::unique_ptr<RemoteSink> remoteSink = std::make_unique<RemoteSink>();
  remoteSink->run(argc, argv);
}