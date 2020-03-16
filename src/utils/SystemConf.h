#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <boost/align/aligned_allocator.hpp>
#include <numaif.h>

#include "tasks/Task.h"

/*
 * \brief These are the system configuration parameters.
 *
 * Check the application benchmarks to find examples of how to
 * set them.
 *
 * */

#if defined(HAVE_NUMA)
#include "NumaAllocator.h"
#include "tasks/NumaTaskQueueWrapper.h"
typedef NumaAlloc::NumaAllocator<char> numa_allocator;
typedef NumaAlloc::NumaAllocator<char*> ptr_numa_allocator;
//using ByteBuffer = std::vector<char, std::allocator<char>>;
//using ByteBufferPtr = std::vector<char *, std::allocator<char*>>;
using ByteBuffer = std::vector<char, tbb::cache_aligned_allocator<char>>;
using ByteBufferPtr = std::vector<char *, tbb::cache_aligned_allocator<char*>>;
//using ByteBuffer = std::vector<char, numa_allocator>;
//using ByteBufferPtr = std::vector<char *, ptr_numa_allocator>;
using TaskQueue = NumaTaskQueueWrapper;
#else
#include <tbb/cache_aligned_allocator.h>
#include "tasks/ConcurrentQueue.h"
using ByteBuffer = std::vector<char,
                               tbb::cache_aligned_allocator<char>>;//boost::alignment::aligned_allocator<char, 4096>>;
using ByteBufferPtr = std::vector<char *,
                                  tbb::cache_aligned_allocator<char *>>;//boost::alignment::aligned_allocator<char*, 4096>>;
using TaskQueue = moodycamel::ConcurrentQueue<std::shared_ptr<Task>>;
#endif

class SystemConf {
 private:
  SystemConf() {};

 public:
  static unsigned int BATCH_SIZE;
  static unsigned int BUNDLE_SIZE;
  static long INPUT_SIZE;
  static int PARTIAL_WINDOWS;
  static size_t HASH_TABLE_SIZE;
  static unsigned long THROUGHPUT_MONITOR_INTERVAL;
  static unsigned long PERFORMANCE_MONITOR_INTERVAL;
  static int MOST_UPSTREAM_QUERIES;
  static int PIPELINE_DEPTH;
  static size_t CIRCULAR_BUFFER_SIZE;
  static size_t UNBOUNDED_BUFFER_SIZE;
  static int WORKER_THREADS;
  static int SLOTS;
  static bool LATENCY_ON;
  static const int POOL_SIZE = 0;
  static int THREADS;
  static long DURATION;
  static int QUERY_NUM;
  static bool PARALLEL_MERGE_ON;

#if defined(HAVE_NUMA)
  void findMemoryNodeForCPU(int &numa_node) {
      int cpu = sched_getcpu();
      numa_node = numa_node_of_cpu(cpu);
  }

  void findMemoryNodeForAddress(void* ptr, int &numa_node) {
      numa_node = -1;
      if(get_mempolicy(&numa_node, NULL, 0, ptr, MPOL_F_NODE | MPOL_F_ADDR) < 0)
          std::cout << "WARNING: get_mempolicy failed" << std::endl;
  }

  void findMemoryNodeForAddress(void* ptr) {
      int numa_node = -1;
      if(get_mempolicy(&numa_node, NULL, 0, ptr, MPOL_F_NODE | MPOL_F_ADDR) < 0)
          std::cout << "WARNING: get_mempolicy failed" << std::endl;
      std::cout << "[DBG] This address is allocated in " << numa_node << " node." << std::endl;
  }
#endif

  static SystemConf &getInstance() {
    static SystemConf instance;
    return instance;
  }

  SystemConf(SystemConf const &) = delete;
  void operator=(SystemConf const &) = delete;

  void dump() {
    std::string s = "=== [System configuration dump] ===\n";

    s.append("Batch size                   : " + std::to_string(SystemConf::BATCH_SIZE) + "\n");
    s.append("Number of worker threads     : " + std::to_string(SystemConf::WORKER_THREADS) + "\n");
    s.append("Number of result slots       : " + std::to_string(SystemConf::SLOTS) + "\n");
    s.append("Number of partial windows    : " + std::to_string(SystemConf::PARTIAL_WINDOWS) + "\n");
    s.append("Circular buffer size         : " + std::to_string(SystemConf::CIRCULAR_BUFFER_SIZE) + " bytes\n");
    s.append("Intermediate buffer size     : " + std::to_string(SystemConf::UNBOUNDED_BUFFER_SIZE) + " bytes\n");
    s.append("Hash table size              : " + std::to_string(SystemConf::HASH_TABLE_SIZE) + " bytes\n");
    s.append("Throughput monitor interval  : " + std::to_string(SystemConf::THROUGHPUT_MONITOR_INTERVAL) + " msec\n");
    s.append("Performance monitor interval : " + std::to_string(SystemConf::PERFORMANCE_MONITOR_INTERVAL) + " msec\n");
    s.append("Number of upstream queries   : " + std::to_string(SystemConf::MOST_UPSTREAM_QUERIES) + "\n");
    s.append("GPU pipeline depth           : " + std::to_string(SystemConf::PIPELINE_DEPTH) + "\n");
    std::string latency = (SystemConf::LATENCY_ON ? "On" : "Off");
    s.append("Latency measurements         : " + latency + "\n");
    s.append("Pool size                    : " + std::to_string(SystemConf::POOL_SIZE) + "\n");
    s.append("Available threads            : " + std::to_string(SystemConf::THREADS) + "\n");
    s.append("Experiment duration          : " + std::to_string(DURATION) + " units (= perf. monitor intervals)\n");

    s.append("=== [End of system configuration dump] ===");

    std::cout << s << std::endl;
  }
};