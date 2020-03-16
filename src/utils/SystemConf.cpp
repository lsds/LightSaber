#include "SystemConf.h"

#include <thread>

unsigned int SystemConf::BATCH_SIZE = 2 * 64 * 1024;
unsigned int SystemConf::BUNDLE_SIZE = 2 * 64 * 1024;
long SystemConf::INPUT_SIZE = 2 * 64 * 1024;
int SystemConf::PARTIAL_WINDOWS = 1024;
size_t SystemConf::HASH_TABLE_SIZE = 512; //4*512;
unsigned long SystemConf::THROUGHPUT_MONITOR_INTERVAL = 1000L;
unsigned long SystemConf::PERFORMANCE_MONITOR_INTERVAL = 1000L;
int SystemConf::MOST_UPSTREAM_QUERIES = 2;
int SystemConf::PIPELINE_DEPTH = 4;
size_t SystemConf::CIRCULAR_BUFFER_SIZE = 4 * 1048576;
size_t SystemConf::UNBOUNDED_BUFFER_SIZE = 2 * 4 * 64 * 1024;
int SystemConf::WORKER_THREADS = 1;
int SystemConf::SLOTS = 256;
bool SystemConf::LATENCY_ON = false;
int SystemConf::THREADS = std::thread::hardware_concurrency();
long SystemConf::DURATION = 0;
int SystemConf::QUERY_NUM = 0;
bool SystemConf::PARALLEL_MERGE_ON = false;