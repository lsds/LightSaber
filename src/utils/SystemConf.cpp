#include "SystemConf.h"

#include <thread>
#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>

[[maybe_unused]] static const char *homedir = ((homedir = getenv("HOME")) == nullptr) ?
                      getpwuid(getuid())->pw_dir : getenv("HOME");

unsigned int SystemConf::BATCH_SIZE = 2 * 64 * 1024;
unsigned int SystemConf::BUNDLE_SIZE = 2 * 64 * 1024;
long SystemConf::INPUT_SIZE = 2 * 64 * 1024;
int SystemConf::PARTIAL_WINDOWS = 1024;
size_t SystemConf::HASH_TABLE_SIZE = 512; //4*512;
unsigned long SystemConf::COMPRESSION_MONITOR_INTERVAL = 4000L;
unsigned long SystemConf::PERFORMANCE_MONITOR_INTERVAL = 1000L;
unsigned long SystemConf::MBs_INGESTED_PER_SEC = 0L;
bool SystemConf::BUFFERED_LATENCY = false;
unsigned long SystemConf::CHECKPOINT_INTERVAL = 1000L;
unsigned long SystemConf::BLOCK_SIZE = 16 * _KB;
unsigned long SystemConf::DISK_BUFFER = 8;
int SystemConf::MOST_UPSTREAM_QUERIES = 2;
int SystemConf::PIPELINE_DEPTH = 4;
size_t SystemConf::CIRCULAR_BUFFER_SIZE = 4 * 1048576;
size_t SystemConf::UNBOUNDED_BUFFER_SIZE = 2 * 4 * 64 * 1024;
size_t SystemConf::OUTPUT_BUFFER_SIZE = 1048576;
int SystemConf::WORKER_THREADS = 1;
int SystemConf::SLOTS = 256;
bool SystemConf::LATENCY_ON = false;
int SystemConf::THREADS = std::thread::hardware_concurrency();
long SystemConf::DURATION = 0;
long SystemConf::FAILURE_TIME = 0;
bool SystemConf::FAILURE_ON = false;
int SystemConf::QUERY_NUM = 0;
bool SystemConf::PARALLEL_MERGE_ON = false;
bool SystemConf::CHECKPOINT_ON = false;
bool SystemConf::ADAPTIVE_COMPRESSION_ON = false;
bool SystemConf::ADAPTIVE_FORCE_RLE = false;
bool SystemConf::ADAPTIVE_CHANGE_DATA = false;
size_t SystemConf::ADAPTIVE_COMPRESSION_INTERVAL = 1024;
bool SystemConf::CREATE_MERGE_WITH_CHECKPOINTS = false;
bool SystemConf::CHECKPOINT_COMPRESSION = false;
size_t SystemConf::OUT_OF_ORDER_SIZE = 4;
bool SystemConf::PERSIST_INPUT = false;
bool SystemConf::LINEAGE_ON = false;
bool SystemConf::RECOVER = false;
const std::string SystemConf::FILE_ROOT_PATH = std::string(homedir) + "/data";
//const std::string SystemConf::FILE_ROOT_PATH = "/mnt/rdisk/data";
//const std::string SystemConf::FILE_ROOT_PATH = "/mnt/LSDSDataShare/projects/21-scabbard/data";
//const std::string SystemConf::FILE_ROOT_PATH = "/home/grt17/data";
//const std::string SystemConf::FILE_ROOT_PATH = "/home/george";
size_t SystemConf::CAMPAIGNS_NUM = 100;
bool SystemConf::USE_FLINK = true;
bool SystemConf::USE_KAFKA = false;

const std::string SystemConf::LOCALHOST = "127.0.0.1";
const std::string SystemConf::PLATYPUS1_1GB = "192.168.0.66";
const std::string SystemConf::PLATYPUS1_10GB = "192.168.10.98";
const std::string SystemConf::KEA03_ib0 = "10.0.0.30";
const std::string SystemConf::KEA03_ib1 = "11.0.0.31";
const std::string SystemConf::KEA04_ib0 = "10.0.0.40";
const std::string SystemConf::KEA04_ib1 = "11.0.0.41";
const std::string SystemConf::WALLABY_ib0 = "10.0.0.90";
const std::string SystemConf::WALLABY_ib1 = "11.0.0.91";

bool SystemConf::HAS_TWO_SOURCES = false;
bool SystemConf::SEND_TO_SECOND_WORKER = false;
const std::string SystemConf::REMOTE_WORKER = SystemConf::WALLABY_ib1; // SystemConf::KEA04_ib0;
const std::string SystemConf::REMOTE_WORKER_2 = SystemConf::KEA04_ib1;
const std::string SystemConf::REMOTE_CLIENT = SystemConf::KEA04_ib1;

// server: iperf -s
// client: iperf -c 10.0.0.40

// rdma
// ibdev2netdev : check status
// sudo ifconfig ib0 10.0.0.30/24 up
// sudo ifconfig ib1 11.0.0.31/24 up