#pragma once

#include <tbb/concurrent_queue.h>

#include <atomic>
#include <memory>
#include <unordered_map>

#include "utils/SystemConf.h"
#include "cql/operators/HashFunctions.h"
#include "cql/operators/HashTable.h"

bool autoConsume = true;
const bool compress = true;
const int mergeDivider = 4;

// for checkpointing
std::atomic<int> checkpointCounter = 0;
std::atomic<int> releaseBarrier = 0;
std::atomic<bool> pushBarriers = false;
bool useCheckpoints = false;
const bool debug = false;

struct QueryByteBuffer {
  int m_id;
  size_t m_capacity;
  size_t m_position;
  long m_latencyMark = -1;
  long m_originalPosition = 0;
  ByteBuffer m_buffer;
  bool m_compressed = false;
  long m_watermark = 0;
  bool m_hasBarrier = false;

  QueryByteBuffer(int id, size_t capacity) : m_id(id), m_capacity(capacity),
                                                  m_position(0L), m_buffer(capacity) {}

  [[nodiscard]] int getBufferId() const {
    return m_id;
  }

  ByteBuffer &getBuffer() {
    return m_buffer;
  }

  [[nodiscard]] size_t getCapacity() const {
    return m_capacity;
  }

  void setPosition(size_t pos) {
    m_position = pos;
  }

  void clear() {
    std::fill(m_buffer.begin(), m_buffer.end(), 0);
  }

  [[nodiscard]] size_t getPosition() const {
    return m_position;
  }

  bool tryToMerge(std::shared_ptr<QueryByteBuffer> &buffer) {
    if (m_position + buffer->getPosition() <= m_capacity/mergeDivider) {
      std::memcpy(m_buffer.data()+m_position, buffer->m_buffer.data(), buffer->m_position);
      m_position += buffer->getPosition();
      m_watermark = std::max(m_watermark, buffer->m_watermark);
      m_latencyMark = std::min(m_latencyMark, buffer->m_latencyMark);
      return true;
    } else {
      return false;
    }
  }

  long getLong(size_t index) {
    auto p = (long *) m_buffer.data();
    return p[index];
  }

  void putLong(size_t index, long value) {
    auto p = (long *) m_buffer.data();
    p[index] = value;
  }

  void putBytes(char *value, size_t length) {
    if (m_position + length > m_capacity) {
      throw std::runtime_error("error: increase the size of the QueryByteBuffer (" + std::to_string(m_capacity) + ")");
    }
    std::memcpy(m_buffer.data() + m_position, value, length);
    m_position += length;
  }
};

struct MemoryPool {
  const int m_numberOfThreads;
  std::atomic<long> count{};
  std::vector<tbb::concurrent_queue<std::shared_ptr<QueryByteBuffer>>> m_pool;
  explicit MemoryPool(int workers = SystemConf::getInstance().WORKER_THREADS)
      : m_numberOfThreads(workers),
        m_pool(m_numberOfThreads){

    /*int pid = 0;
    for (auto &q : m_pool) {
      auto maxSize = 64;
      std::vector<std::shared_ptr<QueryByteBuffer>> tempVec(maxSize);
      for (int cnt = 0; cnt < maxSize; cnt++) {
        tempVec[cnt] = newInstance(pid);
      }
      for (int cnt = 0; cnt < maxSize; cnt++) {
        free(tempVec[cnt]->m_id, tempVec[cnt]);
      }
      pid++;
    }*/
  };

  std::shared_ptr<QueryByteBuffer> newInstance(int pid) {
    if (pid >= m_numberOfThreads)
      throw std::runtime_error("error: invalid pid for creating an unbounded buffer: " + std::to_string(pid) + " >= " + std::to_string(m_numberOfThreads));
    std::shared_ptr<QueryByteBuffer> buffer;
    bool hasRemaining = m_pool[pid].try_pop(buffer);
    if (!hasRemaining) {
      count.fetch_add(1);
      buffer = std::make_shared<QueryByteBuffer>(QueryByteBuffer(pid, SystemConf::getInstance().BLOCK_SIZE));
    }
    return buffer;
  }

  void free(int pid, std::shared_ptr<QueryByteBuffer> &buffer) {
    if (buffer.use_count() > 1) {
      //std::cout << "warning: in worker " + std::to_string(pid) + " the buffer has multiple owners: " + std::to_string(buffer.use_count()) << std::endl;
      return;
    }
    //buffer->clear();
    buffer->m_watermark = 0;
    buffer->m_hasBarrier = false;
    buffer->m_compressed = false;
    buffer->m_originalPosition = 0;
    buffer->setPosition(0);
    m_pool[pid].push(buffer);
  }

  void freeUnsafe(int pid, std::shared_ptr<QueryByteBuffer> &buffer) {
    buffer->m_watermark = 0;
    buffer->m_hasBarrier = false;
    buffer->m_compressed = false;
    buffer->m_originalPosition = 0;
    buffer->setPosition(0);
    m_pool[pid].push(buffer);
  }
};

using BoundedQueue = tbb::concurrent_bounded_queue<std::shared_ptr<QueryByteBuffer>>;
using BoundedQueuePtr = std::shared_ptr<BoundedQueue>;
using Queue = tbb::concurrent_queue<std::shared_ptr<QueryByteBuffer>>;
using QueuePtr = std::shared_ptr<Queue>;
using LatQueue = tbb::concurrent_queue<long>;
using LatQueuePtr = std::shared_ptr<LatQueue>;

/*
 *
 * Queries
 *
 * */

namespace YSBQuery {
struct InputSchema {
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

struct IntermSchema {
  long timestamp;
  long padding_0;
  __uint128_t ad_id;
  __uint128_t campaing_id;
  __uint128_t padding_1;
};

struct OutputSchema {
  long timestamp;
  __uint128_t ad_id;
  int count;
};

MurmurHash3<__uint128_t, 16> m_hash;

struct hash {
  static inline int partition(InputSchema &event, int partitions) {
    //return m_hash(event.ad_id) % partitions;
    //return ((int) event.ad_id) % partitions;
    const __uint128_t __mask = static_cast<std::size_t>(-1);
    const std::size_t __a = (std::size_t)(event.ad_id & __mask);
    const std::size_t __b = (std::size_t)((event.ad_id & (__mask << 64)) >> 64);
    auto hasher = std::hash<size_t>();
    return (hasher(__a) + hasher(__b)) % partitions;
  }
  std::size_t operator()(const __uint128_t &key) const {
    const __uint128_t __mask = static_cast<std::size_t>(-1);
    const std::size_t __a = (std::size_t)(key & __mask);
    const std::size_t __b = (std::size_t)((key & (__mask << 64)) >> 64);
    auto hasher = std::hash<size_t>();
    return hasher(__a) + hasher(__b);
  }
};
struct Eq {
  constexpr bool operator()(const __uint128_t &k1,
                            const __uint128_t &k2) const {
    return k1 == k2;
  }
};
using Key = __uint128_t;
using Value = __uint128_t;


struct static_node {
  __uint128_t key;
  __uint128_t value;
};
struct staticHashTable {
  int size = 1024;
  int mask = size - 1;
  static_node *table;
  hash hashVal;

 public:
  explicit staticHashTable(static_node *table) { this->table = table; }
  bool get_value (const __uint128_t key, __uint128_t &result) const {
    int ind = hashVal(key) & mask;
    int i = ind;
    for (; i < this->size; i++) {
      if (this->table[i].key == key) {
        result = this->table[i].value;
        return true;
      }
    }
    for (i = 0; i < ind; i++) {
      if (this->table[i].key == key) {
        result = this->table[i].value;
        return true;
      }
    }
    return false;
  }
};
std::vector<char> *m_staticData[32];
staticHashTable *staticMap[32];

bool isFirst[32] = {true,true,true,true,true,true,true,true,
                    true,true,true,true,true,true,true,true,
                    true,true,true,true,true,true,true,true,
                    true,true,true,true,true,true,true,true};

long m_timestampOffset = 0;

thread_local long partitionOffset[16] {0,0,0,0,
                                       0,0,0,0,
                                       0,0,0,0,
                                       0,0,0,0};

struct StatelessOp {
  static inline void process(int pid, long watermark, char *input, int inLength, std::shared_ptr<QueryByteBuffer> &output) {
    // initialize input
    auto data = (InputSchema *) input;
    inLength = inLength / sizeof (InputSchema);

    /*for (unsigned long i = 0; i < inLength; ++i) {
      printf("[DBG] %09d: %7d %13ld %8ld %13ld %3ld %6ld %2ld \n",
             i, data[i].timestamp, (long) data[i].user_id, (long) data[i].page_id, (long) data[i].ad_id,
             data[i].ad_type, data[i].event_type, (long) data[i].ip_address);
    }*/

    // initialize hashtable
    if (isFirst[pid]) {
      auto *sBuf = (static_node *) m_staticData[pid];
      staticMap[pid] = new  staticHashTable(sBuf);
      isFirst[pid] = false;
    }

    for (int idx = 0; idx < inLength; idx++) {
      if (data[idx].event_type == 0) {
        __uint128_t joinRes;
        bool joinFound = staticMap[pid]->get_value(data[idx].ad_id, joinRes);
        if (joinFound) {
          IntermSchema tempNode = {data[idx].timestamp + m_timestampOffset, 0, data[idx].ad_id,
                                  joinRes, 0};
          output->putBytes((char *) &tempNode, sizeof(IntermSchema));
        }
      }
    }

    partitionOffset[0] += inLength;
  }

  static inline void sendDownstream(
      int pid,
      BoundedQueuePtr &outputQueue,
      std::vector<std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>>> *nextOperatorQueues,
      std::vector<std::shared_ptr<QueryByteBuffer>> &partitionBuffers,
      std::shared_ptr<MemoryPool> &pool) {
    if (!nextOperatorQueues) {
      std::shared_ptr<QueryByteBuffer> buffer;
      while (outputQueue->try_pop(buffer)) {
        if (buffer) {
          pool->freeUnsafe(buffer->m_id, buffer);
        }
      }
    } else {
      // if outputQueue is full wait until data is pushed downstream
      std::shared_ptr<QueryByteBuffer> buffer;
      while (outputQueue->try_pop(buffer)) {
        if (buffer) {
          auto data = (IntermSchema *)buffer->getBuffer().data();
          size_t length = buffer->getPosition() / sizeof(IntermSchema);
          auto partitions = partitionBuffers.size();
          auto watermark = buffer->m_watermark;
          auto latencyMark = buffer->m_latencyMark;
          /*if (length > 0) {
            // assume there is a single partition
            auto partition = ((int)data[0].ad_id) % partitions;
            auto &parBuf = partitionBuffers[partition];
            if (!parBuf) {
              parBuf = pool->newInstance(partition);
            }
            bool hasSinglePartition = true;
            for (size_t idx = 0; idx < length; idx++) {
              auto nextPart = ((int)data[idx].ad_id) % partitions;
              if (partition != nextPart) {
                hasSinglePartition = false;
                break;
              }
              parBuf->putBytes((char *)&data[idx], sizeof(IntermSchema));
            }
            if (hasSinglePartition) {
              auto &queues = (*nextOperatorQueues)[pid];
              while (!queues[partition]->try_push(parBuf)) {
                // std::cout << "warning: partition " + std::to_string(par) << "
                // is full" << std::endl;
              }
              parBuf = nullptr;
            } else {
              parBuf->setPosition(0);
              for (size_t idx = 0; idx < length; idx++) {
                partition = ((int)data[idx].ad_id) % partitions;
                auto &buf = partitionBuffers[partition];
                if (!buf) {
                  buf = pool->newInstance(partition);
                }
                buf->putBytes((char *)&data[idx], sizeof(IntermSchema));
              }

              int countPartitions = 0;
              auto &queues = (*nextOperatorQueues)[pid];
              for (auto par = 0; par < partitions; par++) {
                auto &buf = partitionBuffers[par];
                if (buf) {
                  if (nextOperatorQueues) {
                    while (!queues[par]->try_push(buf)) {
                      // std::cout << "warning: partition " +
                      // std::to_string(par) << " is full" << std::endl;
                    }
                    countPartitions++;
                  } else {
                    pool->freeUnsafe(buf->m_id, buf);
                  }
                  buf = nullptr;
                }
              }
              if (countPartitions > 1) {
                std::cout << "Worker " + std::to_string(pid) + " sent to " +
                                 std::to_string(countPartitions) +
                                 " partitions."
                          << std::endl;
              }
            }
          }*/
          for (size_t idx = 0; idx < length; idx++) {
            auto partition = ((int) data[idx].ad_id) % partitions;
            auto &buf = partitionBuffers[partition];
            if (!buf) {
              buf = pool->newInstance(partition);
            }
            buf->putBytes((char *)&data[idx], sizeof(IntermSchema));
          }

          int countPartitions = 0;
          auto &queues = (*nextOperatorQueues)[pid];
          for (auto par = 0; par < partitions; par++) {
            auto &buf = partitionBuffers[par];
            if (buf) {
              buf->m_watermark = watermark;
              buf->m_latencyMark = latencyMark;
              if (nextOperatorQueues) {
                while (!queues[par]->try_push(buf)) {
                  //std::cout << "warning: partition " + std::to_string(par) << " is full" << std::endl;
                }
                countPartitions++;
              } else {
                pool->freeUnsafe(buf->m_id, buf);
              }
              buf = nullptr;
            }
          }
          // free initial batch
          pool->freeUnsafe(buffer->m_id, buffer);
        }
      }
    }
  }

  static inline void checkpoint(int pid, int fd, std::shared_ptr<QueryByteBuffer> &temp) {
    ::pwrite(fd, &partitionOffset, 16 * sizeof(long), 0);
    fsync(fd);
    //std::cout << "Worker " + std::to_string(pid) << " finished its checkpoint" << std::endl;
  }
};


struct GroupKey {
  __uint128_t key;
  long window;
  bool operator==(const GroupKey &other) const {
    return (key == other.key && window == other.window);
  }
};
struct GroupHash {
  MurmurHash<GroupKey, 24> ghash;
  std::size_t operator()(const GroupKey &key) const { return ghash(key); }
};


thread_local std::unordered_map<GroupKey, int, GroupHash> map;
thread_local int checkpointCnt = 0;
thread_local long checkpointSize = 0;
thread_local int checkpointTimes = 0;

struct StateFulOp {
  static inline void process(int pid, long watermark, char *input, int inLength, std::shared_ptr<QueryByteBuffer> &output)  {
    // initialize input
    auto data = (IntermSchema *) input;
    inLength = inLength / sizeof (IntermSchema);

    for (int idx = 0; idx < inLength; idx++) {
      GroupKey tempNode = {data[idx].ad_id, data[idx].timestamp / 100};
      auto elem = map.find(tempNode);
      if (elem != map.end()) {
        elem->second++;
      } else {
        map[tempNode] = 1;
      }
    }

    // do this only with a watermark
    if (watermark > 0) {
      auto it = map.begin();
      while (it != map.end()) {
        if (it->first.window < watermark) {
          OutputSchema tempNode = {it->first.window, it->first.key, it->second};
          output->putBytes((char *)&tempNode, sizeof(OutputSchema));
          it = map.erase(it);
        } else
          it++;
      }
      map.size();
    }

    //for (const auto &elem : map) {
    //}
    //map.clear();
  }

  static inline void sendDownstream(
      int pid,
      BoundedQueuePtr &outputQueue,
      std::vector<std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>>> *nextOperatorQueues,
      std::vector<std::shared_ptr<QueryByteBuffer>> &partitionBuffers,
      std::shared_ptr<MemoryPool> &pool) {
    std::shared_ptr<QueryByteBuffer> buffer;
    while (outputQueue->try_pop(buffer)) {
      if (buffer) {
        pool->freeUnsafe(buffer->m_id, buffer);
      }
    }
  }

  static inline void checkpoint(int pid, int fd, std::shared_ptr<QueryByteBuffer> &temp) {
    for (const auto &elem : map) {
      OutputSchema tempNode = {elem.first.window, elem.first.key,
                               elem.second};
      temp->putBytes((char *)&tempNode, sizeof(OutputSchema));
    }
    ::pwrite(fd, temp->getBuffer().data(), temp->getPosition(), 0);
    fsync(fd);
    checkpointTimes++;
    checkpointCnt++;
    checkpointSize += temp->getPosition();
    if (pid == 0 && checkpointTimes == 5) {
      std::cout << "Worker " + std::to_string(pid) << " finished its checkpoint: " + std::to_string(temp->getPosition()) +
                " bytes with " + std::to_string(map.size()) + " elements"
                + "[AVG: " + std::to_string(checkpointSize/checkpointCnt) +  "]" << std::endl;
      checkpointTimes = 0;
    }
    temp->setPosition(0);
  }
};

};

namespace CM1Query {
std::vector<char> *m_staticData[32];
struct InputSchema {
  long timestamp;
  long jobId;
  long taskId;
  long machineId;
  int eventType;
  int userId;
  int category;
  int priority;
  float cpu;
  float ram;
  float disk;
  int constraints;
};
struct hash {
  static inline int partition(InputSchema &event, int partitions) {
    auto hasher = std::hash<int>();
    return (hasher(event.category)) % partitions;
  }
  std::size_t operator()(const int &key) const {
    auto hasher = std::hash<int>();
    return hasher(key);
  }
};
};

namespace CM2Query {
std::vector<char> *m_staticData[32];
struct InputSchema {
  long timestamp;
  long jobId;
  long taskId;
  long machineId;
  int eventType;
  int userId;
  int category;
  int priority;
  float cpu;
  float ram;
  float disk;
  int constraints;
};
struct hash {
  static inline int partition(InputSchema &event, int partitions) {
    auto hasher = std::hash<int>();
    return (hasher(event.jobId)) % partitions;
  }
  std::size_t operator()(const int &key) const {
    auto hasher = std::hash<int>();
    return hasher(key);
  }
};
};

namespace SG1Query {
std::vector<char> *m_staticData[32];
struct InputSchema {
  long timestamp;
  float value;
  int property;
  int plug;
  int household;
  int house;
  int padding;
};
struct hash {
  static inline int partition(InputSchema &event, int partitions) {
    return 0;
  }
  std::size_t operator()(const int &key) const {
    return 0;
  }
};
};

namespace SG2Query {
std::vector<char> *m_staticData[32];
struct InputSchema {
  long timestamp;
  float value;
  int property;
  int plug;
  int household;
  int house;
  int padding;
};
struct key {
  int plug;
  int household;
  int house;
};
MurmurHash3<key, 12> m_hash;

struct hash {
  static inline int partition(InputSchema &event, int partitions) {
    key temp = {event.plug, event.household, event.house};
    return m_hash(temp) % partitions;
  }
  std::size_t operator()(const InputSchema &event) const {
    key temp = {event.plug, event.household, event.house};
    return m_hash(temp);
  }
};
};

namespace SG3Query {
std::vector<char> *m_staticData[32];
struct InputSchema {
  long timestamp;
  float value;
  int property;
  int plug;
  int household;
  int house;
  int padding;
};
struct key {
  int plug;
  int household;
  int house;
};
MurmurHash3<key, 12> m_hash;

struct hash {
  static inline int partition(InputSchema &event, int partitions) {
    key temp = {event.plug, event.household, event.house};
    return m_hash(temp) % partitions;
  }
  std::size_t operator()(const InputSchema &event) const {
    key temp = {event.plug, event.household, event.house};
    return m_hash(temp);
  }
};
};

namespace LRB1Query {
std::vector<char> *m_staticData[32];
struct InputSchema {
  long timestamp;
  int vehicle;
  float speed;
  int highway;
  int lane;
  int direction;
  int position;
};
struct key {
  int highway;
  int direction;
  int segment;
};
MurmurHash3<key, 12> m_hash;

struct hash {
  static inline int partition(InputSchema &event, int partitions) {
    key temp = {event.highway, event.direction, event.position / 5280};
    return m_hash(temp) % partitions;
  }
  std::size_t operator()(const InputSchema &event) const {
    key temp = {event.highway, event.direction, event.position / 5280};
    return m_hash(temp);
  }
};
};

namespace LRB2Query {
std::vector<char> *m_staticData[32];
struct InputSchema {
  long timestamp;
  int vehicle;
  float speed;
  int highway;
  int lane;
  int direction;
  int position;
};
struct key {
  int highway;
  int vehicle;
  int direction;
  int segment;
};
MurmurHash3<key, 16> m_hash;

struct hash {
  static inline int partition(InputSchema &event, int partitions) {
    key temp = {event.vehicle, event.highway, event.direction, event.position / 5280};
    return m_hash(temp) % partitions;
  }
  std::size_t operator()(const InputSchema &event) const {
    key temp = {event.vehicle, event.highway, event.direction, event.position / 5280};
    return m_hash(temp);
  }
};
};

namespace LRB3Query {
std::vector<char> *m_staticData[32];
struct InputSchema {
  long timestamp;
  int vehicle;
  float speed;
  int highway;
  int lane;
  int direction;
  int position;
};
struct key {
  int highway;
  int vehicle;
  int direction;
  int segment;
};
MurmurHash3<key, 16> m_hash;

struct hash {
  static inline int partition(InputSchema &event, int partitions) {
    key temp = {event.vehicle, event.highway, event.direction, event.position / 5280};
    return m_hash(temp) % partitions;
  }
  std::size_t operator()(const InputSchema &event) const {
    key temp = {event.vehicle, event.highway, event.direction, event.position / 5280};
    return m_hash(temp);
  }
};
};

namespace ME1Query {
std::vector<char> *m_staticData[32];
struct alignas(64) InputSchema {
  long timestamp;
  long messageIndex;
  int mf01;           //Electrical Power Main Phase 1
  int mf02;           //Electrical Power Main Phase 2
  int mf03;           //Electrical Power Main Phase 3
  int pc13;           //Anode Current Drop Detection Cell 1
  int pc14;           //Anode Current Drop Detection Cell 2
  int pc15;           //Anode Current Drop Detection Cell 3
  unsigned int pc25;  //Anode Voltage Drop Detection Cell 1
  unsigned int pc26;  //Anode Voltage Drop Detection Cell 2
  unsigned int pc27;  //Anode Voltage Drop Detection Cell 3
  unsigned int res;
  int bm05 = 0;
  int bm06 = 0;
};
struct hash {
  static inline int partition(InputSchema &event, int partitions) {
    return 0;
  }
  std::size_t operator()(const int &key) const {
    return 0;
  }
};
};

namespace NBQ5Query {
std::vector<char> *m_staticData[32];
struct alignas(16) InputSchema {
  long timestamp;
  long id;
  long itemName;
  long description;
  long initialBid;
  long reserve;
  long expires;
  long seller;
  long category;
  long padding_0;
  long padding_1;
  long padding_2;
  long padding_3;
  long padding_4;
  long padding_5;
  long padding_6;
};
struct hash {
  static inline int partition(InputSchema &event, int partitions) {
    auto hasher = std::hash<int>();
    return (hasher(event.id)) % partitions;
  }
  std::size_t operator()(const int &key) const {
    auto hasher = std::hash<int>();
    return hasher(key);
  }
};
};