#pragma once

#include <fcntl.h>
#include <snappy.h>
#include <sys/stat.h>
#include <tbb/concurrent_queue.h>
#include <unistd.h>

#include <boost/align/aligned_allocator.hpp>
#include <cfloat>
#include <cstdio>
#include <iomanip>
#include <sstream>
#include <vector>
#include <unordered_set>
#include <mutex>

#include "BenchmarkUtils.h"
#include "benchmarks/kafka-flink/queues/readerwritercircularbuffer.h"
#include "benchmarks/kafka-flink/queues/readerwriterqueue.h"
#include "utils/Utils.h"

/*
 * \brief A prototype implementation of Kafka in C++ using fsync after
 * storing a number of data batches on disk.
 *
 * */

std::atomic<bool> startKafka = false;
std::atomic<int> kafkaBarrier;
size_t m_kafkaDuration = 60;
thread_local bool m_measureLatency = true;
std::mutex m_measurementsMutex;
std::vector<double> m_totalMeasurements;
int m_batchSize = 8;

//using BoundedQueue = moodycamel::BlockingReaderWriterCircularBuffer<std::shared_ptr<QueryByteBuffer>>;
//using BoundedQueuePtr = std::shared_ptr<BoundedQueue>;
// using Queue = moodycamel::ReaderWriterQueue<std::shared_ptr<QueryByteBuffer>>;
//using QueuePtr = std::shared_ptr<Queue>;

struct KafkaProcessor {
  std::shared_ptr<MemoryPool> m_pool;
  BoundedQueuePtr m_inputQueue;
  BoundedQueuePtr m_outputQueue;
  LatQueuePtr m_latQueue;
  int m_pid;
  std::shared_ptr<QueryByteBuffer> m_tempBuffer;

  // file properties
  std::vector<std::shared_ptr<QueryByteBuffer>> m_diskBuffer;
  size_t m_offset = 0;
  size_t m_fileSize = 64 * 1024 * 1024;
  const std::string m_fileName;
  int m_fd;

  // used for latency measurements
  int m_repeat = 0;
  std::vector<double> m_measurements;
  long m_count = 0;
  double m_min = DBL_MAX, m_max = DBL_MIN, m_avg = 0;
  long m_timestampReference = 0;
  double m_latency = 0;

  KafkaProcessor(int pid, BoundedQueuePtr &inputQueue, BoundedQueuePtr &outputQueue,
                 std::shared_ptr<MemoryPool> &pool,
                 LatQueuePtr &latQueue, long timestampReference)
      : m_pool(pool),
        m_inputQueue(inputQueue),
        m_outputQueue(outputQueue),
        m_latQueue(latQueue),
        m_pid(pid),
        m_diskBuffer(m_batchSize),
        m_fileName(SystemConf::FILE_ROOT_PATH + "/kafka/file_" + std::to_string(m_pid)),
        m_timestampReference(timestampReference) {
    m_tempBuffer = m_pool->newInstance(m_pid);

    // create file
    std::remove(m_fileName.c_str());
    m_fd = ::open(m_fileName.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  }

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
  void operator()() {
    while (!startKafka)
      ;

    std::shared_ptr<QueryByteBuffer> buffer;
    long latencyMark;
    int writes = 0;
    while (true) {
      try {
        while (!m_inputQueue->try_pop(buffer)) {
        //while (!m_inputQueue->try_dequeue(buffer) || !buffer) {
          // std::cout << "warning: worker " + std::to_string(m_pid) << " is waiting for work" << std::endl;
          std::this_thread::sleep_for(std::chrono::nanoseconds(1));
        }
        if (SystemConf::LATENCY_ON && autoConsume) {
          while (!m_latQueue->try_pop(latencyMark))
            ;
        }

        if (buffer->m_hasBarrier) {
          m_outputQueue->push(buffer);
        } else {
          // todo: build indexes and persist the metadata
          // todo: assign an offset to each record

          auto tempBuffer = m_pool->newInstance(m_pid);
          tempBuffer->m_originalPosition = buffer->getPosition();
          tempBuffer->m_latencyMark = latencyMark;
          // compress data
          if (compress) {
            size_t output_length;
            snappy::RawCompress(buffer->getBuffer().data(),
                                buffer->getPosition(),
                                tempBuffer->getBuffer().data(), &output_length);
            tempBuffer->setPosition(output_length);
            tempBuffer->m_compressed = true;
            buffer = tempBuffer;
          } else {
            std::memcpy(tempBuffer->getBuffer().data(),
                        buffer->getBuffer().data(), buffer->getPosition());
            tempBuffer->setPosition(buffer->getPosition());
            buffer = tempBuffer;
          }

          /*if (!m_diskBuffer[0]) {
            m_diskBuffer[0] = buffer;
          } else {
            if (!m_diskBuffer[writes]->tryToMerge(buffer)) {
              ::pwrite(m_fd, m_diskBuffer[writes]->getBuffer().data(),
          m_diskBuffer[writes]->getPosition(), m_offset); m_offset +=
          m_diskBuffer[writes]->getPosition(); if (m_offset >= m_fileSize) {
                m_offset = 0;
              }
              writes++;
              if (writes == m_batchSize) {
                fsync(m_fd);
                // fdatasync(m_fd);
                writes = 0;
                for (auto &b : m_diskBuffer) {
                  // append data to the output queue
                  if (!b) {
                    throw std::runtime_error("error: invalid buffer ptr");
                  }
                  if (SystemConf::LATENCY_ON && autoConsume && m_measureLatency)
          { measureLatency(b->m_latencyMark);
                  }
                  m_outputQueue->push(b);
                  // while (m_outputQueue->try_enqueue(b))
                  //  ;
                  b = nullptr;
                }

                if (SystemConf::LATENCY_ON && autoConsume) {
                  m_repeat++;
                  if (m_pid == 0 && m_repeat == 128) {
                    std::cout << "Latency metrics [avg " + std::to_string(m_avg)
          +
                                     "] " + "[min " + std::to_string(m_min) +
                                     "] " + "[max " + std::to_string(m_max) + "]
          "
                              << std::endl;
                    m_repeat = 0;
                  }
                }
              }
              m_diskBuffer[writes] = buffer;
            } else {
              m_pool->free(buffer->getBufferId(), buffer);
            }
          }*/

          // write data to disk and fsync
          ::pwrite(m_fd, buffer->getBuffer().data(), buffer->getPosition(),
                   m_offset);
          if (writes == m_batchSize) {
            fsync(m_fd);
            // fdatasync(m_fd);
            writes = 0;
            for (auto &b : m_diskBuffer) {
              // append data to the output queue
              if (!b) {
                throw std::runtime_error("error: invalid buffer ptr");
              }
              if (SystemConf::LATENCY_ON && autoConsume && m_measureLatency) {
                measureLatency(b->m_latencyMark);
              }
              m_outputQueue->push(b);
              // while (m_outputQueue->try_enqueue(b))
              //  ;
              b = nullptr;
            }

            if (SystemConf::LATENCY_ON && autoConsume) {
              m_repeat++;
              if (m_pid == 0 && m_repeat == 128) {
                std::cout << "Latency metrics [avg " + std::to_string(m_avg) +
                                 "] " + "[min " + std::to_string(m_min) + "] " +
                                 "[max " + std::to_string(m_max) + "] "
                          << std::endl;
                m_repeat = 0;
              }
            }
          }
          if (!buffer) {
            throw std::runtime_error("error: invalid buffer ptr");
          }
          m_diskBuffer[writes] = buffer;
          writes++;

          m_offset += buffer->getPosition();
          if (m_offset >= m_fileSize) {
            m_offset = 0;
          }

          if (autoConsume) {
            // return buffer
            while (m_outputQueue->try_pop(buffer)) {
              // while (m_outputQueue->try_dequeue(buffer)) {
              m_pool->free(buffer->getBufferId(), buffer);
            }
          }
        }
      } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
        exit(1);
      }
    }
  }

  void measureLatency(long latencyMark) {
    double dt = 0;
    long t1 = latencyMark;
    auto currentTime = std::chrono::high_resolution_clock::now();
    auto currentTimeNano = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               currentTime.time_since_epoch())
                               .count();
    long t2 = (currentTimeNano - m_timestampReference) / 1000L;
    dt = ((double)(t2 - t1)) / 1000.; /* In milliseconds */

    m_measurements.push_back(dt);

    m_latency += dt;
    m_count += 1;
    m_min = std::min(dt, m_min);
    m_max = std::max(dt, m_max);
    m_avg = m_latency / ((double)m_count);

    if ((t2 / 1000) >= (m_kafkaDuration * 1000)) {
      stop(false);
    }
  }

  void stop(bool print = true) {
    int length = m_measurements.size();
    if (length < 1)
      return;

    if (print) {
      std::sort(m_measurements.begin(), m_measurements.end());
      std::ostringstream streamObj;
      streamObj << std::fixed;
      streamObj << std::setprecision(3);
      streamObj << "[MON] [LatencyMonitor] " << std::to_string(length) << " measurements\n";
      streamObj << "[MON] [LatencyMonitor] " << m_pid << " 5th " << std::to_string(evaluateSorted(5));
      streamObj << " 25th " << std::to_string(evaluateSorted(25));
      streamObj << " 50th " << std::to_string(evaluateSorted(50));
      streamObj << " 75th " << std::to_string(evaluateSorted(75));
      streamObj << " 99th " << std::to_string(evaluateSorted(99));
      std::cout << streamObj.str() << std::endl;
    } else {
      std::lock_guard<std::mutex> guard(m_measurementsMutex);
      //std::cout << m_pid << " adding its latency results " << std::endl;
      //m_totalMeasurements.resize(m_totalMeasurements.size() + m_measurements.size());
      m_totalMeasurements.insert(m_totalMeasurements.end(), m_measurements.begin(), m_measurements.end());
    }
    kafkaBarrier.fetch_add(-1);
    m_measureLatency = false;
  }

  double evaluateSorted(double p) {
    double n = m_measurements.size();
    double pos = p * (n + 1) / 100;
    double fpos = floor(pos);
    int intPos = (int) fpos;
    double dif = pos - fpos;

    if (pos < 1) {
      return m_measurements[0];
    }
    if (pos >= n) {
      return m_measurements[m_measurements.size() - 1];
    }

    double lower = m_measurements[intPos - 1];
    double upper = m_measurements[intPos];
    return lower + dif * (upper - lower);
  }
};

template <typename Input, typename hash>
struct Kafka {
  typedef boost::alignment::aligned_allocator<int, 64> aligned_allocator;
  using IntBuffer = std::vector<int, aligned_allocator>;

  const int m_queueSize = 64;

  int m_numberOfThreads = 0;
  int m_partitions = 0;
  IntBuffer m_partitionOffsets;
  std::vector<std::thread> m_threads;
  std::vector<std::unique_ptr<KafkaProcessor>> m_processors;
  std::vector<IntBuffer, boost::alignment::aligned_allocator<IntBuffer, 64>> m_writerOffsets;
  std::vector<IntBuffer, boost::alignment::aligned_allocator<IntBuffer, 64>> m_readerOffsets;
  std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>> m_consumerQueues;
  std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>> m_producerQueues;
  std::vector<LatQueuePtr, boost::alignment::aligned_allocator<LatQueuePtr, 64>> m_latQueues;
  std::vector<std::shared_ptr<QueryByteBuffer>> m_partitionBuffers;
  std::shared_ptr<MemoryPool> m_pool;

  long m_timestampReference = 0;

  bool m_first = true;

  /* Information used for pining worker threads to cores in-order based on the
   * socket topology */
  std::vector<int> m_orderedCores;

  explicit Kafka(int workers = SystemConf::getInstance().WORKER_THREADS,
                 long timestampReference = 0, bool autoConsume = true)
      : m_numberOfThreads(workers),
        m_partitions(m_numberOfThreads),
        m_partitionOffsets(m_numberOfThreads, 0),
        m_processors(m_numberOfThreads),
        m_writerOffsets(m_numberOfThreads, IntBuffer(1, 0)),
        m_readerOffsets(m_numberOfThreads, IntBuffer(1, 0)),
        m_consumerQueues(m_numberOfThreads,nullptr),
        m_producerQueues(m_numberOfThreads, nullptr),
        m_latQueues(m_numberOfThreads, std::make_shared<LatQueue>()),
        m_partitionBuffers(m_numberOfThreads),
        m_pool(std::make_shared<MemoryPool>(m_numberOfThreads)),
        m_timestampReference(timestampReference) {
    Utils::getOrderedCores(m_orderedCores);
    for (int i = 0; i < m_numberOfThreads; ++i) {
      // m_consumerQueues[i]->set_capacity(m_queueSize);
      m_consumerQueues[i] = std::make_shared<BoundedQueue>();//m_queueSize);
      m_producerQueues[i] = std::make_shared<BoundedQueue>();//m_queueSize);
      m_latQueues[i] = std::make_shared<LatQueue>();
      m_consumerQueues[i]->set_capacity(m_queueSize);
      m_producerQueues[i]->set_capacity(m_queueSize);
      m_processors[i] = std::make_unique<KafkaProcessor>(i, m_consumerQueues[i], m_producerQueues[i], m_pool, m_latQueues[i],timestampReference);
    }

    if (autoConsume) {
      startThreads();
    }
  }

  void startThreads() {
    for (int i = 0; i < m_numberOfThreads; ++i) {
      m_threads.emplace_back(std::thread(*m_processors[i]));
      Utils::bindProcess(m_threads[i], m_orderedCores[i + 1]);
    }
  }

  static void startWorkers() {
    std::cout << "Starting kafka workers" << std::endl;
    kafkaBarrier.store(SystemConf::getInstance().WORKER_THREADS);
    startKafka.store(true);
  }

  void processData(std::vector<char> &values) {
    auto data = (Input *)values.data();
    size_t length = values.size() / sizeof(Input);

    // partition by key
    for (size_t idx = 0; idx < length; idx++) {
      auto partition = hash::partition(data[idx], m_partitions);  // m_hash(data[idx].ad_id) % m_partitions;
      auto &buffer = m_partitionBuffers[partition];
      if (!buffer) {
        buffer = m_pool->newInstance(0);
      }
      buffer->putBytes((char *)&data[idx], sizeof(Input));
    }

    for (auto par = 0; par < m_partitions; par++) {
      auto &buffer = m_partitionBuffers[par];
      auto bytes = buffer->getPosition();
      if (buffer) {
        // while (!m_consumerQueues[par].try_push(buffer)) {
        //  std::cout << "warning: partition " + std::to_string(par) << " is full" << std::endl;
        //}
        m_writerOffsets[par][0] += bytes;
        buffer.reset();
      }
    }
  }

  void processPartitionedData(std::vector<char> &values, long latencyMark) {
    // pay the partitioning tax once and keep sending over the same data
    if (m_first) {
      auto data = (Input *)values.data();
      size_t length = values.size() / sizeof(Input);

      std::unordered_set<int> set;
      // partition by key
      for (size_t idx = 0; idx < length; idx++) {
        auto partition = hash::partition( data[idx], m_partitions); // m_hash(data[idx].ad_id) % m_partitions;
        if (set.find(partition) == set.end()) {
          set.insert(partition);
          //std::cout << "Found " << set.size() << " partitions: " << partition << std::endl;
        } else {
          //std::cout << "Found " << set.size() << " partitions until " << idx << std::endl;
        }
        auto &buffer = m_partitionBuffers[partition];
        if (!buffer) {
          buffer = m_pool->newInstance(partition);
        }
        buffer->putBytes((char *)&data[idx], sizeof(Input));
      }
      m_first = false;
    }

    for (auto par = 0; par < m_partitions; par++) {
      auto &buffer = m_partitionBuffers[par];
      if (buffer) {
        auto bytes = buffer->getPosition();
        auto tempBuffer = buffer;
        // auto tempBuffer = m_pool->newInstance(par);
        // std::memcpy(tempBuffer->getBuffer().data(),
        // buffer->getBuffer().data(), bytes); tempBuffer->setPosition(bytes);
        if (!tempBuffer) {
          throw std::runtime_error("error: adding invalid buffer to the queue");
        }
        while (!m_consumerQueues[par]->try_push(tempBuffer)) {
        //while (!m_consumerQueues[par]->try_enqueue(tempBuffer)) {
          // std::cout << "warning: partition " + std::to_string(par) << " is full" << std::endl;
          //if(!startKafka) {
          //  return;
          //}
        }
        if (SystemConf::LATENCY_ON) {
          m_latQueues[par]->push(latencyMark);
        }
        m_writerOffsets[par][0] += bytes;
      }
    }
  }

  void processPartitionedDataWithCheckpoints(std::vector<char> &values, long latencyMark) {
    // pay the partitioning tax once and keep sending over the same data
    if (m_first) {
      auto data = (Input *)values.data();
      size_t length = values.size() / sizeof(Input);

      std::unordered_set<int> set;
      // partition by key
      for (size_t idx = 0; idx < length; idx++) {
        auto partition = hash::partition( data[idx], m_partitions); // m_hash(data[idx].ad_id) % m_partitions;
        auto &buffer = m_partitionBuffers[partition];
        if (!buffer) {
          buffer = m_pool->newInstance(partition);
        }
        buffer->putBytes((char *)&data[idx], sizeof(Input));
      }
      m_first = false;
    }

    bool hasSentBarrier = false;
    bool beforeLoopBarrier = pushBarriers.load();
    for (auto par = 0; par < m_partitions; par++) {
      if (useCheckpoints && beforeLoopBarrier) {
        auto barrier = m_pool->newInstance(0);
        barrier->m_hasBarrier = true;
        if (debug) {
          std::cout << "Start pushing checkpoint barriers downstream to " + std::to_string(par) << std::endl;
        }
        while (!m_consumerQueues[par]->try_push(barrier)) {
          // std::cout << "warning: partition " + std::to_string(par) << " is full" << std::endl;
          _mm_pause();
        }
        //std::cout << "Finished pushing checkpoint barriers downstream" << std::endl;
        hasSentBarrier = true;
      } else {
        auto &buffer = m_partitionBuffers[par];
        if (buffer) {
          auto tempBuffer = buffer;
          if (!tempBuffer) {
            throw std::runtime_error("error: adding invalid buffer to the queue");
          }
          while (!m_consumerQueues[par]->try_push(tempBuffer)) {
            // std::cout << "warning: partition " + std::to_string(par) << " is full" << std::endl;
            _mm_pause();
          }
          if (SystemConf::LATENCY_ON) {
            m_latQueues[par]->push(latencyMark);
          }
        }
      }
    }

    if (useCheckpoints && beforeLoopBarrier && hasSentBarrier) {
      //std::cout << "Unsetting pushBarriers flag" << std::endl;
      pushBarriers.store(false);
    }
  }

  void measureLatency() {
    //while (kafkaBarrier.load() != 0) {
    //  ;
    //}
    if (kafkaBarrier.load() != 0) {
      std::this_thread::sleep_for(std::chrono::seconds (2));
    }
    std::sort(m_totalMeasurements.begin(), m_totalMeasurements.end());
    std::ostringstream streamObj;
    streamObj << std::fixed;
    streamObj << std::setprecision(3);
    streamObj << "[MON] [LatencyMonitor] 5th " << std::to_string(evaluateSorted(5, m_totalMeasurements));
    streamObj << " 25th " << std::to_string(evaluateSorted(25, m_totalMeasurements));
    streamObj << " 50th " << std::to_string(evaluateSorted(50, m_totalMeasurements));
    streamObj << " 75th " << std::to_string(evaluateSorted(75, m_totalMeasurements));
    streamObj << " 99th " << std::to_string(evaluateSorted(99, m_totalMeasurements));
    std::cout << streamObj.str() << std::endl;
  }

  double evaluateSorted(double p, std::vector<double> &totalMeasurements) {
    if (totalMeasurements.empty()) {
      return -1;
    }
    double n = totalMeasurements.size();
    double pos = p * (n + 1) / 100;
    double fpos = floor(pos);
    int intPos = (int) fpos;
    double dif = pos - fpos;

    if (pos < 1) {
      return totalMeasurements[0];
    }
    if (pos >= n) {
      return totalMeasurements[totalMeasurements.size() - 1];
    }

    double lower = totalMeasurements[intPos - 1];
    double upper = totalMeasurements[intPos];
    return lower + dif * (upper - lower);
  }
};