#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <tbb/concurrent_queue.h>
#include <unistd.h>

#include <boost/align/aligned_allocator.hpp>
#include <cfloat>
#include <cstdio>
#include <unordered_set>
#include <vector>

#include "BenchmarkUtils.h"
#include "benchmarks/kafka-flink/queues/readerwritercircularbuffer.h"
#include "benchmarks/kafka-flink/queues/readerwriterqueue.h"
#include "snappy.h"
#include "utils/Utils.h"

/*
 * \brief A prototype implementation of Flink in C++.
 *
 * */

std::atomic<bool> startFlink = false;
// for checkpointing
const bool batchResults = true;
const bool copyInput = false;
thread_local std::unordered_set<BoundedQueuePtr> blockedQueues;

std::atomic<int> flinkBarrier;
size_t m_flinkDuration = 60;
thread_local bool m_flinkMeasureLatency = true;
std::mutex m_flinkMeasurementsMutex;
std::vector<double> m_flinkTotalMeasurements;
int m_flinkBatchSize = 8;

template<typename Op>
struct FlinkProcessor {
  int m_opId, m_pid, m_allocateId;
  std::shared_ptr<MemoryPool> m_pool, m_kafkaPool = nullptr;
  BoundedQueuePtr m_inputQueue;
  std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>> *m_inputShuffleQueues;
  BoundedQueuePtr m_outputQueue;
  std::shared_ptr<QueryByteBuffer> m_tempBuffer, m_tempResBuffer;

  long m_watermark = 0;
  long limitOffset = 100;
  long limit = 100;

  bool m_hasShuffle = false;

  // upstream queues
  int m_upstreamQueues = 0;

  // downstream queues
  std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>> *m_nextOperatorQueues;
  std::vector<std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>>> *m_nextOperatorShuffleQueues;
  std::vector<std::shared_ptr<QueryByteBuffer>> m_partitionBuffers;
  int m_itemsToSend = -1, m_itemsToSend_ = 0;

  // file properties
  std::vector<std::shared_ptr<QueryByteBuffer>> m_diskBuffer;
  size_t m_offset = 0;
  size_t m_fileSize = 64 * 1024 * 1024;
  const std::string m_fileName;
  int m_fd;

  // used for latency measurements
  long m_timestampReference = 0;
  LatQueuePtr m_latQueue;
  int m_repeat = 0;
  std::vector<double> m_measurements;
  long m_count = 0;
  double m_min = DBL_MAX, m_max = DBL_MIN, m_avg = 0;
  double m_latency = 0;

  FlinkProcessor(int opId, int pid, bool hasShuffle, BoundedQueuePtr &inputQueue, BoundedQueuePtr &outputQueue,
                 std::shared_ptr<MemoryPool> &pool, std::shared_ptr<MemoryPool> kafkaPool = nullptr,
                 std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>> *nextOperatorQueues = nullptr,
                 std::vector<std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>>> *nextOperatorShuffleQueues = nullptr)
      : m_opId(opId),
        m_pid(pid),
        m_hasShuffle(hasShuffle),
        m_pool(pool),
        m_kafkaPool(kafkaPool),
        m_inputQueue(inputQueue),
        m_outputQueue(outputQueue),
        m_nextOperatorQueues(nextOperatorQueues),
        m_nextOperatorShuffleQueues(nextOperatorShuffleQueues),
        m_diskBuffer(m_flinkBatchSize),
        m_fileName(SystemConf::FILE_ROOT_PATH + "/kafka/flink_file_" + std::to_string(opId) + "_" + std::to_string(m_pid)) {
    m_allocateId = (m_opId == 0) ? m_pid : m_opId * SystemConf::getInstance().WORKER_THREADS + m_pid - 1;
    m_tempBuffer = m_pool->newInstance(m_pid);

    if (m_nextOperatorQueues) {
      m_partitionBuffers.resize(m_nextOperatorQueues->size());
      for (auto &p: m_partitionBuffers) {
        //p = m_pool->newInstance(m_pid);
      }
    }

    // create file
    std::remove(m_fileName.c_str());
    m_fd = ::open(m_fileName.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  }

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
  void operator()() {
    while (!startFlink)
      ;

    std::shared_ptr<QueryByteBuffer> buffer;
    while (true) {
      try {
        long latencyMark = -1;
        buffer = getNextData();

        if (SystemConf::LATENCY_ON && m_latQueue) {
          while (!m_latQueue->try_pop(latencyMark))
            ;
        }

        //__builtin_prefetch(buffer->getBuffer().data(), 1, 3);

        // decompress data
        if (buffer->m_compressed) {
          snappy::RawUncompress(buffer->getBuffer().data(), buffer->getPosition(), m_tempBuffer->getBuffer().data());
          m_tempBuffer->setPosition(buffer->m_originalPosition);
          freeInputBuffer(buffer);
          buffer = m_tempBuffer;
        } else if (m_opId == 0) {
          if (copyInput) {
            std::memcpy(m_tempBuffer->getBuffer().data(), buffer->getBuffer().data(), buffer->getPosition());
            m_tempBuffer->setPosition(buffer->getPosition());
            ////freeInputBuffer(buffer);
            buffer = m_tempBuffer;
          }
        } else if (m_opId > 0 && buffer->m_latencyMark > 0) {
          latencyMark = buffer->m_latencyMark;
        }

        auto resultBuffer = m_pool->newInstance(m_pid);
        resultBuffer->m_latencyMark = latencyMark;
        // process data
        Op::process(m_pid, buffer->m_watermark, buffer->getBuffer().data(), buffer->getPosition(), resultBuffer);

        // add watermark
        if (m_watermark >= limit) {
          resultBuffer->m_watermark = m_watermark;
          limit += limitOffset;
        }
        // Put result in the output queue
        if (/*resultBuffer->m_hasBarrier || */resultBuffer->getPosition() > 0 || resultBuffer->m_watermark > 0) {
          // batch results to save up space
          if (batchResults) {
            if (!m_tempResBuffer) {
              m_tempResBuffer = resultBuffer;
            } else {
              if (m_tempResBuffer->tryToMerge(resultBuffer)) {
                m_pool->free(resultBuffer->m_id, resultBuffer);
              } else {
                addToOutputQueue(m_tempResBuffer);
                m_tempResBuffer = resultBuffer;
              }
            }
          } else {
            addToOutputQueue(resultBuffer);
          }
        } else {
          m_pool->free(resultBuffer->m_id, resultBuffer);
        }
        resultBuffer.reset();

        // send downstream
        if (m_hasShuffle) {
          if (useCheckpoints) {
            // todo: this won't work if there are no items to send
            if (m_itemsToSend >= 0) {
              // Op::sendDownstream(m_pid, m_outputQueue, nullptr,m_partitionBuffers, m_pool);
              while (m_outputQueue->try_pop(buffer)) {
                if (buffer) {
                  if (SystemConf::LATENCY_ON && m_flinkMeasureLatency) {
                    measureLatency(buffer->m_latencyMark);
                  }
                  m_pool->freeUnsafe(buffer->m_id, buffer);
                }
              }
              releaseBarrier.fetch_add(-1);
              m_itemsToSend = -1;
              if (debug) {
                std::cout << "Worker " + std::to_string(m_pid) << " decreased the release barrier" << std::endl;
              }
            }
          } else {
            //Op::sendDownstream(m_pid, m_outputQueue, nullptr, m_partitionBuffers, m_pool);
            while (m_outputQueue->try_pop(buffer)) {
              if (buffer) {
                if (SystemConf::LATENCY_ON && m_flinkMeasureLatency) {
                  measureLatency(buffer->m_latencyMark);
                }
                m_pool->freeUnsafe(buffer->m_id, buffer);
              }
            }
          }
          reportLatency();
        } else {
          Op::sendDownstream(m_pid, m_outputQueue, m_nextOperatorShuffleQueues, m_partitionBuffers, m_pool);
        }

        // increase watermark
        m_watermark += YSBQuery::m_timestampOffset;

      } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
        exit(1);
      }
    }
  }

  inline void freeInputBuffer(std::shared_ptr<QueryByteBuffer> &buffer) {
    if (m_kafkaPool) {
      m_kafkaPool->free(buffer->m_id, buffer);
    } else {
      m_pool->freeUnsafe(buffer->m_id, buffer);
    }
    buffer.reset();
  }

  inline std::shared_ptr<QueryByteBuffer> getNextData() {
    std::shared_ptr<QueryByteBuffer> buffer;
    if (!useCheckpoints) {
      if (m_hasShuffle) {
        if (!m_inputShuffleQueues) {
          throw std::runtime_error(
              "error: m_inputShuffleQueues are not initialized");
        }
        while (true) {
          for (auto &sq : *m_inputShuffleQueues) {
            sq->try_pop(buffer);
            if (buffer) {
              return buffer;
            }
          }
          // std::cout << "warning: worker " + std::to_string(m_pid) << " is waiting for work" << std::endl;
        }
      } else {
        while (!m_inputQueue->try_pop(buffer) || !buffer) {
          // std::cout << "warning: worker " + std::to_string(m_pid) << " is waiting for work" << std::endl;
          std::this_thread::sleep_for(std::chrono::nanoseconds(1));
        }
      }
    } else {
      if (m_hasShuffle) {
        if (!m_inputShuffleQueues) {
          throw std::runtime_error(
              "error: m_inputShuffleQueues are not initialized");
        }
        while (true) {
          int cnt = 0;
          for (auto &sq : *m_inputShuffleQueues) {
            if (blockedQueues.find(sq) == blockedQueues.end()) {
              sq->try_pop(buffer);
            }

            if (buffer) {
              if (buffer->m_hasBarrier) {
                if (debug) {
                  std::cout << "Worker " + std::to_string(m_pid) << " received a barrier from " + std::to_string(cnt) << std::endl;
                }
                // block queue
                blockedQueues.insert(sq);
                m_pool->free(buffer->m_id, buffer);
                buffer = nullptr;
              } else {
                return buffer;
              }

              // if all queues blocked
              if (blockedQueues.size() == m_upstreamQueues) {
                if (m_tempResBuffer && m_tempResBuffer->getPosition() > 0) {
                  addToOutputQueue(m_tempResBuffer);
                  m_tempResBuffer.reset();
                  m_itemsToSend_++;
                }
                m_itemsToSend = m_itemsToSend_;
                m_itemsToSend_ = 0;
                //auto &queues = (*m_nextOperatorShuffleQueues)[m_pid];
                // send out markers
                /*for (auto par = 0; par < queues.size(); par++) {
                  auto barrierBuf = m_pool->newInstance(par);
                  barrierBuf->m_hasBarrier = true;
                  while (!queues[par]->try_push(barrierBuf))
                    ;
                }*/
                // take a snapshot
                Op::checkpoint(m_pid, m_fd, m_tempBuffer);
                // update checkpoint counter
                checkpointCounter.fetch_add(1);
                // clear the blocked queues
                blockedQueues.clear();
              }
            }
            cnt++;
          }
        }
      } else {
        while (true) {
          while (!m_inputQueue->try_pop(buffer) || !buffer) {
            // std::cout << "warning: worker " + std::to_string(m_pid) << " is waiting for work" << std::endl;
            std::this_thread::sleep_for(std::chrono::nanoseconds(1));
          }
          if (buffer->m_hasBarrier) {
            // block queue
            blockedQueues.insert(m_inputQueue);
            m_pool->free(buffer->m_id, buffer);
            buffer = nullptr;
            // if all queues blocked
            if (blockedQueues.size() == 1) {
              auto &queues = (*m_nextOperatorShuffleQueues)[m_pid];
              // send out markers
              if (debug) {
                std::cout << "Worker op0_" + std::to_string(m_pid) << " starts sending barriers" << std::endl;
              }
              if (batchResults) {
                addToOutputQueue(m_tempResBuffer);
                m_tempResBuffer.reset();
                Op::sendDownstream(m_pid, m_outputQueue, m_nextOperatorShuffleQueues, m_partitionBuffers, m_pool);
              }
              for (auto par = 0; par < queues.size(); par++) {
                auto barrierBuf = m_pool->newInstance(par);
                barrierBuf->m_hasBarrier = true;
                int cnt = 0;
                while (!queues[par]->try_push(barrierBuf)) {
                  cnt++;
                  if ((cnt % 1000000) == 0) {
                    //std::cout << "warning: waiting to send the barrier downstream" << std::endl;
                  }
                }
                if (debug) {
                  std::cout << "Worker op0_" + std::to_string(m_pid) << " sent a barrier to " + std::to_string(par) << std::endl;
                }
              }
              // take a snapshot
              Op::checkpoint(m_pid, m_fd, m_tempBuffer);
              // update checkpoint counter
              checkpointCounter.fetch_add(1);
              // clear the blocked queues
              blockedQueues.clear();
              //std::cout << "Worker " + std::to_string(m_pid) << " unblocked its queues" << std::endl;
            }
          } else {
            break;
          }
        }
      }
    }
    return buffer;
  }

  inline void addToOutputQueue(std::shared_ptr<QueryByteBuffer> &resultBuffer) {
    /*if (resultBuffer->m_watermark > 0) {
      std::cout << "sending watermark " + std::to_string(resultBuffer->m_watermark) << std::endl;
    }*/
    bool flag = m_outputQueue->try_push(resultBuffer);
    if (!flag && m_outputQueue->capacity() == m_outputQueue->size()) {
      //if (m_pid == 0) {
      std::cout << "warning: increasing the output queue size in pipeline " + std::to_string(m_hasShuffle) << std::endl;
      //}
      m_outputQueue->set_capacity(m_outputQueue->capacity() * 2);
      m_outputQueue->push(resultBuffer);
    }
    m_itemsToSend_++;
  }

  inline void measureLatency(long latencyMark) {
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

    if ((t2 / 1000) >= (m_flinkDuration * 1000)) {
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
      std::lock_guard<std::mutex> guard(m_flinkMeasurementsMutex);
      //std::cout << m_pid << " adding its latency results " << std::endl;
      //m_flinkTotalMeasurements.resize(m_flinkTotalMeasurements.size() + m_measurements.size());
      m_flinkTotalMeasurements.insert(m_flinkTotalMeasurements.end(), m_measurements.begin(), m_measurements.end());
    }
    flinkBarrier.fetch_add(-1);
    m_flinkMeasureLatency = false;
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

  inline void reportLatency() {
    if (SystemConf::LATENCY_ON && m_count > 0) {
      m_repeat++;
      if (m_pid == 0 && m_repeat == 1024) {
        std::cout << "Latency metrics [avg " + std::to_string(m_avg) +
            "] " + "[min " + std::to_string(m_min) + "] " +
            "[max " + std::to_string(m_max) + "] "
                  << std::endl;
        m_repeat = 0;
      }
    }
  }
};


// todo: this template doesn't work...
template<typename Op, typename nextOp>
struct Operator {
  typedef boost::alignment::aligned_allocator<int, 64> aligned_allocator;
  using IntBuffer = std::vector<int, aligned_allocator>;

  const int m_queueSize = 32;

  int m_opId = 0;
  int m_numberOfThreads = 0;
  int m_partitions = 0;
  IntBuffer m_partitionOffsets;
  std::vector<std::thread> m_threads;
  std::vector<std::unique_ptr<FlinkProcessor<Op>>> m_processors;
  std::vector<IntBuffer, boost::alignment::aligned_allocator<IntBuffer, 64>> m_readerOffsets;
  std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>> m_consumerQueues;
  std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>> m_producerQueues;
  std::vector<LatQueuePtr, boost::alignment::aligned_allocator<LatQueuePtr, 64>> m_latQueues;
  std::vector<std::shared_ptr<UnboundedQueryBuffer>> m_partitionBuffers;
  std::shared_ptr<MemoryPool> m_pool, m_kafkaPool = nullptr;

  // for grouping operators
  bool m_hasShuffle = false;
  std::vector<std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>>> m_shuffleQueues;
  int m_upstreamQueues = 0;

  long m_timestampReference = 0;

  /* Information used for pining worker threads to cores in-order based on the
   * socket topology */
  std::vector<int> m_orderedCores;

  // next operator -- assume only one downstream operator
  std::shared_ptr<Operator<nextOp, nextOp>> m_nextOperator;
  std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>> *m_nextOperatorQueues = nullptr;
  std::vector<std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>>> *m_nextOperatorShuffleQueues = nullptr;

  explicit Operator(int opId, std::shared_ptr<MemoryPool> &pool, bool hasShuffle, int workers = SystemConf::getInstance().WORKER_THREADS,
                 long timestampReference = 0)
      : m_opId(opId),
        m_numberOfThreads(workers),
        m_partitions(m_numberOfThreads),
        m_partitionOffsets(m_numberOfThreads, 0),
        m_processors(m_numberOfThreads),
        m_readerOffsets(m_numberOfThreads, IntBuffer(1, 0)),
        m_consumerQueues(m_numberOfThreads,std::make_shared<BoundedQueue>()),
        m_producerQueues(m_numberOfThreads,std::make_shared<BoundedQueue>()),
        m_latQueues(m_numberOfThreads, std::make_shared<LatQueue>()),
        m_partitionBuffers(m_numberOfThreads),
        m_pool(pool), m_hasShuffle(hasShuffle),
        m_timestampReference(timestampReference) {

    if (m_hasShuffle) {
      m_shuffleQueues.resize(m_partitions);
      for (auto &sq: m_shuffleQueues) {
        sq.resize(m_partitions);
        for (int i = 0; i < m_numberOfThreads; ++i) {
          sq[i] = std::make_shared<BoundedQueue>();
          sq[i]->set_capacity(m_queueSize);
        }
      }
    }
  }


  void connectWith(std::shared_ptr<Operator<nextOp, nextOp>> &next) {
    m_nextOperatorQueues = new std::vector<BoundedQueuePtr, boost::alignment::aligned_allocator<BoundedQueuePtr, 64>>();
    for (auto &q: next->m_consumerQueues) {
      m_nextOperatorQueues->push_back(q);
    }
    if (next->m_hasShuffle) {
      m_nextOperatorShuffleQueues = &next->m_shuffleQueues;
    }
    m_nextOperator = next;

    next->m_upstreamQueues = m_partitions;
  }

  void setupOperator(bool autoConsume = true) {
    Utils::getOrderedCores(m_orderedCores);
    for (int i = 0; i < m_numberOfThreads; ++i) {
      m_consumerQueues[i] = std::make_shared<BoundedQueue>();
      m_consumerQueues[i]->set_capacity(m_queueSize);
      m_producerQueues[i] = std::make_shared<BoundedQueue>();
      m_producerQueues[i]->set_capacity(m_queueSize);
      m_latQueues[i] = std::make_shared<LatQueue>();
      m_processors[i] = std::make_unique<FlinkProcessor<Op>>(m_opId, i, m_hasShuffle, m_consumerQueues[i], m_producerQueues[i], m_pool, m_kafkaPool, m_nextOperatorQueues, m_nextOperatorShuffleQueues);
      m_processors[i]->m_timestampReference = m_timestampReference;
      if (m_hasShuffle) {
        m_processors[i]->m_inputShuffleQueues = &m_shuffleQueues[i];
        m_processors[i]->m_upstreamQueues = m_upstreamQueues;
      } else {
        m_processors[i]->m_latQueue = m_latQueues[i];
      }
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
};

struct JobManager {
  std::shared_ptr<Operator<YSBQuery::StatelessOp, YSBQuery::StateFulOp>> m_op1;
  std::shared_ptr<MemoryPool> m_pool;
  long m_checkpointId = 0;
  long m_checkpoinDur = 0;
  long m_checkpoinCnt = 0;

  [[maybe_unused]] JobManager(std::shared_ptr<Operator<YSBQuery::StatelessOp, YSBQuery::StateFulOp>>&op1,
      std::shared_ptr<MemoryPool> &pool)
      : m_op1(op1), m_pool(pool) {}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
  [[noreturn]] void operator()() {
    while (!startFlink)
      ;

    std::cout << "[CP] starting the checkpoint coordinator" << std::endl;
    auto t1 = std::chrono::high_resolution_clock::now();
    auto t2 = t1;
    auto time_span =
        std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);

    while(true) {
      auto duration =
          std::max((int)(SystemConf::getInstance().CHECKPOINT_INTERVAL -
              (size_t) (time_span.count() * 1000)), 0);
      if (duration) {
        //std::cout << "[CP] sleeping for " + std::to_string(duration) << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(duration));
      }

      t1 = std::chrono::high_resolution_clock::now();
      std::cout << "[CP] starting checkpoint " + std::to_string(m_checkpointId) << std::endl;
      // reset counter and barrier
      checkpointCounter.store(0);
      releaseBarrier.store(SystemConf::getInstance().WORKER_THREADS);

      // insert marker to the first operator
      pushBarriers.store(true);
      /*auto &queues = m_op1->m_consumerQueues;
      for (auto &q: queues) {
        auto barrier = m_pool->newInstance(0);
        barrier->m_hasBarrier = true;
        while (!q->try_push(barrier))
          ;
      }*/

      while (checkpointCounter.load() != SystemConf::getInstance().WORKER_THREADS * 2) {
        //std::cout << "[CP] waiting for the checkpointCounter: " + std::to_string(checkpointCounter) << std::endl;
        //std::this_thread::sleep_for(std::chrono::nanoseconds (1));
        _mm_pause();
      }

      if (debug) {
        std::cout << "[CP] waiting for the releaseBarrier: " + std::to_string(releaseBarrier) << std::endl;
      }

      while (releaseBarrier.load() != 0){
      //  std::cout << "[CP] waiting for the releaseBarrier: " + std::to_string(releaseBarrier) << std::endl;
      //  //std::this_thread::sleep_for(std::chrono::nanoseconds (1));
        _mm_pause();
      }

      m_checkpointId++;
      typedef std::chrono::milliseconds ms;
      t2 = std::chrono::high_resolution_clock::now();
      time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
      auto ms_time = std::chrono::duration_cast<ms>(time_span);
      m_checkpoinDur += ms_time.count();
      m_checkpoinCnt++;
      std::cout << "[CP] checkpoint duration " + std::to_string(ms_time.count()) + " ms "
                       "[AVG:" + std::to_string(m_checkpoinDur/m_checkpoinCnt) + "]" << std::endl;
    }
  }
};

struct FlinkYSB {
  int m_numberOfThreads = 0;
  int m_partitions = 0;
  std::vector<std::shared_ptr<QueryByteBuffer>> m_partitionBuffers;
  std::shared_ptr<MemoryPool> m_pool;

  long m_timestampReference = 0;

  bool m_first = true;

  std::shared_ptr<Operator<YSBQuery::StatelessOp, YSBQuery::StateFulOp>> m_op1;
  std::shared_ptr<Operator<YSBQuery::StateFulOp, YSBQuery::StateFulOp>> m_op2;

  std::vector<std::thread> m_threads;
  std::unique_ptr<JobManager> m_jobManager;

  // kafka variables
  Kafka<YSBQuery::InputSchema, YSBQuery::hash> *m_kakfa;
  std::shared_ptr<MemoryPool> m_kafkaPool;

  explicit FlinkYSB(int workers = SystemConf::getInstance().WORKER_THREADS,
                 long timestampReference = 0, bool autoConsume = true)
      : m_numberOfThreads(workers),
        m_partitions(m_numberOfThreads),
        m_partitionBuffers(m_numberOfThreads),
        m_pool(std::make_shared<MemoryPool>(m_numberOfThreads * 2)),
        m_timestampReference(timestampReference) {

    m_op1 = std::make_shared<Operator<YSBQuery::StatelessOp, YSBQuery::StateFulOp>>(0, m_pool, false, m_partitions, timestampReference);
    m_op2 = std::make_shared<Operator<YSBQuery::StateFulOp, YSBQuery::StateFulOp>>(1, m_pool, true, m_partitions, timestampReference);
    m_op1->connectWith(m_op2);
    m_op1->setupOperator(autoConsume);
    m_op2->setupOperator(autoConsume);

    if (useCheckpoints) {
      m_jobManager = std::make_unique<JobManager>(m_op1, m_pool);
      m_threads.emplace_back(std::thread(*m_jobManager));
      Utils::bindProcess(m_threads[0], 0);
    }
  }

  static void startWorkers() {
    std::cout << "Starting flink workers" << std::endl;
    startFlink.store(true);
  }

  void connect(Kafka<YSBQuery::InputSchema, YSBQuery::hash> *kafka) {
    if (!kafka) {
      throw std::runtime_error("error: kafka is not set");
    }
    m_kakfa = kafka;
    m_kafkaPool = kafka->m_pool;
    int idx = 0;
    for (auto &q: m_op1->m_consumerQueues) {
      m_op1->m_processors[idx]->m_kafkaPool = m_kafkaPool;
      kafka->m_producerQueues[idx] = q;
      kafka->m_processors[idx]->m_outputQueue = q;
      if (SystemConf::LATENCY_ON) {
        kafka->m_latQueues[idx] = m_op1->m_latQueues[idx];
        m_op1->m_processors[idx]->m_latQueue = m_op1->m_latQueues[idx];
      }
      idx++;
    }

    // start now the threads
    kafka->startThreads();
    m_op1->startThreads();
    m_op2->startThreads();
  }

  void processPartitionedData(std::vector<char> &values, long latencyMark) {
    // pay the partitioning tax once and keep sending over the same data
    if (m_first) {
      auto data = (YSBQuery::InputSchema *)values.data();
      size_t length = values.size() / sizeof(YSBQuery::InputSchema);

      // partition by key
      for (size_t idx = 0; idx < length; idx++) {
        auto partition = YSBQuery::hash::partition( data[idx], m_partitions);  // m_hash(data[idx].ad_id) % m_partitions;
        auto &buffer = m_partitionBuffers[partition];
        if (!buffer) {
          buffer = m_pool->newInstance(partition);
        }
        buffer->putBytes((char *)&data[idx], sizeof(YSBQuery::InputSchema));
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
        while (!m_op1->m_consumerQueues[par]->try_push(barrier)) {
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
          while (!m_op1->m_consumerQueues[par]->try_push(tempBuffer)) {
            // std::cout << "warning: partition " + std::to_string(par) << " is full" << std::endl;
            _mm_pause();
          }
          if (SystemConf::LATENCY_ON) {
            m_op1->m_latQueues[par]->push(latencyMark);
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
    if (flinkBarrier.load() != 0) {
      std::this_thread::sleep_for(std::chrono::seconds (2));
    }
    std::sort(m_flinkTotalMeasurements.begin(), m_flinkTotalMeasurements.end());
    std::ostringstream streamObj;
    streamObj << std::fixed;
    streamObj << std::setprecision(3);
    streamObj << "[MON] [LatencyMonitor] 5th " << std::to_string(evaluateSorted(5, m_flinkTotalMeasurements));
    streamObj << " 25th " << std::to_string(evaluateSorted(25, m_flinkTotalMeasurements));
    streamObj << " 50th " << std::to_string(evaluateSorted(50, m_flinkTotalMeasurements));
    streamObj << " 75th " << std::to_string(evaluateSorted(75, m_flinkTotalMeasurements));
    streamObj << " 99th " << std::to_string(evaluateSorted(99, m_flinkTotalMeasurements));
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