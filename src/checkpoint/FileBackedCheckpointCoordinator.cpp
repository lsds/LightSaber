#include "checkpoint/FileBackedCheckpointCoordinator.h"

#include <xmmintrin.h>

#include <cassert>
#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/persistent_ptr_base.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <unordered_set>

#include "buffers/UnboundedQueryBufferFactory.h"
#include "checkpoint/Checkpoint.h"
#include "checkpoint/CheckpointStatistics.h"
#include "checkpoint/Recovery.h"
#include "dispatcher/ITaskDispatcher.h"
#include "filesystem/File.h"
#include "result/PartialResultSlot.h"
#include "result/ResultHandler.h"
#include "tasks/TaskFactory.h"
#include "tasks/WindowBatchFactory.h"
#include "utils/Channel.h"
#include "utils/Guid.h"
#include "utils/Query.h"
#include "utils/QueryConfig.h"
#include "utils/QueryOperator.h"
#include "utils/Utils.h"

class AckCheckpointContext : public IAsyncContext {
 public:
  AckCheckpointContext(Checkpoint *checkpoint, int slotId, std::atomic<int> *slot,
                       int previousState, std::atomic<int> *frCounter,
                       std::mutex *lock, std::shared_ptr<UnboundedQueryBuffer> buffer)
      : m_checkpoint(checkpoint),
        m_slotId(slotId),
        m_slot(slot),
        m_previousState(previousState),
        m_frCounter(frCounter),
        m_lock(lock),
        m_buffer(buffer) {
    if (previousState != 1 && previousState != 3)
      throw std::runtime_error("error: invalid previousState value " + std::to_string(previousState));
  }

 protected:
  Status deepCopyInternal(IAsyncContext *&context_copy) final {
    return IAsyncContext::deepCopyInternal(*this, context_copy);
  }

 public:
  Checkpoint *m_checkpoint;
  int m_slotId;
  std::atomic<int> *m_slot;
  int m_previousState;
  std::atomic<int> *m_frCounter;
  std::mutex *m_lock;
  std::shared_ptr<UnboundedQueryBuffer> m_buffer;
};

FileBackedCheckpointCoordinator::FileBackedCheckpointCoordinator(
    long jobId, const std::vector<std::shared_ptr<Query>> &queries, std::atomic<bool> *clearFiles,
    std::shared_ptr<disk_t> filesystem, bool triggersCheckpoints)
    : m_jobId(jobId),
      m_triggersCheckpoints(triggersCheckpoints),
      m_waitCondition(false),
      m_guid(Guid::Create()),
      m_checkpointId(0),
      m_checkpointCounter(0),
      m_recoveryCounter(0),
      m_numOfQueries(queries.size()),
      m_queries(queries),
      m_checkpoints(m_numOfQueries, std::vector<std::shared_ptr<Checkpoint>>(
                                        m_numberOfCheckpoints)),
      m_recoveries(m_numOfQueries),
      m_checkpointInput(m_numOfQueries, std::vector<bool>(2, false)),
      m_lastTaskId(m_numOfQueries),
      m_checkpointPtrs(m_numOfQueries, std::vector<int>(12, -1)),
      m_taskDispatchers(m_numOfQueries),
      m_resultHandlers(m_numOfQueries),
      m_hasConcurrentCheckpoint(false),
      m_intermSizes(m_numOfQueries),
      m_outputSizes(m_numOfQueries),
      m_expectedBytes(0),
      m_measuredBytes(0),
      m_statistics(std::make_unique<CheckpointStatistics>()),
      m_useCompression(m_numOfQueries),
      m_compressionFP(m_numOfQueries),
      m_decompressionFP(m_numOfQueries),
      m_readySlots(m_numOfQueries, std::vector<tbb::concurrent_queue<int>>(3)),
      m_filesystem(filesystem),
      m_poolSize(PMEMOBJ_MIN_POOL),
      m_pmFileName("scabbard/checkpoint_metadata_" + std::to_string(jobId)),
      m_slotFileSize(SystemConf::getInstance().BLOCK_SIZE),
      m_asyncFileNames(m_numOfQueries,
                       std::vector<std::string>(3 * m_numberOfCheckpoints)),
      m_asyncFiles(m_numOfQueries,
                   std::vector<file_t *>(3 * m_numberOfCheckpoints)),
      m_asyncFileOptions(std::make_unique<FileOptions>(true, false)),
      m_clearFiles(clearFiles), m_ready(false) {
  // initialize persistent memory for metadata
  try {
    /* Bind main thread to a CPU core */
    Utils::bindProcess(SystemConf::getInstance().WORKER_THREADS + 1);

    if (m_clearFiles && m_clearFiles->load()) {
      std::vector<std::string> files;
      auto path = SystemConf::FILE_ROOT_PATH + "/scabbard";
      Utils::readDirectory(path, files);
      for (auto &f : files) {
        auto absolutePath = path + "/" + f;
        if (f != "." && f != ".." && std::experimental::filesystem::is_directory(absolutePath)) {
          auto res = std::experimental::filesystem::remove_all(absolutePath);
          if (res == 0) {
            std::cout << "Failed to remove folder " << (absolutePath) << std::endl;
          } else {
            std::cout << "Removing folder " << (absolutePath) << std::endl;
          }
        }
        if (f != "." && f != ".." && f.find("checkpoint_metadata_") != std::string::npos) {
          auto res = std::experimental::filesystem::remove_all(absolutePath);
          if (res == 0) {
            std::cout << "Failed to remove file " << (absolutePath) << std::endl;
          } else {
            std::cout << "Removing file " << (absolutePath) << std::endl;
          }
        }
      }
    }

    if (!m_filesystem) {
      std::cout << "warning: no filesystem passed to the constructor. "
                   "Initializing a new filesystem for CP..."
                << std::endl;
      m_filesystem = std::make_shared<disk_t>(SystemConf::FILE_ROOT_PATH);
    }

    Utils::tryCreateDirectory(m_filesystem->getRootPath() + "scabbard");
    auto pmPath = m_filesystem->getRootPath() + m_pmFileName;
    if (Utils::fileExists(pmPath.c_str()) != 0) {
      m_pop = pmem::obj::pool<PMem>::create(pmPath.c_str(), "", m_poolSize,
                                            CREATE_MODE_RW);
      m_root = m_pop.root();
      pmem::obj::make_persistent_atomic<PMem>(m_pop, m_root->next);
      pmem::obj::transaction::run(m_pop, [&] { m_root = m_root->next; });
      persistGuid();
    } else {
      m_pop = pmem::obj::pool<PMem>::open(pmPath, "");
      m_root = m_pop.root();
      m_root = m_root->next;
      std::string guidS;
      for (size_t idx = 0; idx < m_root->m_guidSize; ++idx) {
        guidS += m_root->m_guid[idx];
      }
      m_guid = Guid::Parse(guidS);
    }
  } catch (const pmem::pool_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return;
  } catch (const pmem::transaction_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return;
  }
  // Open File handlers
  m_pmFile = m_filesystem->newFile(m_pmFileName);
  m_filesystem->createOrOpenCheckpointDirectory(m_guid);

  // get handles to task dispatcher and result buffers
  for (int q = 0; q < m_numOfQueries; ++q) {
    m_taskDispatchers[q] = m_queries[q]->getTaskDispatcher();
    m_resultHandlers[q] = m_queries[q]->getResultHandler();

    if (m_clearFiles && !m_clearFiles->load()) {
      m_recoveries[q] = std::make_shared<Recovery>();
      m_recoveries[q]->setRecoveryCounter(&m_recoveryCounter);
    }

    for (int checkpoint = 0; checkpoint < m_numberOfCheckpoints; ++checkpoint) {
      m_checkpoints[q][checkpoint] = std::make_shared<Checkpoint>();
      if (m_queries[q]->isMarkedForCheckpoint()) {
        m_asyncFileNames[q][checkpoint] =
            "checkpoint_" + std::to_string(jobId) + "_" + std::to_string(q) +
            "_v" + std::to_string(checkpoint);
        auto relativePath = m_filesystem->getRelativeCheckpointPath(m_guid);
        m_asyncFiles[q][checkpoint] = m_filesystem->newFile(
            relativePath  + m_asyncFileNames[q][checkpoint],
            m_resultHandlers[q]->m_numberOfSlots * m_slotFileSize);
        if (!m_queries[q]->getBuffer()->isPersistent() && m_checkpointInputQueues) {
          m_asyncFileNames[q][checkpoint + 2] =
              "checkpoint_buffer1_" + std::to_string(jobId) + "_" +
              std::to_string(q) + "_v" + std::to_string(checkpoint);
          m_asyncFiles[q][checkpoint + 2] = m_filesystem->newFile(
              relativePath  + m_asyncFileNames[q][checkpoint + 2], m_queries[q]->getBuffer()->getCapacity());
          m_queries[q]->getBuffer()->setupForCheckpoints(m_filesystem);
          m_checkpointInput[q][0] = true;
        }
        if (m_queries[q]->getSecondSchema() &&
            !m_queries[q]->getSecondBuffer()->isPersistent() &&
            m_checkpointInputQueues) {
          m_asyncFileNames[q][checkpoint + 4] =
              "checkpoint_buffer2_" + std::to_string(jobId) + "_" +
              std::to_string(q) + "_v" + std::to_string(checkpoint);
          m_asyncFiles[q][checkpoint + 4] = m_filesystem->newFile(
              relativePath + m_asyncFileNames[q][checkpoint + 4], m_queries[q]->getSecondBuffer()->getCapacity());
          m_queries[q]->getSecondBuffer()->setupForCheckpoints(m_filesystem);
          m_checkpointInput[q][1] = true;
        }

        m_checkpoints[q][checkpoint]->setFilePath(relativePath);
      }
      m_checkpoints[q][checkpoint]->setCheckpointCounter(&m_checkpointCounter);
    }
    auto &code = m_queries[q]->getOperator()->getCode();
    m_intermSizes[q] = 0;
    if (auto *aggrCode = dynamic_cast<AggregateOperatorCode *>(&code)) {
      m_intermSizes[q] = aggrCode->getBucketSize();
    }
    m_outputSizes[q] = code.getOutputSchema().getTupleSize();
  }

  // topologically sort queries
  //size_t maxBatchSize = SystemConf::getInstance().BATCH_SIZE;
  std::vector<bool> visited (m_numOfQueries, false);
  std::stack<int> stack;
  for (int i = 0; i < m_numOfQueries; i++) {
    //if (m_queries[i]->getConfig())
    //  maxBatchSize = std::max(m_queries[i]->getConfig()->getBatchSize(), maxBatchSize);
    if (visited[i] == false) {
      topologicalSort(i, visited, stack);
    }
  }
  while (!stack.empty()) {
    m_sortedQueries.push_back(m_queries[stack.top()]);
    stack.pop();
  }
  std::reverse(m_sortedQueries.begin(), m_sortedQueries.end());

  // set the checkpoint coordinator only in the upstream task dispatcher
  // todo: generalize this for multiple pipelines/join operators
  // if (!m_triggersCheckpoints) {
  for (int q = 0; q < m_numOfQueries; ++q) {
    m_taskDispatchers[q]->m_triggerCheckpoints = false;
    m_taskDispatchers[q]->m_coordinator = this;
  }
  // m_taskDispatchers[0]->m_checkpointFinished.store(true);
  // m_taskDispatchers[0]->setCheckpointCoordinator(this);
  //}

  if (m_clearFiles && m_clearFiles->load()) {
    m_ready.store(true);
  }
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
void FileBackedCheckpointCoordinator::operator()() {
  if (m_clearFiles && !m_clearFiles->load()) {
    prepareRecovery();
    m_clearFiles->store(true);
    if (m_printMessages) {
      std::cout << "[CP] waiting for the input buffers to load their data" << std::endl;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1 * SystemConf::getInstance().CHECKPOINT_INTERVAL));
  }

  while (!m_ready.load())
    ;

  //std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  if (m_printMessages) {
    std::cout << "[CP] starting the checkpoint coordinator" << std::endl;
  }
  auto t1 = std::chrono::high_resolution_clock::now();
  auto t2 = t1;
  auto time_span =
      std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
  while (true) {
    try {
      if (m_triggersCheckpoints) {
        auto duration =
            std::max((int)(SystemConf::getInstance().CHECKPOINT_INTERVAL -
                (size_t) (time_span.count() * 1000)), 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(duration));
        // for (int q = 0; q < m_numOfQueries; ++q) {
        //  auto lastTaskId = m_taskDispatchers[q]->m_nextTask.load();
        //  m_taskDispatchers[q]->setLastTaskId(lastTaskId);
        //}
      } else {
        while (!m_waitCondition.load()) {
          // std::cout << "[CP] warning: the checkpoint is waiting for the "
          //             "condition to become true" << std::endl;
        }
        // reset the atomic for the next iteration
        m_waitCondition.store(false);
      }
      while (m_hasConcurrentCheckpoint.load()) {
        std::cout << "[CP] warning: the checkpoint is waiting for the "
                     "previous one to finish" << std::endl;
        _mm_pause();
      }
    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
    }

    //m_hasConcurrentCheckpoint.store(true);
    auto flag = false;
    while (!m_hasConcurrentCheckpoint.compare_exchange_weak(flag, true)) {
      flag = false;
    }
    t1 = std::chrono::high_resolution_clock::now();
    // first stop creating merging tasks
    if (m_printMessages) {
      std::cout << "[CP] disabling merge tasks for checkpoint preparation " << std::endl;
    }
    for (int q = 0; q < m_numOfQueries; ++q) {
      m_taskDispatchers[q]->createMergeTasks(false);
    }

    // trigger checkpoint
    // take the latest task-id from all pipelines
    // and block merge in checkpointed slots (result collector)
    while (!prepareCheckpoint()) {
      /*for (int q = 0; q < m_numOfQueries; ++q) {
        if (m_taskDispatchers[q]->m_workerQueue) {
          createMergeTask(q);
        }
      }*/
    }

    // first resume creating merging tasks
    if (m_printMessages) {
      std::cout << "[CP] enabling merge tasks for checkpoint preparation " << std::endl;
    }
    for (int q = 0; q < m_numOfQueries; ++q) {
      m_taskDispatchers[q]->createMergeTasks(true);
    }

    // start checkpointing
    // checkpointAll(); // single-threaded implementation used for testing

    auto checkpointVersion = m_checkpointId % 2;
    // wait until all checkpoints complete
    //for (int q = 0; q < m_numOfQueries;) {
    //  if (m_checkpointCounter.load() >= (q+1) && m_checkpoints[q][checkpointVersion]->m_readyLock.try_lock()){
    //    m_checkpoints[q][checkpointVersion]->m_readyLock.unlock();
    //    q++;
    //  } else {
    while (m_checkpointCounter.load(std::memory_order_acquire) != m_numOfQueries) {
      std::atomic_thread_fence(std::memory_order_acquire);
      // std::this_thread::yield();
      _mm_pause();
      m_filesystem->getHandler().tryCompleteMultiple();
      for (int q = 0; q < m_numOfQueries; ++q) {
        if (m_checkpoints[q][checkpointVersion]->m_state != CheckpointState::COMPLETED &&
            m_checkpoints[q][checkpointVersion]->m_workers.load() > 0) {
          //createCheckpointTask(q);
          //m_checkpoints[q][checkpointVersion]->m_workers.fetch_add(-1);
        }
      }
      // std::cout << "checkpointcounter " << m_checkpointCounter.load() <<
      // std::endl;
      // std::cout << "[DBG] number of checkpoints " <<
      // m_checkpoints[0][checkpointVersion]->m_counter.load() << std::endl;
    }

    // measure checkpoint size and acknowledge checkpoint
    size_t checkpointSize = 0;
    std::string metadata; // = std::to_string(m_checkpointId);
    for (int q = 0; q < m_numOfQueries; ++q) {
      // stop creating checkpointing tasks
      m_taskDispatchers[q]->m_triggerCheckpoints = false;
      //if (m_checkpointInput[q][0]) {}
      //if (m_checkpointInput[q][1]) {}
      auto sIdx = m_checkpointPtrs[q][0];
      auto eIdx = m_checkpointPtrs[q][1];
      auto b1SIdx = m_checkpointPtrs[q][4];
      auto b1EIdx = m_checkpointPtrs[q][5];
      auto b2SIdx = m_checkpointPtrs[q][6];
      auto b2EIdx = m_checkpointPtrs[q][7];
      auto task1 = m_checkpointPtrs[q][8];
      auto task2 = m_checkpointPtrs[q][9];
      metadata += " q " + std::to_string(q) + " " + std::to_string(b1SIdx) + " " + std::to_string(b1EIdx)
          + " " + std::to_string(b2SIdx) + " " + std::to_string(b2EIdx)
          + " " + std::to_string(sIdx) + " " + std::to_string(eIdx)
          + " " + std::to_string(task1) + " " + std::to_string(task2);
      auto numOfSlots = m_resultHandlers[q]->m_numberOfSlots;
      while (sIdx != eIdx) {
        if (m_resultHandlers[q]->m_hasWindowFragments) {
          while (m_resultHandlers[q]->m_results[sIdx].m_slot.load() == 5) {
            if (m_resultHandlers[q]->m_results[sIdx].m_previousSlot.load() == 5) {
              unsafePrint();
              throw std::runtime_error(
                  "error: invalid previous slot value after the end of the checkpoint for query " + std::to_string(q));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            if (m_printMessages) {
              std::cout << "[CP] warning: the checkpoint for query "
                  << std::to_string(q)
                  << " is waiting in the validation phase for slot " << sIdx
                  << " with value "
                  << m_resultHandlers[q]->m_results[sIdx].m_slot.load()
                  << ", previous value "
                  << m_resultHandlers[q]->m_results[sIdx].getPreviousSlot()
                  << std::endl;
            }
            // todo: do I need this?
            /*m_resultHandlers[q]->m_results[sIdx].m_slot.store(
                m_resultHandlers[q]->m_results[sIdx].getPreviousSlot(),
               std::memory_order_release);*/
          }
        } else {
          while (m_resultHandlers[q]->m_resultsWithoutFrags[sIdx].m_slot.load() == 5) {
            if (m_resultHandlers[q]->m_resultsWithoutFrags[sIdx].m_previousSlot.load() == 5) {
              unsafePrint();
              throw std::runtime_error(
                  "error: invalid previous slot value after the end of the checkpoint for query " + std::to_string(q));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            if (m_printMessages) {
              std::cout << "[CP] warning: the checkpoint for query "
                        << std::to_string(q) << " is waiting in the validation phase for slot "
                        << sIdx << " with value " << m_resultHandlers[q]->m_resultsWithoutFrags[sIdx].m_slot.load()
                        << ", previous value " << m_resultHandlers[q]->m_resultsWithoutFrags[sIdx].getPreviousSlot()
                        << std::endl;
            }
            // todo: do I need this?
            /*m_resultHandlers[q]->m_resultsWithoutFrags[sIdx].m_slot.store(
                m_resultHandlers[q]->m_resultsWithoutFrags[sIdx].getPreviousSlot(),
               std::memory_order_release);*/
          }
        }
        sIdx++;
        if (sIdx == numOfSlots) sIdx = 0;
      }
      // m_resultHandlers[q]->m_mergeLock.unlock();
      // create a merge task
      if (m_taskDispatchers[q]->m_workerQueue) {
        createMergeTask(q);
      }
      // if (m_readySlots[q][0].unsafe_size() != 0)
      //  throw std::runtime_error("error: the checkpoint queue is not empty for
      //  query " + std::to_string(q));
      // finalize checkpoint
      m_taskDispatchers[q]->m_checkpointFinished.store(true);
      checkpointSize +=
          m_checkpoints[q][checkpointVersion]->getCheckpointSize();
    }
    persistMetadata(metadata);
    m_root->m_version.get_rw().store(m_checkpointId);
    m_root->m_valid.get_rw().store(true);

    if (SystemConf::getInstance().LINEAGE_ON) {
      // todo: make this more general
      for (int q = 0; q < m_numOfQueries; ++q) {
        auto query = m_sortedQueries[q];
        long fo1 = m_checkpointPtrs[q][10];
        long fo2 = m_checkpointPtrs[q][11];
        if (query->getBuffer()->isPersistent() && fo1 != INT_MIN) {
          query->getBuffer()->getFileStore()->freePersistent(query->getId(), 0, fo1);
        }
        if (query->getSecondBuffer()->isPersistent() && fo2 != INT_MIN) {
          query->getSecondBuffer()->getFileStore()->freePersistent(query->getId(), 1, fo2);
        }
      }
    }

    t2 = std::chrono::high_resolution_clock::now();
    time_span =
        std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);

    m_statistics->registerDuration(time_span.count());
    m_statistics->registerCheckpointSize(checkpointSize);
    if (m_printMessages) {
      std::cout << "[CP] checkpoint duration " << time_span.count()
                << " with size " << checkpointSize << " "
                << m_statistics->toString() << std::endl;
    }

    if (checkpointSize < (m_expectedBytes) &&
        checkpointSize != m_measuredBytes.load()) {
      std::cout << "warning: the checkpoint size ("
                << std::to_string(checkpointSize)
                << ") was smaller than the expected "
                << std::to_string(m_expectedBytes) << " and measured "
                << std::to_string(m_measuredBytes.load()) << std::endl;
      /*throw std::runtime_error(
          "error: the checkpoint size (" + std::to_string(checkpointSize) +
          ") was smaller than the expected " + std::to_string(m_expectedBytes) +
          " and measured " + std::to_string(m_measuredBytes.load()));*/
    }

    // reset checkpoints
    m_hasConcurrentCheckpoint.store(false);
    m_checkpointCounter.store(0);
    m_expectedBytes = 0;
    m_measuredBytes.store(0);

    if (SystemConf::getInstance().DURATION > 0) {
      // this works assuming there is no stalling for triggering checkpoints
      if ((long)((m_checkpointId * SystemConf::getInstance().CHECKPOINT_INTERVAL) / 1000) >
          SystemConf::getInstance().DURATION) {
        // delete checkpoint files in we reach the end of the experiment's
        // duration
        clearPersistentMemory();
        if (m_printMessages) {
          std::cout << "[CP] Done." << std::endl;
        }
        break;
      }
    }
    m_checkpointId++;
    //if (m_checkpointId == 2)
    //  throw std::runtime_error("failing after checkpoint");
  }
}

void FileBackedCheckpointCoordinator::checkpoint(int pid, int q) {
  if (m_checkpointInput[q][0]) {
    checkpointBuffer(pid, 0, q);
  }
  if (m_checkpointInput[q][1]) {
    checkpointBuffer(pid, 1, q);
  }
  if (m_resultHandlers[q]->m_hasWindowFragments) {
    checkpointWithFragments(pid, q);
  } else {
    checkpointWithoutFragments(pid, q);
  }
}

void FileBackedCheckpointCoordinator::recover(int pid, int q) {
  if (m_checkpointInput[q][0]) {
    recoverBuffer(pid, 0, q);
  }
  if (m_checkpointInput[q][1]) {
    recoverBuffer(pid, 1, q);
  }
  if (m_resultHandlers[q]->m_hasWindowFragments) {
    recoverWithFragments(pid, q);
  } else {
    recoverWithoutFragments(pid, q);
  }
}

int FileBackedCheckpointCoordinator::roundOffset(int offset) {
  /*auto alignment = m_filesystem->getSectorSize();
  if (offset % alignment != 0) {
    int d = offset / alignment;
    offset = (d + 1) * alignment;
  }
  return offset;*/
  auto alignment = m_filesystem->getSectorSize();
  if (offset < 8 * 1024 && offset != 0) {
    offset = 8 * 1024;
  } else if (offset % alignment != 0) {
    auto d = offset / alignment;
    offset = (d + 1) * alignment;
  }
  return offset;
}

bool FileBackedCheckpointCoordinator::prepareCheckpoint() {
  if (m_debug) {
    std::cout << "[CP] preparing checkpoint " << m_checkpointId << std::endl;
  }
  auto t1 = std::chrono::high_resolution_clock::now();

  bool flag = false;
  // prepare checkpoints using the topological sorted queries
  for (auto &query: m_sortedQueries) {
    auto q = query->getId();
    auto checkpointVersion = m_checkpointId % 2;
    auto &slotsWithoutFrags = m_resultHandlers[q]->m_resultsWithoutFrags;
    auto &slots = m_resultHandlers[q]->m_results;
    m_checkpoints[q][checkpointVersion]->resetCheckpoint();
    m_checkpoints[q][checkpointVersion]->setCheckpointId(m_checkpointId, q);
    m_checkpointPtrs[q][0] = -1;
    m_checkpointPtrs[q][1] = -1;
    m_checkpointPtrs[q][10] = INT_MIN;
    m_checkpointPtrs[q][11] = INT_MIN;

    // Lock the forwarding section of the result handler for the preparation
    // Do I need to keep the lock for the whole checkpoint duration?
    if (query->isMarkedForCheckpoint()) {
      if (m_printMessages) {
        std::cout << "[CP] entering preparation for query "
                  << std::to_string(q) + "..." << std::endl;
      }
      m_resultHandlers[q]->m_stopMerging = true;
      std::scoped_lock<std::mutex, std::mutex> lock(
          m_resultHandlers[q]->m_mergeLock, m_resultHandlers[q]->m_forwardLock);
      auto idx = m_resultHandlers[q]->m_nextToForward.load();
      //std::cout << "[CP] entered preparation..." << std::endl;
      if (m_lastTaskId[q] == m_resultHandlers[q]->m_maxTaskId.load()) {
        std::cout << "[CP] warning: checkpointing has to wait for processing "
                  << "... increase the size of the checkpoint duration or "
                  << "the size of the input queue (last task: "
                  << m_lastTaskId[q] << ")" << std::endl;
        m_resultHandlers[q]->m_stopMerging = false;
        createMergeTask(q);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        // unsafePrint();
        // flag = true;
        // break;
      } else {
        m_checkpointPtrs[q][0] = idx;
        m_checkpointPtrs[q][1] = idx;
        if (m_resultHandlers[q]->m_hasWindowFragments) {
          while (slots[idx].m_slot.load() != -1 &&
                 slots[idx].m_slot.load() != 1 &&
                 slots[idx].m_slot.load() != 3) {
            std::cout << "[CP] warning: the checkpoint for query "
                      << std::to_string(q) << " is waiting for the "
                      << "first slot to checkpoint with id "
                      << slots[idx].m_taskId << " and slot "
                      << slots[idx].m_slot.load() << std::endl;
            if (slots[idx].m_slot.load() == 5) {
              unsafePrint();
              throw std::runtime_error(
                  "error: wrong state type at the beginning of the checkpoint "
                  "preparation for query " +
                  std::to_string(q));
            }
          }
        } else {
          while (slotsWithoutFrags[idx].m_slot.load() != -1 &&
                 slotsWithoutFrags[idx].m_slot.load() != 1) {
            std::cout << "[CP] warning: the checkpoint for query "
                      << std::to_string(q)
                      << " is waiting for the first slot to checkpoint with id "
                      << slotsWithoutFrags[idx].m_taskId << " and slot "
                      << slots[idx].m_slot.load() << std::endl;
            if (slotsWithoutFrags[idx].m_slot.load() == 5) {
              unsafePrint();
              throw std::runtime_error(
                  "error: wrong state type at the beginning of the checkpoint "
                  "preparation for query " +
                  std::to_string(q));
            }
          }
        }
        auto firstTaskId = m_resultHandlers[q]->m_hasWindowFragments
                               ? slots[idx].m_taskId.load()
                               : slotsWithoutFrags[idx].m_taskId.load();
        auto numOfSlots = m_resultHandlers[q]->m_numberOfSlots;
        // get the last task id after obtaining the locks
        int tryCnt = 0;
        long cnt = 0;
        //while (cnt < numOfSlots) {
        m_lastTaskId[q] = m_resultHandlers[q]->m_maxTaskId.load();

        auto prevIdx = (idx - 1) < 0 ? numOfSlots-1 : (idx - 1);
        auto prevTaskId = m_resultHandlers[q]->m_hasWindowFragments
                           ? slots[prevIdx].m_taskId.load()
                           : slotsWithoutFrags[prevIdx].m_taskId.load();
        if (prevTaskId >= m_lastTaskId[q]) {
          cnt = 0;
        } else {
          if (prevTaskId < 0)
            prevTaskId = 0;
          cnt = m_lastTaskId[q] - prevTaskId; //firstTaskId + 1;
          while (cnt > numOfSlots) {
            //firstTaskId = m_resultHandlers[q]->m_hasWindowFragments
            //                  ? slots[idx].m_taskId.load()
            //                  : slotsWithoutFrags[idx].m_taskId.load();
            cnt = m_lastTaskId[q] - prevTaskId; //firstTaskId + 1;
          }
        }

        // mark input queues for checkpoint
        if (m_checkpointInput[q][0]) {
          auto freeIdx = (idx + cnt - 1) % numOfSlots;
          if (freeIdx < 0)
            freeIdx = 0;
          auto freePtr = m_resultHandlers[q]->m_hasWindowFragments
                         ? slots[freeIdx].m_freePointer
                         : slotsWithoutFrags[freeIdx].m_freePointer1;
          if (freePtr < 0) {
            freePtr = m_resultHandlers[q]->m_hasWindowFragments
                           ? -1 : slotsWithoutFrags[freeIdx].m_prevFreePointer1;
            if (freePtr < 0)
              throw std::runtime_error("error: negative freePtr1");
          }
          auto slotsToCheck = query->getBuffer()->prepareCheckpoint(freePtr, m_readySlots[q][1], m_checkpointPtrs[q][4], m_checkpointPtrs[q][5]);
          m_checkpoints[q][checkpointVersion]->increaseSlots(slotsToCheck, slotsToCheck);
          m_expectedBytes += slotsToCheck * query->getBuffer()->getSlots()[0].m_size;
        }
        if (m_checkpointInput[q][1]) {
          auto freeIdx = (idx + cnt - 1) % numOfSlots;
          if (freeIdx < 0)
            freeIdx = 0;
          auto freePtr = slotsWithoutFrags[freeIdx].m_freePointer2;
          if (freePtr < 0) {
            freePtr = slotsWithoutFrags[freeIdx].m_prevFreePointer2;
            if (freePtr < 0)
              throw std::runtime_error("error: negative freePtr2");
          }
          auto slotsToCheck = query->getSecondBuffer()->prepareCheckpoint(freePtr, m_readySlots[q][2], m_checkpointPtrs[q][6], m_checkpointPtrs[q][7]);
          m_checkpoints[q][checkpointVersion]->increaseSlots(slotsToCheck, slotsToCheck);
          m_expectedBytes += slotsToCheck * query->getBuffer()->getSlots()[0].m_size;
        }
        // std::this_thread::sleep_for(std::chrono::milliseconds(1));
        // if (tryCnt++ == 0) break;
        //}

        // todo: control the size of the checkpoint here
        // cnt -= ...

        if (cnt < 0 || cnt > numOfSlots) {
          unsafePrint();
          throw std::runtime_error(
              "error: invalid number of measured slots to checkpoint (" +
              std::to_string(cnt) + " - " + std::to_string(numOfSlots) +
              ") last task id " +
              std::to_string(m_lastTaskId[q]) + " first task id " +
              std::to_string(firstTaskId));
        }
        int workThreshold = ((int) cnt / SystemConf::getInstance().WORKER_THREADS) + 1;

        m_checkpointPtrs[q][8] = firstTaskId;
        m_checkpointPtrs[q][9] = m_lastTaskId[q];
        if (cnt > 0) {
          // std::cout << "[CP] the number of slots is " << cnt << std::endl;
          std::unordered_set<int> idxs;  // used for testing correctness
          if (m_resultHandlers[q]->m_hasWindowFragments) {
            while (true) {
              if (cnt == 0) break;
              auto &slot = slots[idx];

              auto waitCnt = 0;
              // spin while waiting for the slots to become available
              while (slot.m_slot.load() != 1 && slot.m_slot.load() != 3) {
                if (waitCnt++ == 1000000) {
                  std::cout << "[CP] warning: the checkpoint for query "
                            << std::to_string(q)
                            << " is waiting in the preparation phase for slot "
                            << idx << " with value " << slots[idx].m_slot.load()
                            << " and taskId " << slots[idx].m_taskId
                            << std::endl;
                  waitCnt = 0;
                  if (slots[idx].m_previousSlot.load() == 5) {
                    unsafePrint();
                    throw std::runtime_error(
                        "error: wrong state type at the beginning of the "
                        "checkpoint "
                        "preparation for query " +
                        std::to_string(q));
                  }
                }
                _mm_pause();
              }

              int winFrags = slot.getNumberOfWindowFragments(true);
              if (winFrags > 0) {
                slot.setPreviousSlot(slot.m_slot.load());  // keep previous state
                m_checkpoints[q][checkpointVersion]->increaseSlots(slot.getNumberOfWindowFragments(true));
                auto *code = dynamic_cast<AggregateOperatorCode *>(&query->getOperator()->getCode());
                if (!code)
                  throw std::runtime_error(
                      "error: invalid aggregation casting in checkpoint "
                      "coordinator");
                auto hashtableSize =
                    code->hasGroupBy()
                        ? (query->getConfig()
                               ? query->getConfig()->getHashtableSize()
                               : SystemConf::getInstance().HASH_TABLE_SIZE)
                        : 1;
                m_expectedBytes +=
                    ((slot.m_numberOfWindows - slot.m_numberOfCompleteWindows) *
                     hashtableSize * m_intermSizes[q]) +
                    (slot.m_numberOfCompleteWindows * hashtableSize *
                     m_outputSizes[q]);

                slot.m_slot.store(5, std::memory_order_release);
                // std::cout << "[DBG] setting slot " +
                // std::to_string(slot.m_index) + " to 3" << std::endl;

                // fill the checkpoint queue
                m_readySlots[q][0].push(idx);
                if (idxs.find(idx) != idxs.end())
                  throw std::runtime_error(
                      "error: the idx already exists during the checkpoint "
                      "preparation");
                else
                  idxs.insert(idx);
              }

              cnt--;
              // std::cout << "[CP] now the number of slots is " << cnt << std::endl;
              idx++;
              if (idx == numOfSlots) idx = 0;
            }
          } else {
            while (true) {
              if (cnt == 0) break;
              auto &slot = slotsWithoutFrags[idx];

              // spin while waiting for the slots to become available
              while (slot.m_slot.load() != 1) {
                auto waitCnt = 0;
                if (waitCnt++ == 1000000) {
                  std::cout << "[CP] warning: the checkpoint for query "
                            << std::to_string(q)
                            << " is waiting in the preparation phase for slot "
                            << idx << " with value "
                            << slotsWithoutFrags[idx].m_slot.load()
                            << " and taskId " << slotsWithoutFrags[idx].m_taskId
                            << std::endl;
                  waitCnt = 0;
                  if (slotsWithoutFrags[idx].m_previousSlot.load() == 5) {
                    unsafePrint();
                    throw std::runtime_error(
                        "error: wrong state type at the beginning of the "
                        "checkpoint "
                        "preparation for query " +
                        std::to_string(q));
                  }
                }
                _mm_pause();
              }
              int byteLength = slot.m_result->getPosition();
              if (byteLength > 0) {
                slot.setPreviousSlot(slot.m_slot.load());  // keep previous state
                m_checkpoints[q][checkpointVersion]->increaseSlots(slot.getNumberOfResults());
                m_expectedBytes += byteLength;

                slot.m_slot.store(5, std::memory_order_release);
                // std::cout << "[DBG] setting slot " + std::to_string(slot.m_index) + " to 3" << std::endl;

                // fill the checkpoint queue
                m_readySlots[q][0].push(idx);
                if (idxs.find(idx) != idxs.end())
                  throw std::runtime_error(
                      "error: the idx already exists during the checkpoint preparation");
                else
                  idxs.insert(idx);
              }

              cnt--;
              // std::cout << "[CP] now the number of slots is " << cnt << std::endl;
              idx++;
              if (idx == numOfSlots) idx = 0;
            }
          }

          idx--;
          if (idx == -1) idx = numOfSlots - 1;
          m_checkpointPtrs[q][1] = idx;
          // get the input buffer offset here
          m_checkpointPtrs[q][2] =
              m_resultHandlers[q]->m_hasWindowFragments
                  ? slots[m_checkpointPtrs[q][1]].m_freePointer
                  : slotsWithoutFrags[m_checkpointPtrs[q][1]].m_freePointer1;
          m_checkpointPtrs[q][3] =
              m_resultHandlers[q]->m_hasWindowFragments
                  ? 0
                  : slotsWithoutFrags[m_checkpointPtrs[q][1]].m_freePointer2;

          if (SystemConf::getInstance().LINEAGE_ON) {
            m_checkpointPtrs[q][10] =
                m_resultHandlers[q]->m_hasWindowFragments
                    ? slots[m_checkpointPtrs[q][1]].m_freeOffset
                    : slotsWithoutFrags[m_checkpointPtrs[q][1]].m_freeOffset1;
            m_checkpointPtrs[q][11] =
                m_resultHandlers[q]->m_hasWindowFragments
                    ? 0
                    : slotsWithoutFrags[m_checkpointPtrs[q][1]].m_freeOffset2;
            if (query->isMostDownstream()) {
              if (m_resultHandlers[q]->m_hasWindowFragments) {
                //slots[m_checkpointPtrs[q][1]].
              } else {

              }
            }
          }
          if (m_checkpointPtrs[q][0] < 0 ||
              m_checkpointPtrs[q][0] >= numOfSlots ||
              m_checkpointPtrs[q][1] < 0 ||
              m_checkpointPtrs[q][1] >= numOfSlots) {
            throw std::runtime_error("error: invalid checkpoint pointers: " +
                                     std::to_string(m_checkpointPtrs[q][0]) +
                                     " - " +
                                     std::to_string(m_checkpointPtrs[q][1]));
          }
        }

        m_resultHandlers[q]->m_stopMerging = false;
        // unlock the merge phase
        // m_resultHandlers[q]->m_mergeLock.unlock();
        // m_resultHandlers[q]->m_forwardLock.unlock();
      }
      // start creating checkpoint tasks
      // m_taskDispatchers[q]->m_triggerCheckpoints = true;
    }

    //if (m_checkpoints[q][checkpointVersion]->getSlots() == 0) {
    //  m_checkpoints[q][checkpointVersion]->setComplete();
    //} else {
    //  // create tasks for checkpointing
    //  for (int i = 0; i < SystemConf::getInstance().WORKER_THREADS; ++i) {
    //    createCheckpointTask(q);
    //  }
    //  // m_checkpoints[q][checkpointVersion]->m_readyFlag.store(true);
    //}

    m_statistics->registerSize(m_expectedBytes);
    if (m_printMessages) {
      std::cout << "[CP] checkpoint " << m_checkpointId << " for pipeline " << q
                << " with last task id " << m_lastTaskId[q] << " has to store "
                << m_checkpoints[q][checkpointVersion]->getSlots() << " (q "
                << m_checkpoints[q][checkpointVersion]->getInputQueueSlots()
                << ")" << " fragments with expected size " << m_expectedBytes
                << std::endl;
    }
  }

  for (auto &query: m_sortedQueries) {
    auto q = query->getId();
    auto checkpointVersion = m_checkpointId % 2;
    if (m_checkpoints[q][checkpointVersion]->getSlots() == 0) {
      m_checkpoints[q][checkpointVersion]->setComplete();
    } else {
      // create tasks for checkpointing
      for (int i = 0; i < SystemConf::getInstance().WORKER_THREADS; ++i) {
        createCheckpointTask(q);
      }
      // m_checkpoints[q][checkpointVersion]->m_readyFlag.store(true);
    }
  }

  if (flag) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    return false;
  }

  auto t2 = std::chrono::high_resolution_clock::now();
  auto time_span =
      std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
  m_statistics->registerPreparation(time_span.count());

  return true;
}

bool FileBackedCheckpointCoordinator::prepareRecovery() {
  m_checkpointId = m_root->m_version.get_ro().load();
  if (!m_root->m_valid.get_ro().load()) {
    std::cout << "[CP] found invalid checkpoint during recovery" << std::endl;
    return false;
  }

  auto t1 = std::chrono::high_resolution_clock::now();
  auto currentMs = std::chrono::duration_cast<std::chrono::milliseconds>(t1.time_since_epoch()).count();
  std::cout << "[CP] " << currentMs << " start recovering last checkpoint" << std::endl;

  // parse metadata
  std::string metadata;
  for (size_t idx = 0; idx < m_root->m_mSize; ++idx) {
    metadata += m_root->m_metadata[idx];
  }
  std::istringstream iss(metadata);
  std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                 std::istream_iterator<std::string>{}};

  for (size_t idx = 0; idx < words.size(); idx+=10) {
    auto q = std::stoi(words[idx + 1]);
    m_checkpointPtrs[q][0] = std::stoi(words[idx + 6]);
    m_checkpointPtrs[q][1] = std::stoi(words[idx + 7]);
    m_checkpointPtrs[q][4] = std::stoi(words[idx + 2]);
    m_checkpointPtrs[q][5] = std::stoi(words[idx + 3]);
    m_checkpointPtrs[q][6] = std::stoi(words[idx + 4]);
    m_checkpointPtrs[q][7] = std::stoi(words[idx + 5]);
    m_checkpointPtrs[q][8] = std::stoi(words[idx + 8]);
    m_checkpointPtrs[q][9] = std::stoi(words[idx + 9]);

    auto buffer = m_queries[q]->getBuffer();
    auto lastTask = (buffer->isPersistent()) ? (int)(buffer->getUnsafeStartPointer()/buffer->getBatchSize()) + 1 : 0;
    if (lastTask <= m_checkpointPtrs[q][9]) {
      auto numOfSlots = m_resultHandlers[q]->m_numberOfSlots;
      if (m_checkpointPtrs[q][8] < lastTask) {
        auto diff = lastTask - m_checkpointPtrs[q][8];
        m_checkpointPtrs[q][8] = m_checkpointPtrs[q][8] + diff;
        m_checkpointPtrs[q][0] = (m_checkpointPtrs[q][0] + diff) % numOfSlots;
      }
      // update unsafe start pointer
      auto diff = m_checkpointPtrs[q][9] - lastTask + 1;
      buffer->incrementUnsafeStartPointer(diff * m_queries[q]->getBuffer()->getBatchSize());

      std::cout << "[CP] start recovering from checkpoint of query " << q
                << " with starting task " << m_checkpointPtrs[q][8] << " and"
                << " last task " << m_checkpointPtrs[q][9] << std::endl;

      // enlist available slots
      auto startSlot = m_checkpointPtrs[q][0];
      auto endSlot = m_checkpointPtrs[q][1];
      if (endSlot != -1) {
        std::cout << "[DBG] For query " + std::to_string(q) +
                         " dropping tasks before task " +
                         std::to_string(m_checkpointPtrs[q][9])
                  << std::endl;
        m_queries[q]->startDroppingTasks(m_checkpointPtrs[q][9]);
        m_resultHandlers[q]->restorePtrs(m_checkpointPtrs[q][8]);
        endSlot = (endSlot + 1) % numOfSlots;
      }
      while (startSlot != -1 && endSlot != -1 && startSlot != endSlot) {
        m_readySlots[q][0].push(startSlot);
        m_recoveries[q]->increaseSlots(1, 0);
        startSlot++;
        if (startSlot == numOfSlots) {
          startSlot = 0;
        }
      }
      numOfSlots = m_queries[q]->getBuffer()->getNumberOfSlots();
      startSlot = m_checkpointPtrs[q][4];
      endSlot = m_checkpointPtrs[q][5];
      if (endSlot != -1) {
        endSlot = (endSlot + 1) % numOfSlots;
      }
      while (startSlot != -1 && endSlot != -1 && startSlot != endSlot) {
        m_readySlots[q][1].push(startSlot);
        m_recoveries[q]->increaseSlots(1, 1);
        startSlot++;
        if (startSlot == numOfSlots) {
          startSlot = 0;
        }
      }
      numOfSlots = m_queries[q]->getSecondBuffer()->getNumberOfSlots();
      startSlot = m_checkpointPtrs[q][6];
      endSlot = m_checkpointPtrs[q][7];
      if (endSlot != -1) {
        endSlot = (endSlot + 1) % numOfSlots;
      }
      while (startSlot != -1 && endSlot != -1 && startSlot != endSlot) {
        m_readySlots[q][2].push(startSlot);
        m_recoveries[q]->increaseSlots(1, 1);
        startSlot++;
        if (startSlot == numOfSlots) {
          startSlot = 0;
        }
      }

      if (m_recoveries[q]->getSlots() > 0) {
        // create recovery tasks
        for (int i = 0; i < SystemConf::getInstance().WORKER_THREADS; ++i) {
          createRecoveryTask(q);
        }
      } else {
        m_recoveries[q]->setComplete();
      }
    } else {
      std::cout << "[CP] the last task id from the checkpoint of query " << q
          << " was smaller than the last id kept in the input buffer ("
                << lastTask << " > " << m_checkpointPtrs[q][9] << ")" << std::endl;
      m_recoveries[q]->setComplete();
    }
  }

  // wait for recovery to finish
  while (m_recoveryCounter.load() != m_numOfQueries) {
    // std::cout << "recoveryCounter " << m_recoveryCounter.load() << std::endl;
  }

  //m_checkpointId = 0;
  auto t2 = std::chrono::high_resolution_clock::now();
  auto time_span =
      std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
  std::cout << "[CP] finishing the recovery of checkpoints in " << time_span.count() << std::endl;
  return true;
}

void FileBackedCheckpointCoordinator::setReady() {
  std::cout << "[CP] setting checkpoint coordinator ready to start" << std::endl;
  m_ready.store(true);
}

void FileBackedCheckpointCoordinator::setCompressionFP(int query, std::function<void(int, char *, int, int, char *, int &, bool, bool &)> fp) {
 if (query >= m_numOfQueries)
   throw std::runtime_error("error: invalid number of query when setting the compression function");
 m_useCompression[query] = true;
 m_compressionFP[query] = fp;
}

void FileBackedCheckpointCoordinator::setDecompressionFP(int query, std::function<void(int, char *, int, int, char *, int &, bool, bool &)> fp) {
  if (query >= m_numOfQueries)
    throw std::runtime_error("error: invalid number of query when setting the compression function");
  m_decompressionFP[query] = fp;
}

void FileBackedCheckpointCoordinator::signalWaitCondition() {
  m_waitCondition.store(true);
}

bool FileBackedCheckpointCoordinator::hasWorkUnsafe(int query) {
  return (m_readySlots[query][0].unsafe_size() + m_readySlots[query][1].unsafe_size() + m_readySlots[query][2].unsafe_size())> 0;
}

void FileBackedCheckpointCoordinator::tryToPurgeCheckpoint(std::shared_ptr<LineageGraph> &graph) {
  if (!SystemConf::getInstance().LINEAGE_ON || !m_root || !m_root->m_valid.get_ro() || !graph)
    return;
  auto flag = false;
  while (!m_hasConcurrentCheckpoint.compare_exchange_weak(flag, true)) {
    flag = false;
  }
  bool purge = true;
  for (int q = 0; q < m_numOfQueries; ++q) {
    if ((graph->m_graph[q]->m_freeOffset1 < m_checkpointPtrs[q][10]) ||
        (m_queries[q]->getSecondSchema() &&
            graph->m_graph[q]->m_freeOffset2 < m_checkpointPtrs[q][11])) {
      purge = false;
      break;
    }
  }
  if (purge) {
    // std::cout << "[DBG] invalidating the latest checkpoint" << std::endl;
    m_root->m_valid.get_rw() = false;
  }
  m_hasConcurrentCheckpoint.store(false);
}

void FileBackedCheckpointCoordinator::clearPersistentMemory() {
  m_pop.close();
  m_filesystem->eraseFiles();
  m_filesystem->tryDeleteCheckpointDirectory(m_guid);
}

FileBackedCheckpointCoordinator::~FileBackedCheckpointCoordinator() {
  m_pop.close();
}

void FileBackedCheckpointCoordinator::checkpointBuffer(int pid, int bId, int q) {
  auto callback = [](IAsyncContext *ctxt, Status result,
                     size_t bytes_transferred) {
    CallbackContext<AckCheckpointContext> context{ctxt};
    if (result != Status::Ok) {
      fprintf(stderr, "AsyncFlushPages(), error: %u\n",
              static_cast<uint8_t>(result));
    }
    auto debug = false;
    if (debug) {
      std::cout << "[DBG] callback updating slot " +
          std::to_string(context->m_slotId) + " with " +
          std::to_string(context->m_slot->load()) + " status, " +
          std::to_string(bytes_transferred) + " bytes_transferred and " +
          std::to_string(context->m_frCounter->load()) + " frCounter " << std::endl;
    }
    // Set the slot status to ready
    context->m_frCounter->fetch_add(-1);
    if (context->m_frCounter->load() == 0) {
      context->m_slot->store(context->m_previousState);
    }
    if (debug) {
      std::cout << "[DBG] callback setting the slot " +
          std::to_string(context->m_slotId) + " status to " +
          std::to_string(context->m_slot->load()) +
          " with previous slot " +
          std::to_string(context->m_previousState) +
          " and frCounter " +
          std::to_string(context->m_frCounter->load())
                << std::endl;
    }
    if (context->m_buffer) {
      UnboundedQueryBufferFactory::getInstance().free(context->m_buffer->getBufferId(), context->m_buffer);
      context->m_buffer.reset();
    }
    context->m_checkpoint->updateCounter(bytes_transferred);
  };

  // try to acknowledge previous request
  m_filesystem->getHandler().tryCompleteMultiple();
  auto checkpointVersion = m_checkpointId % 2;

  int slotId = -1;
  auto buffer = (bId == 0) ? m_queries[q]->getBuffer() :
                           m_queries[q]->getSecondBuffer();
  while (m_readySlots[q][bId + 1].try_pop(slotId)) {
    auto &slot = buffer->getSlots()[slotId];
#if defined(PREFETCH)
    // prefetch data here
    slot.prefetch();
#endif
    auto copyBuffer = UnboundedQueryBufferFactory::getInstance().newInstance(pid);
    int diskBytes = 0;
    char *diskValues = copyBuffer->getBuffer().data();
    bool clear = false;
    std::function<void(int, char *, int, int, char *, int &, int, bool &, long)> fp;
    auto bytes = slot.m_size;
    if (buffer->hasCompression(fp)) {
      fp(pid, slot.m_bufferPtr, 0, (int) bytes, copyBuffer->getBuffer().data(), diskBytes, (int) copyBuffer->getBuffer().size(), clear, -1);
      auto latency = (SystemConf::getInstance().LATENCY_ON) ? 0 : -1;
      fp(pid, slot.m_bufferPtr, 0, -1, copyBuffer->getBuffer().data(), diskBytes, (int) copyBuffer->getBuffer().size(), clear, latency);
      //diskBytes = 64 * 1024;//bytes;
    } else {
      std::memcpy(copyBuffer->getBuffer().data(), slot.m_bufferPtr, bytes);
      diskBytes = bytes;
    }
    diskBytes = roundOffset(Utils::getPowerOfTwo(diskBytes));
#if defined(NO_DISK)
    diskBytes = 0;
#endif

    if (m_debug) {
      std::cout << "[DBG] Worker compressing data in query " << std::to_string(q)
                << " for slot " << std::to_string(slotId) << " in buffer " << std::to_string(bId)
                << " of " << std::to_string(bytes) << " bytes to "
                << std::to_string(diskBytes) << " with "
                << std::to_string((double)bytes/(double)diskBytes) << " ratio " << std::endl;
    }

    if (diskBytes > (int) m_slotFileSize)
      throw std::runtime_error("error: the write exceeds the size of slots in the input log");
    m_measuredBytes.fetch_add(diskBytes);

    AckCheckpointContext context{
        m_checkpoints[q][checkpointVersion].get(), slot.m_id,
        &slot.m_slot, slot.getPreviousSlot(),
        &slot.m_numberOfResults, &slot.m_updateLock, copyBuffer};
    assert(m_asyncFiles[q][checkpointVersion + 2 + bId]->writeAsync(
               reinterpret_cast<const uint8_t *>(diskValues),
               slotId * m_slotFileSize, diskBytes, callback,
               context) == Status::Ok);

    slot.setReady();

    // check if some async calls have finished
    m_filesystem->getHandler().tryCompleteMultiple();
  }
}

void FileBackedCheckpointCoordinator::checkpointWithFragments(int pid, int q) {
  auto callback = [](IAsyncContext *ctxt, Status result,
                     size_t bytes_transferred) {
    CallbackContext<AckCheckpointContext> context{ctxt};
    if (result != Status::Ok) {
      fprintf(stderr, "AsyncFlushPages(), error: %u\n",
              static_cast<uint8_t>(result));
    }
    auto debug = false;
    if (debug) {
      std::cout << "[DBG] callback updating slot " +
          std::to_string(context->m_slotId) + " with " +
          std::to_string(bytes_transferred) + " bytes_transferred"
                << std::endl;
    }
    // Set the slot status to ready
    context->m_frCounter->fetch_add(-1);
    if (context->m_frCounter->load() == 0) {
      if (context->m_slot->load() == 5) {
        //const std::lock_guard<std::mutex> lock(*context->m_lock);
        auto oldVal = context->m_slot->load();
        if (context->m_previousState != 1 && context->m_previousState != 3) {
          throw std::runtime_error(
              "error: in the callback the previous slot value is " +
                  std::to_string(context->m_previousState));
        }
        context->m_slot->store(context->m_previousState);
        // if (!context->m_slot->compare_exchange_weak(oldVal,
        //                                            context->m_previousState))
        //                                            {
        if (context->m_slot->load() != 1 && context->m_slot->load() != 3) {
          throw std::runtime_error(
              "error: failed updating the result slot after checkpointing: " +
                  std::to_string(oldVal));
        }
      } else {
        throw std::runtime_error(
            "error: failed updating the result slot because of invalid slot "
            "value: " +
                std::to_string(context->m_slot->load()));
      }
    }
    if (debug) {
      std::cout << "[DBG] callback setting the slot " +
          std::to_string(context->m_slotId) + " status to " +
          std::to_string(context->m_slot->load()) +
          " with previous slot " +
          std::to_string(context->m_previousState) +
          " and frCounter " +
          std::to_string(context->m_frCounter->load())
                << std::endl;
    }
    if (context->m_buffer) {
      UnboundedQueryBufferFactory::getInstance().free(context->m_buffer->getBufferId(), context->m_buffer);
      context->m_buffer.reset();
    }
    context->m_checkpoint->updateCounter(bytes_transferred);
  };

  // try to acknowledge previous request
  m_filesystem->getHandler().tryCompleteMultiple();
  auto checkpointVersion = m_checkpointId % 2;
  // todo: create 4 different checkpoint functions (simple, agg, aggPtr,
  // aggPar). The one bellow is only for the aggPtr case
  auto &slots = m_resultHandlers[q]->m_results;
  auto numOfSlots = m_resultHandlers[q]->m_numberOfSlots;
  auto cnt = 0;
  int idx = -1;
  bool clear = true;
  while (m_readySlots[q][0].try_pop(idx)) {
    auto &slot = slots[idx];
    if (slot.m_slot.load(std::memory_order_acquire) == 5) {
      if (m_debug) {
        std::cout << "[DBG] creating callback for slot " +
            std::to_string(slot.m_index) +
            " with previous status " +
            std::to_string(slot.getPreviousSlot())
                  << std::endl;
      }
#if defined(PREFETCH)
      // prefetch data here
      slot.prefetch();
#endif
      auto buffer = UnboundedQueryBufferFactory::getInstance().newInstance(pid);
      auto capacity = buffer->getBuffer().size();
      std::string metadata;  // metadata stored at the beginning of the slot
      auto writeIdx = 6 * 1024;
      auto query = m_queries[q];
      auto *code = dynamic_cast<AggregateOperatorCode *>(&query->getOperator()->getCode());
      if (!code)
        throw std::runtime_error("error: invalid aggregation casting in checkpoint coordinator");
      auto hashtableSize = code->hasGroupBy() ? (query->getConfig() ? query->getConfig()->getHashtableSize()
                                                                    : SystemConf::getInstance().HASH_TABLE_SIZE) : 1;
      auto offset = hashtableSize * m_intermSizes[q];
      metadata.append(std::to_string(slot.m_freePointer) + " ")
          .append(std::to_string(slot.m_latencyMark) + " ")
          .append(std::to_string(slot.m_taskId) + " ")
          .append(std::to_string(slot.m_previousSlot.load()) + " ")
          .append(std::to_string(hashtableSize) + " ")
          .append(std::to_string(m_intermSizes[q]) + " ")
          .append(std::to_string(offset) + " ");
      // check all the fragments of the slot
      auto copyIdx = 0;
      if (slot.m_closingWindows) {
        metadata.append("cl "+
            std::to_string(slot.m_closingWindows->numberOfWindows()) + " ");
        if (code->hasGroupBy()) {
          for (int wIdx = 0; wIdx < slot.m_closingWindows->numberOfWindows(); ++wIdx) {
            checkBlockSize(copyIdx + offset + writeIdx, capacity);
            if (!m_useCompression[q]) {
              std::memcpy(buffer->getBuffer().data() + copyIdx + writeIdx,
                          slot.m_closingWindows->getBufferPtrs()[wIdx], offset);
              copyIdx += offset;
            } else {
              m_compressionFP[q](
                  pid, slot.m_closingWindows->getBufferPtrs()[wIdx], 0, offset,
                  buffer->getBuffer().data() + writeIdx, copyIdx, false, clear);
              metadata.append(std::to_string(copyIdx) + " ");
            }
          }
        } else if (slot.m_closingWindows->numberOfWindows() > 0) {
          offset = slot.m_closingWindows->getPosition();
          checkBlockSize(offset + writeIdx, capacity);
          if (!m_useCompression[q]) {
            std::memcpy(buffer->getBuffer().data() + copyIdx + writeIdx,
                        slot.m_closingWindows->getBufferRaw(), offset);
            copyIdx += offset;
          } else {
            m_compressionFP[q](
                pid, slot.m_closingWindows->getBufferRaw(), 0, offset,
                buffer->getBuffer().data() + writeIdx, copyIdx, false, clear);
            metadata.append(std::to_string(copyIdx) + " ");
          }
        }
      }
      if (slot.m_pendingWindows) {
        metadata.append("p " +
            std::to_string(slot.m_pendingWindows->numberOfWindows()) + " ");
        if (code->hasGroupBy()) {
          for (int wIdx = 0; wIdx < slot.m_pendingWindows->numberOfWindows(); ++wIdx) {
            checkBlockSize(copyIdx + offset + writeIdx, capacity);
            if (!m_useCompression[q]) {
              std::memcpy(buffer->getBuffer().data() + copyIdx + writeIdx,
                          slot.m_pendingWindows->getBufferPtrs()[wIdx], offset);
              copyIdx += offset;
            } else {
              m_compressionFP[q](
                  pid, slot.m_pendingWindows->getBufferPtrs()[wIdx], 0, offset,
                  buffer->getBuffer().data() + writeIdx, copyIdx, false, clear);
              metadata.append(std::to_string(copyIdx) + " ");
            }
          }
        } else if (slot.m_pendingWindows->numberOfWindows() > 0) {
          offset = slot.m_pendingWindows->getPosition();
          checkBlockSize(copyIdx + offset + writeIdx, capacity);
          if (!m_useCompression[q]) {
            std::memcpy(buffer->getBuffer().data() + copyIdx + writeIdx,
                        slot.m_pendingWindows->getBufferRaw(), offset);
            copyIdx += offset;
          } else {
            m_compressionFP[q](
                pid, slot.m_pendingWindows->getBufferRaw(), 0, offset,
                buffer->getBuffer().data() + writeIdx, copyIdx, false, clear);
            metadata.append(std::to_string(copyIdx) + " ");
          }
        }
      }
      if (slot.m_openingWindows) {
        metadata.append("o " +
            std::to_string(slot.m_openingWindows->numberOfWindows()) + " ");
        if (code->hasGroupBy()) {
          for (int wIdx = 0; wIdx < slot.m_openingWindows->numberOfWindows(); ++wIdx) {
            checkBlockSize(copyIdx + offset + writeIdx, capacity);
            if (!m_useCompression[q]) {
              std::memcpy(buffer->getBuffer().data() + copyIdx + writeIdx,
                          slot.m_openingWindows->getBufferPtrs()[wIdx], offset);
              copyIdx += offset;
            } else {
              m_compressionFP[q](
                  pid, slot.m_openingWindows->getBufferPtrs()[wIdx], 0, offset,
                  buffer->getBuffer().data() + writeIdx, copyIdx, false, clear);
              metadata.append(std::to_string(copyIdx) + " ");
            }
          }
        } else if (slot.m_openingWindows->numberOfWindows() > 0) {
          offset = slot.m_openingWindows->getPosition();
          checkBlockSize(copyIdx + offset + writeIdx, capacity);
          if (!m_useCompression[q]) {
            std::memcpy(buffer->getBuffer().data() + copyIdx + writeIdx,
                        slot.m_openingWindows->getBufferRaw(), offset);
            copyIdx += offset;
          } else {
            m_compressionFP[q](
                pid, slot.m_openingWindows->getBufferRaw(), 0, offset,
                buffer->getBuffer().data() + writeIdx, copyIdx, false, clear);
            metadata.append(std::to_string(copyIdx) + " ");
          }
        }
      }
      if (slot.m_completeWindows &&
          slot.m_completeWindows->numberOfWindows() > 0) {
        offset = slot.m_completeWindows->getPosition();
        metadata.append("co " + std::to_string(offset) + " ");
        checkBlockSize(copyIdx + offset + writeIdx, capacity);
        if (!m_useCompression[q]) {
          std::memcpy(buffer->getBuffer().data() + copyIdx + writeIdx,
                      slot.m_completeWindows->getBuffer().data(), offset);
          copyIdx += offset;
        } else {
          metadata.append(std::to_string(copyIdx) + " ");
          m_compressionFP[q](
              pid, slot.m_completeWindows->getBuffer().data(), 0, offset,
              buffer->getBuffer().data() + writeIdx, copyIdx, true, clear);
          metadata.append(std::to_string(copyIdx) + " ");
        }
      }
      if (m_useCompression[q] && copyIdx > 0) {
        m_compressionFP[q](
            pid, buffer->getBuffer().data() + writeIdx, 0, -1,
            buffer->getBuffer().data() + writeIdx, copyIdx, false, clear);
      }
      if ((int) metadata.size() > writeIdx)
        throw std::runtime_error("error: increase the metadata section (" +
            std::to_string(metadata.size()) + " - " +
            std::to_string(writeIdx) + ")");
      std::memcpy(buffer->getBuffer().data(), metadata.data(), metadata.size());
      if (copyIdx >= 0) {
        copyIdx = roundOffset(Utils::getPowerOfTwo(copyIdx + writeIdx)); //roundOffset(copyIdx + writeIdx);
#if defined(NO_DISK)
        copyIdx = 0;
#endif
        checkBlockSize(copyIdx, capacity);
        m_measuredBytes.fetch_add(copyIdx);

        AckCheckpointContext context{
            m_checkpoints[q][checkpointVersion].get(), slot.m_index,
            &slot.m_slot, slot.getPreviousSlot(),
            &slot.m_numberOfFragments, &slot.m_updateLock, buffer};
        assert(m_asyncFiles[q][checkpointVersion]->writeAsync(
            reinterpret_cast<const uint8_t *>(buffer->getBuffer().data()),
            slot.m_index * m_slotFileSize, copyIdx, callback,
            context) == Status::Ok);
        if (m_debug) {
          std::cout << "[DBG] submiting callback for slot " +
              std::to_string(slot.m_index)
                    << std::endl;
        }
        m_filesystem->getHandler().tryCompleteMultiple();
      }
    } else {
      throw std::runtime_error(
          "error: attempting to checkpoint slot " + std::to_string(idx) +
              " with state " + std::to_string(slot.m_slot.load(std::memory_order_acquire)) +
              " and checkpoint pointers: " +
              std::to_string(m_checkpointPtrs[q][0]) + " - " +
              std::to_string(m_checkpointPtrs[q][1]));
    }
  }
  // std::cout << "[CP] worker leaving checkpoint function " << std::endl;
}

void FileBackedCheckpointCoordinator::checkpointWithoutFragments(int pid, int q) {
  auto callback = [](IAsyncContext *ctxt, Status result,
                     size_t bytes_transferred) {
    CallbackContext<AckCheckpointContext> context{ctxt};
    if (result != Status::Ok) {
      fprintf(stderr, "AsyncFlushPages(), error: %u\n",
              static_cast<uint8_t>(result));
    }
    auto debug = false;
    if (debug) {
      std::cout << "[DBG] callback updating slot " +
          std::to_string(context->m_slotId) + " with " +
          std::to_string(bytes_transferred) + " bytes_transferred"
                << std::endl;
    }
    // Set the slot status to ready
    context->m_frCounter->fetch_add(-1);
    if (context->m_frCounter->load() == 0) {
      if (context->m_slot->load() == 5) {
        //const std::lock_guard<std::mutex> lock(*context->m_lock);
        auto oldVal = context->m_slot->load();
        if (context->m_previousState != 1) {
          throw std::runtime_error(
              "error: in the callback the previous slot value is " +
                  std::to_string(context->m_previousState));
        }
        context->m_slot->store(context->m_previousState);
        // if (!context->m_slot->compare_exchange_weak(oldVal,
        //                                            context->m_previousState))
        //                                            {
        if (context->m_slot->load() != 1) {
          throw std::runtime_error(
              "error: failed updating the result slot after checkpointing: " +
                  std::to_string(oldVal));
        }
      } else {
        throw std::runtime_error(
            "error: failed updating the result slot because of invalid slot "
            "value: " +
                std::to_string(context->m_slot->load()));
      }
    }
    if (debug) {
      std::cout << "[DBG] callback setting the slot " +
          std::to_string(context->m_slotId) + " status to " +
          std::to_string(context->m_slot->load()) +
          " with previous slot " +
          std::to_string(context->m_previousState) +
          " and frCounter " +
          std::to_string(context->m_frCounter->load())
                << std::endl;
    }
    if (context->m_buffer) {
      UnboundedQueryBufferFactory::getInstance().free(context->m_buffer->getBufferId(), context->m_buffer);
      context->m_buffer.reset();
    }
    context->m_checkpoint->updateCounter(bytes_transferred);
  };

  // try to acknowledge previous request
  m_filesystem->getHandler().tryCompleteMultiple();
  auto checkpointVersion = m_checkpointId % 2;
  auto &slots = m_resultHandlers[q]->m_resultsWithoutFrags;
  auto numOfSlots = m_resultHandlers[q]->m_numberOfSlots;
  auto cnt = 0;
  int idx = -1;
  bool clear = true;
  while (m_readySlots[q][0].try_pop(idx)) {
    auto &slot = slots[idx];
    if (slot.m_slot.load(std::memory_order_acquire) == 5) {
      if (m_debug) {
        std::cout << "[DBG] creating callback for slot " +
            std::to_string(slot.m_index) +
            " with previous status " +
            std::to_string(slot.getPreviousSlot())
                  << std::endl;
      }
#if defined(PREFETCH)
      // prefetch data here
      slot.prefetch();
#endif
      auto buffer = UnboundedQueryBufferFactory::getInstance().newInstance(pid);
      auto capacity = buffer->getBuffer().size();
      std::string metadata;  // metadata stored at the beginning of the slot
      auto writeIdx = 512;
      auto query = m_queries[q];

      auto offset = slot.m_result->getPosition();
      metadata.append(std::to_string(slot.m_freePointer1) + " ")
          .append(std::to_string(slot.m_freePointer2) + " ")
          .append(std::to_string(slot.m_prevFreePointer1) + " ")
          .append(std::to_string(slot.m_prevFreePointer2) + " ")
          .append(std::to_string(slot.m_latencyMark) + " ")
          .append(std::to_string(slot.m_taskId) + " ")
          .append(std::to_string(offset) + " ");

      // copy result
      auto copyIdx = 0;
      checkBlockSize(copyIdx + offset + writeIdx, capacity);
      if (!m_useCompression[q]) {
        std::memcpy(buffer->getBuffer().data() + copyIdx + writeIdx,
                    slot.m_result->getBufferRaw(), offset);
        copyIdx += offset;
      } else {
        m_compressionFP[q](
            pid, slot.m_result->getBufferRaw(), 0, offset,
            buffer->getBuffer().data() + writeIdx, copyIdx, false, clear);
        metadata.append(std::to_string(copyIdx) + " ");
      }

      if (m_useCompression[q] && copyIdx > 0) {
        m_compressionFP[q](
            pid, buffer->getBuffer().data() + writeIdx, 0, -1,
            buffer->getBuffer().data() + writeIdx, copyIdx, false, clear);
      }
      if ((int) metadata.size() > writeIdx)
        throw std::runtime_error("error: increase the metadata section (" +
            std::to_string(metadata.size()) + " - " +
            std::to_string(writeIdx) + ")");
      std::memcpy(buffer->getBuffer().data(), metadata.data(), metadata.size());
      if (copyIdx >= 0) {
        copyIdx = roundOffset(Utils::getPowerOfTwo(copyIdx + writeIdx)); //roundOffset(copyIdx + writeIdx);
#if defined(NO_DISK)
        copyIdx = 0;
#endif
        checkBlockSize(copyIdx, capacity);
        m_measuredBytes.fetch_add(copyIdx);

        AckCheckpointContext context{
            m_checkpoints[q][checkpointVersion].get(), slot.m_index,
            &slot.m_slot, slot.getPreviousSlot(),
            &slot.m_numberOfResults, &slot.m_updateLock, buffer};
        assert(m_asyncFiles[q][checkpointVersion]->writeAsync(
            reinterpret_cast<const uint8_t *>(buffer->getBuffer().data()),
            slot.m_index * m_slotFileSize, copyIdx, callback,
            context) == Status::Ok);
        if (m_debug) {
          std::cout << "[DBG] submiting callback for slot " +
              std::to_string(slot.m_index)
                    << std::endl;
        }
        m_filesystem->getHandler().tryCompleteMultiple();
      }
    } else {
      throw std::runtime_error(
          "error: attempting to checkpoint slot " + std::to_string(idx) +
              " with state " + std::to_string(slot.m_slot.load(std::memory_order_acquire)) +
              " and checkpoint pointers: " +
              std::to_string(m_checkpointPtrs[q][0]) + " - " +
              std::to_string(m_checkpointPtrs[q][1]));
    }
  }
  // std::cout << "[CP] worker leaving checkpoint function " << std::endl;
}

void FileBackedCheckpointCoordinator::recoverBuffer(int pid, int bId, int q) {
  throw std::runtime_error("error: not implemented yet.");
}

void FileBackedCheckpointCoordinator::recoverWithFragments(int pid, int q) {
  auto checkpointVersion = m_checkpointId % 2;
  auto &slots = m_resultHandlers[q]->m_results;
  auto numOfSlots = m_resultHandlers[q]->m_numberOfSlots;
  auto cnt = 0;
  int idx = -1;
  bool clear = true;

  auto buffer = UnboundedQueryBufferFactory::getInstance().newInstance(pid);
  while (m_readySlots[q][0].try_pop(idx)) {
    auto &slot = slots[idx];

    if (m_debug) {
      std::cout << "[DBG] restoring slot " + std::to_string(idx) +
          " for query " + std::to_string(q) << std::endl;
    }

    auto capacity = buffer->getBuffer().size();
    auto writeIdx = 6 * 1024;
    std::string metadata (writeIdx, '\0');  // metadata stored at the beginning of the slot
    auto query = m_queries[q];
    auto *code = dynamic_cast<AggregateOperatorCode *>(&query->getOperator()->getCode());
    if (!code)
      throw std::runtime_error("error: invalid aggregation casting in checkpoint coordinator");
    auto hashtableSize = code->hasGroupBy() ? (query->getConfig() ? query->getConfig()->getHashtableSize()
                                                                  : SystemConf::getInstance().HASH_TABLE_SIZE) : 1;
    auto offset = hashtableSize * m_intermSizes[q];

    auto fptr = m_asyncFiles[q][checkpointVersion];
    assert(fptr->readSync(slot.m_index * m_slotFileSize, buffer->getBuffer().data(), SystemConf::getInstance().BLOCK_SIZE) == Status::Ok);

    std::memcpy(metadata.data(), buffer->getBuffer().data(), writeIdx);
    std::istringstream iss(metadata);
    std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                   std::istream_iterator<std::string>{}};
    slot.m_slot.store(0);
    slot.m_freePointer = INT_MIN;//std::stol(words[0]);
    slot.m_latencyMark = std::stol(words[1]);
    slot.m_taskId = std::stoi(words[2]);
    auto hashTableSize = std::stoi(words[4]);
    auto intermSize = std::stoi(words[5]);
    auto storedOffset = std::stoi(words[6]);
    auto hashTableStart = (m_useCompression[q]) ? std::stoi(words[words.size()-2]) : 0;
    size_t wIdx = 7;
    auto fileOffset = writeIdx;

    // initialize window fragments
    slot.m_closingWindows = (!code->hasGroupBy()) ? PartialWindowResultsFactory::getInstance().newInstance(pid) :
                            PartialWindowResultsFactory::getInstance().newInstance(pid, storedOffset);
    slot.m_pendingWindows = (!code->hasGroupBy()) ? PartialWindowResultsFactory::getInstance().newInstance(pid) :
                            PartialWindowResultsFactory::getInstance().newInstance(pid, storedOffset);
    slot.m_openingWindows = (!code->hasGroupBy()) ? PartialWindowResultsFactory::getInstance().newInstance(pid) :
                            PartialWindowResultsFactory::getInstance().newInstance(pid, storedOffset);
    slot.m_completeWindows = PartialWindowResultsFactory::getInstance().newInstance(pid);

    slot.m_graph = LineageGraphFactory::getInstance().newInstance();

    int partialWindows = 0;
    bool flag = false;
    while (wIdx < words.size()) {
      if (words[wIdx] == "cl") {
        auto numOfWindows = std::stoi(words[wIdx+1]);
        partialWindows += numOfWindows;
        slot.m_closingWindows->incrementCount(numOfWindows);
        if (code->hasGroupBy()) {
          auto startPtrs = slot.m_closingWindows->getStartPointers().data();
          auto prevStartPos = 0;
          for (auto w = 0; w < numOfWindows; ++w) {
            int pos;
            if (!m_useCompression[q]) {
              pos = storedOffset;
              startPtrs[w] = w * hashTableSize;
              std::memcpy(slot.m_closingWindows->getBufferPtrs()[w], buffer->getBuffer().data() + fileOffset, pos);
            } else {
              auto endPos = std::stoi(words[wIdx+2+w]);
              startPtrs[w] = w * hashTableSize;
              if (prevStartPos < endPos)
                m_decompressionFP[q](pid, buffer->getBuffer().data() + writeIdx, prevStartPos, endPos, slot.m_closingWindows->getBufferPtrs()[w],
                                   hashTableStart, false, flag);
              prevStartPos = endPos;
            }
            fileOffset += pos;
          }
          wIdx += (m_useCompression[q]) ? numOfWindows : 0;
        } else {
          // setPosition
          throw std::runtime_error("error: not implemented yet");
        }
        wIdx += 1;
      }
      else if (words[wIdx] == "p") {
        auto numOfWindows = std::stoi(words[wIdx+1]);
        partialWindows += numOfWindows;
        slot.m_pendingWindows->incrementCount(numOfWindows);
        if (code->hasGroupBy()) {
          auto startPtrs = slot.m_pendingWindows->getStartPointers().data();
          auto prevStartPos = 0;
          for (auto w = 0; w < numOfWindows; ++w) {
            int pos;
            if (!m_useCompression[q]) {
              pos = storedOffset;
              startPtrs[w] = w * hashTableSize;
              std::memcpy(slot.m_pendingWindows->getBufferPtrs()[w], buffer->getBuffer().data() + fileOffset, pos);
            } else {
              auto endPos = std::stoi(words[wIdx+2+w]);
              startPtrs[w] = w * hashTableSize;
              if (prevStartPos < endPos)
                m_decompressionFP[q](pid, buffer->getBuffer().data() + writeIdx, prevStartPos, endPos, slot.m_pendingWindows->getBufferPtrs()[w],
                                   hashTableStart, false, flag);
              prevStartPos = endPos;
            }
            fileOffset += pos;
          }
          wIdx += (m_useCompression[q]) ? numOfWindows : 0;
        } else {
          throw std::runtime_error("error: not implemented yet");
        }
        wIdx += 1;
      }
      else if (words[wIdx] == "o") {
        auto numOfWindows = std::stoi(words[wIdx+1]);
        partialWindows += numOfWindows;
        slot.m_openingWindows->incrementCount(numOfWindows);
        if (code->hasGroupBy()) {
          auto startPtrs = slot.m_openingWindows->getStartPointers().data();
          auto prevStartPos = 0;
          for (auto w = 0; w < numOfWindows; ++w) {
            int pos;
            if (!m_useCompression[q]) {
              pos = storedOffset;
              startPtrs[w] = w * hashTableSize;
              std::memcpy(slot.m_openingWindows->getBufferPtrs()[w], buffer->getBuffer().data() + fileOffset, pos);
            } else {
              auto endPos = std::stoi(words[wIdx+2+w]);
              startPtrs[w] = w * hashTableSize;
              if (prevStartPos < endPos)
                m_decompressionFP[q](pid, buffer->getBuffer().data() + writeIdx, prevStartPos, endPos, slot.m_openingWindows->getBufferPtrs()[w],
                                   hashTableStart, false, flag);
              prevStartPos = endPos;
            }
            fileOffset += pos;
          }
          wIdx += (m_useCompression[q]) ? numOfWindows : 0;
        } else {
          throw std::runtime_error("error: not implemented yet");
        }
        wIdx += 1;
      }
      else if (words[wIdx] == "co") {
        slot.m_completeWindows->incrementCount(1);
        auto pos = std::stoi(words[wIdx+1]);
        slot.m_completeWindows->setPosition(pos);
        auto startPtrs = slot.m_completeWindows->getStartPointers().data();
        startPtrs[0] = 0;
        startPtrs[1] = pos;
        if (!m_useCompression[q]) {
          std::memcpy(slot.m_completeWindows->getBuffer().data(), buffer->getBuffer().data() + fileOffset, pos);
        } else {
          auto startPos = std::stoi(words[wIdx+2]);
          auto endPos = std::stoi(words[wIdx+3]);
          wIdx += 1;
          if (startPos < endPos)
            m_decompressionFP[q](pid, buffer->getBuffer().data() + writeIdx, startPos, endPos, slot.m_completeWindows->getBuffer().data(),
                               hashTableStart, true, flag);
        }
        fileOffset += pos;
        wIdx += (m_useCompression[q]) ? 2 : 0;
        wIdx += 1;
      }

      wIdx++;
    }
    if (partialWindows > 0) {
      slot.m_slot.store(1);
    } else {
      slot.m_slot.store(3);
    }
    m_recoveries[q]->updateCounter(1);

    if (m_debug || true) {
      std::cout << "[DBG] worker finishing recovering slot " << idx
                << " with " << slot.m_taskId << " for query "
                << q << std::endl;
    }
  }
  UnboundedQueryBufferFactory::getInstance().free(buffer);
  // std::cout << "[CP] worker leaving checkpoint function " << std::endl;
}

void FileBackedCheckpointCoordinator::recoverWithoutFragments(int pid, int q) {
  throw std::runtime_error("error: not implemented yet.");
  // std::cout << "[CP] worker leaving checkpoint function " << std::endl;
}

void FileBackedCheckpointCoordinator::persistGuid() {
  auto str = m_guid.ToString();
  auto *arr = str.c_str();
  auto size = str.size();
  pmem::obj::transaction::run(m_pop, [&] {
    if (m_root->m_guidSize > 0)
      pmem::obj::delete_persistent<char[]>(m_root->m_guid, m_root->m_guidSize);

    pmem::obj::persistent_ptr<char[]> new_array =
        pmem::obj::make_persistent<char[]>(size);

    for (size_t i = 0; i < size; i++) new_array[i] = arr[i];

    m_root->m_guidSize = (size_t)size;
    m_root->m_guid = new_array;
  });
}

void FileBackedCheckpointCoordinator::persistMetadata(std::string &metadata) {
  auto str = metadata;
  auto *arr = str.c_str();
  auto size = str.size();
  pmem::obj::transaction::run(m_pop, [&] {
    if (m_root->m_mSize > 0)
      pmem::obj::delete_persistent<char[]>(m_root->m_metadata, m_root->m_mSize);

    pmem::obj::persistent_ptr<char[]> new_array =
        pmem::obj::make_persistent<char[]>(size);

    for (size_t i = 0; i < size; i++) new_array[i] = arr[i];

    m_root->m_mSize = (size_t)size;
    m_root->m_metadata = new_array;
  });
}

void FileBackedCheckpointCoordinator::unsafePrint() {
  std::string str;
  for (int q = 0; q < m_numOfQueries; ++q) {
    auto checkpointVersion = m_checkpointId % 2;
    str.append("query " + std::to_string(q) + " nextToForward " +
               std::to_string(m_resultHandlers[q]->m_nextToForward));
    str.append(" nextToAggregate " +
               std::to_string(m_resultHandlers[q]->m_nextToAggregate) + "\n");
    int idx = 0;
    if (m_resultHandlers[q]->m_hasWindowFragments) {
      auto &slots = m_resultHandlers[q]->m_results;
      for (auto &slot : slots) {
        str.append(std::to_string(idx) + ": slot " +
                   std::to_string(slot.m_slot.load()) + " taskId " +
                   std::to_string(slot.m_taskId) + " windowFrags " +
                   std::to_string(slot.getNumberOfWindowFragments(true)) +
                   "\n");
        idx++;
      }
    } else {
      auto &slots = m_resultHandlers[q]->m_resultsWithoutFrags;
      for (auto &slot : slots) {
        str.append(std::to_string(idx) + ": slot " +
            std::to_string(slot.m_slot.load()) + " taskId " +
            std::to_string(slot.m_taskId) + "\n");
        idx++;
      }
    }
  }
  std::cout << "[CP] warning: \n" << str << std::endl;
}

void FileBackedCheckpointCoordinator::createMergeTask(int query) {
  if (m_taskDispatchers[query]->m_workerQueue->size_approx() < m_queries[query]->getTaskQueueCapacity()) {
    auto batch = WindowBatchFactory::getInstance().newInstance(
        0, 0, -1, -1, m_queries[query].get(), nullptr,
        &m_queries[query]->getWindowDefinition(), m_queries[query]->getSchema(),
        -1);
    batch->setTaskType(TaskType::MERGE_FORWARD);
    auto task = TaskFactory::getInstance().newInstance(0, batch, nullptr,
                                                       TaskType::MERGE_FORWARD);
    if (!m_taskDispatchers[query]->m_workerQueue->try_enqueue(task)) {
      std::cout << "warning: waiting to enqueue MERGE_FORWARD task in the "
                << "checkpoint coordinator with size "
                << std::to_string(
                       m_taskDispatchers[query]->m_workerQueue->size_approx())
                << std::endl;
      WindowBatchFactory::getInstance().free(batch);
      TaskFactory::getInstance().free(task);
    }
  }
}

void FileBackedCheckpointCoordinator::createCheckpointTask(int query) {
  if (m_taskDispatchers[query]->m_workerQueue->size_approx() < m_queries[query]->getTaskQueueCapacity()) {
    auto batch = WindowBatchFactory::getInstance().newInstance(
        0, 0, -1, -1, m_queries[query].get(), nullptr,
        &m_queries[query]->getWindowDefinition(), m_queries[query]->getSchema(),
        -1);
    batch->setTaskType(TaskType::CHECKPOINT);
    auto task = TaskFactory::getInstance().newInstance(0, batch, nullptr,
                                                       TaskType::CHECKPOINT);
    if (!m_taskDispatchers[query]->m_workerQueue->try_enqueue(task)) {
      std::cout << "warning: waiting to enqueue CHECKPOINT task in the "
                << "checkpoint coordinator with size "
                << std::to_string(
                       m_taskDispatchers[query]->m_workerQueue->size_approx())
                << std::endl;
      WindowBatchFactory::getInstance().free(batch);
      TaskFactory::getInstance().free(task);
    }
  }
}

void FileBackedCheckpointCoordinator::createRecoveryTask(int query) {
  if (m_taskDispatchers[query]->m_workerQueue->size_approx() < m_queries[query]->getTaskQueueCapacity()) {
    auto batch = WindowBatchFactory::getInstance().newInstance(
        0, 0, -1, -1, m_queries[query].get(), nullptr,
        &m_queries[query]->getWindowDefinition(), m_queries[query]->getSchema(),
        -1);
    batch->setTaskType(TaskType::RECOVER);
    auto task = TaskFactory::getInstance().newInstance(0, batch, nullptr,
                                                       TaskType::RECOVER);
    if (!m_taskDispatchers[query]->m_workerQueue->try_enqueue(task)) {
      std::cout << "warning: waiting to enqueue RECOVER task in the "
                << "checkpoint coordinator with size "
                << std::to_string(
                    m_taskDispatchers[query]->m_workerQueue->size_approx())
                << std::endl;
      WindowBatchFactory::getInstance().free(batch);
      TaskFactory::getInstance().free(task);
    }
  }
}

void FileBackedCheckpointCoordinator::checkBlockSize(size_t size, size_t capacity) {
  if (size > capacity)
    throw std::runtime_error(
        "error: the write exceeds the size of slots in the "
        "checkpoint stage: " +
        std::to_string(size) + " > " + std::to_string(capacity));
}

void FileBackedCheckpointCoordinator::topologicalSort(int q, std::vector<bool> &visited, std::stack<int> &stack) {
  visited[q] = true;
  for (int i = 0; i < m_queries[q]->getNumberOfUpstreamQueries(); i++) {
    auto qId = m_queries[q]->getUpstreamQuery(i)->getId();
    if (!visited[qId]) {
      topologicalSort(qId, visited, stack);
    }
  }
  stack.push(q);
}