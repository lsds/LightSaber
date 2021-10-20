#pragma once

#include <tbb/concurrent_queue.h>

#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr_base.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <stack>

#include "utils/Guid.h"
#include "utils/SystemConf.h"

class Checkpoint;
class Recovery;
class Query;
struct PartialResultSlot;
class ResultHandler;
class ITaskDispatcher;
class ResultHandler;
struct FileOptions;
class QueueIoHandler;
template <class H>
class FileSystemDisk;
template <class H>
class FileSystemFile;
class CheckpointStatistics;
struct LineageGraph;

/*
 * \brief The checkpoint coordinator coordinates the snapshots of operators and
 * state. It triggers the checkpoint by sending the messages to the relevant
 * operators and collects the checkpoint acknowledgements. It also collects and
 * maintains the overview of the state handles reported by the tasks that
 * acknowledge the checkpoint.
 *
 * */

class FileBackedCheckpointCoordinator {
 private:
  long m_jobId;
  bool m_triggersCheckpoints;
  std::atomic<bool> m_waitCondition;
  Guid m_guid;
  size_t m_checkpointId;
  std::atomic<int> m_checkpointCounter;
  std::atomic<int> m_recoveryCounter;
  const int m_numOfQueries;
  const std::vector<std::shared_ptr<Query>> &m_queries;
  std::vector<std::shared_ptr<Query>> m_sortedQueries;
  const int m_numberOfCheckpoints = 2;  // use double buffering for checkpoints
  std::vector<std::vector<std::shared_ptr<Checkpoint>>> m_checkpoints;
  std::vector<std::shared_ptr<Recovery>> m_recoveries;
  std::vector<std::vector<bool>> m_checkpointInput;
  std::vector<int> m_lastTaskId;
  std::vector<std::vector<int>> m_checkpointPtrs; // the last slot hold the input buffer offset
  std::vector<std::shared_ptr<ITaskDispatcher>> m_taskDispatchers;
  std::vector<std::shared_ptr<ResultHandler>> m_resultHandlers;
  std::atomic<bool> m_hasConcurrentCheckpoint;  // allow only a single checkpoint to happen

  std::vector<int> m_intermSizes;
  std::vector<int> m_outputSizes;
  size_t m_expectedBytes;
  std::atomic<size_t> m_measuredBytes;

  std::unique_ptr<CheckpointStatistics> m_statistics;

  // variables used for compression
  std::vector<bool> m_useCompression;
  std::vector<std::function<void(int, char *, int, int, char *, int &, bool, bool &)>> m_compressionFP;
  std::vector<std::function<void(int, char *, int, int, char *, int &, bool, bool &)>> m_decompressionFP;

  // slots for checkpointing
  std::vector<std::vector<tbb::concurrent_queue<int>>> m_readySlots;

  typedef QueueIoHandler adapter_t;
  typedef FileSystemDisk<adapter_t> disk_t;
  typedef FileSystemFile<adapter_t> file_t;

  std::shared_ptr<disk_t> m_filesystem;

  // Variables for persisting metadata
  struct PMem;
  const size_t m_poolSize;
  const std::string m_layout = "";
  pmem::obj::pool<PMem> m_pop;
  pmem::obj::persistent_ptr<PMem> m_root;
  std::string m_pmFileName;
  file_t *m_pmFile;

  // Variables for persisting asynchronously the actual data
  size_t m_slotFileSize;
  std::vector<std::vector<std::string>> m_asyncFileNames;
  std::vector<std::vector<file_t *>> m_asyncFiles;
  std::unique_ptr<FileOptions> m_asyncFileOptions;

  std::atomic<bool> *m_clearFiles;
  std::atomic<bool> m_ready;
  const bool m_checkpointInputQueues = true;
  const bool m_debug = false;
  const bool m_printMessages = true;

 public:
  FileBackedCheckpointCoordinator(
      long jobId, const std::vector<std::shared_ptr<Query>> &queries, std::atomic<bool> *clearFiles = nullptr,
      std::shared_ptr<disk_t> filesystem = nullptr, bool triggersCheckpoints = false);

  void operator()();

  void checkpoint(int pid, int query);

  void recover(int pid, int query);

  int roundOffset(int offset);

  bool prepareCheckpoint();

  bool prepareRecovery();

  void setReady();

  void setCompressionFP(int query, std::function<void(int, char *, int, int, char *, int &, bool, bool &)> fp);

  void setDecompressionFP(int query, std::function<void(int, char *, int, int, char *, int &, bool, bool &)> fp);

  void signalWaitCondition();

  bool hasWorkUnsafe(int query);

  void clearPersistentMemory();

  void tryToPurgeCheckpoint(std::shared_ptr<LineageGraph> &graph);

  ~FileBackedCheckpointCoordinator();

 private:
  /*
   * \brief This is used to store metadata for the checkpoints.
   * A checkpoint is valid if every pipeline has checkpointed
   * successfully.
   *
   * */
  struct PMem {
    pmem::obj::p<std::atomic<size_t>> m_version;
    pmem::obj::persistent_ptr<char[]> m_guid;
    pmem::obj::p<size_t> m_guidSize;
    pmem::obj::persistent_ptr<char[]> m_metadata;
    pmem::obj::p<size_t> m_mSize;
    pmem::obj::p<std::atomic<bool>> m_valid;
    pmem::obj::persistent_ptr<PMem> next;
    PMem() {
      m_version.get_rw() = 0L;
      m_guidSize.get_rw() = 0;
      m_mSize.get_rw() = 0;
      m_valid.get_rw() = false;
    };
    /** Copy constructor is deleted */
    PMem(const PMem &) = delete;
    /** Assignment operator is deleted */
    PMem &operator=(const PMem &) = delete;
  };

  void checkpointBuffer(int pid, int bufferId, int query);

  void checkpointWithFragments(int pid, int query);

  void checkpointWithoutFragments(int pid, int query);

  void recoverBuffer(int pid, int bufferId, int query);

  void recoverWithFragments(int pid, int query);

  void recoverWithoutFragments(int pid, int query);

  void persistGuid();

  void persistMetadata(std::string &metadata);

  void unsafePrint();

  void createMergeTask(int query);

  void createCheckpointTask(int query);

  void createRecoveryTask(int query);

  void checkBlockSize(size_t size, size_t capacity);

  void topologicalSort(int q, std::vector<bool> &visited, std::stack<int> &stack);
};