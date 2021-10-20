#pragma once

#include <atomic>

/*
 * \brief This class describes a checkpoint, which is considered as completed
 * if all required tasks have acknowledged it. It contains all the metadata of
 * a checkpoint, while handles to the state are stored in the
 * FileBackedCheckpointCoordinator
 *
 * */

enum CheckpointState : uint8_t { COMPLETED, PENDING };

// todo: should we resume pending checkpoints??
class Checkpoint {
 private:
  long m_checkpointId;
  int m_pipelineId;
  long m_triggerTimestamp;
  std::atomic<int> m_numberOfSlots;
  std::atomic<int> m_inputQueueSlots;
  std::mutex m_completionLock;
  std::atomic<int> m_counter;
  std::atomic<int> m_workers;
  long m_lastTaskId;
  std::atomic<int> *m_checkpointCounter;
  CheckpointState m_state;
  std::atomic<size_t> m_checkpointSize;
  std::atomic<size_t> m_checkpointDuration;
  std::string m_filePath;
  //std::atomic<bool> m_readyFlag;

  friend class FileBackedCheckpointCoordinator;

 public:
  explicit Checkpoint(long id = -1, long timestamp = -1, int slots = -1,
                      long taskId = -1,
                      std::atomic<int> *checkpointCounter = nullptr)
      : m_checkpointId(id),
        m_pipelineId(0),
        m_triggerTimestamp(timestamp),
        m_numberOfSlots(slots),
        m_inputQueueSlots(0),
        m_counter(0),
        m_workers(SystemConf::getInstance().WORKER_THREADS),
        m_lastTaskId(taskId),
        m_checkpointCounter(checkpointCounter),
        m_state(CheckpointState::PENDING),
        m_checkpointSize(0),
        m_checkpointDuration(0){};

  void updateCounter(size_t size = 0) {
    m_counter.fetch_add(1);
    /*int oldValue = m_counter.load() ;
    while(!m_counter.compare_exchange_weak(oldValue, oldValue + 1,
                                       std::memory_order_release,
                                       std::memory_order_relaxed)) {
      _mm_pause();
      oldValue = m_counter.load() ;
    }*/
    updateSize(size);
    if (m_counter.load() == m_numberOfSlots.load()) {
      const std::lock_guard<std::mutex> lock(m_completionLock);
      setComplete();
      if (m_counter.load() > m_numberOfSlots.load()) {
        std::cout << "m_counter " << m_counter.load() << " m_numberOfSlots "
                  << m_numberOfSlots.load() << std::endl;
        throw std::runtime_error(
            "error: the counter of the checkpoint exceeds the expected number");
      }
    }
  }

  void updateSize(size_t size) { m_checkpointSize.fetch_add(size); }

  void updateDuration(size_t duration) {
    // take the current timestamp after the last checkpoint for the duration
    // m_checkpointDuration.fetch_add(duration);
    throw std::runtime_error(
        "error: the duration is measured only for a full snapshot and not "
        "individual checkpoints yet");
  }

  void setFilePath(const std::string path) { m_filePath = path; }

  void setCheckpointCounter(std::atomic<int> *checkpointCounter) {
    m_checkpointCounter = checkpointCounter;
  }

  void resetSlots() { m_numberOfSlots.store(0); }

  void increaseSlots(int slots, int inputQueueSlots = 0) {
    m_numberOfSlots.fetch_add(slots);
    m_inputQueueSlots.fetch_add(inputQueueSlots);
  }

  int getSlots() { return m_numberOfSlots.load(); }

  int getInputQueueSlots() { return m_inputQueueSlots.load(); }

  size_t getCheckpointSize() { return m_checkpointSize.load(); }

  void setCheckpointId(long id, int pipeline = 0) {
    m_checkpointId = id;
    m_pipelineId = pipeline;
  }

  void resetCheckpoint() {
    m_checkpointId = -1;
    m_triggerTimestamp = -1;
    m_numberOfSlots.store(0);
    m_inputQueueSlots.store(0);
    m_counter.store(0);
    m_workers.store(SystemConf::getInstance().WORKER_THREADS);
    m_lastTaskId = -1;
    m_state = CheckpointState::PENDING;
    m_checkpointSize = 0;
    m_checkpointDuration = 0;
    //m_readyFlag.store(false);
  }

  std::string toString() {
    std::string s;
    s.append(std::to_string(m_checkpointId));
    s.append(" [");
    if (m_state == CheckpointState::PENDING)
      s.append("Pending").append(" ");
    else
      s.append("Completed").append(" ");
    s.append("] ");
    s.append("duration (" + std::to_string(m_checkpointDuration) + ") ");
    s.append("triggered (" + std::to_string(m_triggerTimestamp) + ") ");
    return s;
  }

 private:
  void setComplete() {
    if (m_state != CheckpointState::COMPLETED /*&& m_readyFlag.load()*/) {
      m_state = CheckpointState::COMPLETED;
      if (m_checkpointCounter) {
        std::atomic_thread_fence(std::memory_order_release);
        m_checkpointCounter->fetch_add(1);
        std::cout << "[CP] checkpoint " + std::to_string(m_checkpointId)
                  << " has finished for pipeline " + std::to_string(m_pipelineId)
                  << std::endl;
      }
    }
  }

  void trySetComplete(){
    if (m_counter.load() == m_numberOfSlots.load()) {
      const std::lock_guard<std::mutex> lock(m_completionLock);
      setComplete();
    }
  }
};