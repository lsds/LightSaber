#pragma once

#include <atomic>
#include <mutex>

#include "utils/SystemConf.h"

/*
 * \brief This class describes the process of recovery for a pipeline.
 *
 * */

enum RecoveryState : uint8_t { FINISHED, AWAITING };

class Recovery {
 private:
  int m_pipelineId;
  long m_triggerTimestamp;
  std::atomic<int> m_numberOfSlots;
  std::atomic<int> m_inputQueueSlots;
  std::mutex m_completionLock;
  std::atomic<int> m_counter;
  std::atomic<int> m_workers;
  long m_lastTaskId;
  std::atomic<int> *m_recoveryCounter;
  RecoveryState m_state;
  std::atomic<size_t> m_recoverySize;
  std::atomic<size_t> m_recoveryDuration;
  std::string m_filePath;
  //std::atomic<bool> m_readyFlag;

  friend class FileBackedCheckpointCoordinator;

 public:
  explicit Recovery(long id = -1, long timestamp = -1, int slots = 0,
                      long taskId = -1,
                      std::atomic<int> *recoveryCounter = nullptr)
      :
        m_pipelineId(0),
        m_triggerTimestamp(timestamp),
        m_numberOfSlots(slots),
        m_inputQueueSlots(0),
        m_counter(0),
        m_workers(SystemConf::getInstance().WORKER_THREADS),
        m_lastTaskId(taskId),
        m_recoveryCounter(recoveryCounter),
        m_state(RecoveryState::AWAITING),
        m_recoverySize(0),
        m_recoveryDuration(0) {};

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
            "error: the counter of the recovery exceeds the expected number");
      }
    }
  }

  void updateSize(size_t size) { m_recoverySize.fetch_add(size); }

  void updateDuration(size_t duration) {
    throw std::runtime_error(
        "error: the updateDuration function is not implemented yet");
  }

  void setFilePath(const std::string path) { m_filePath = path; }

  void setRecoveryCounter(std::atomic<int> *recoveryCounter) {
    m_recoveryCounter = recoveryCounter;
  }

  void resetSlots() { m_numberOfSlots.store(0); }

  void increaseSlots(int slots, int inputQueueSlots = 0) {
    m_numberOfSlots.fetch_add(slots);
    m_inputQueueSlots.fetch_add(inputQueueSlots);
  }

  int getSlots() { return m_numberOfSlots.load(); }

  int getInputQueueSlots() { return m_inputQueueSlots.load(); }

  size_t getRecoverySize() { return m_recoverySize.load(); }

  void setRecoveryId(int pipeline = 0) {
    m_pipelineId = pipeline;
  }

  void resetRecovery() {
    m_triggerTimestamp = -1;
    m_numberOfSlots.store(0);
    m_inputQueueSlots.store(0);
    m_counter.store(0);
    m_workers.store(SystemConf::getInstance().WORKER_THREADS);
    m_lastTaskId = -1;
    m_state = RecoveryState::AWAITING;
    m_recoverySize = 0;
    m_recoveryDuration = 0;
    //m_readyFlag.store(false);
  }

  std::string toString() {
    std::string s;
    s.append(" [");
    if (m_state == RecoveryState::AWAITING)
      s.append("Pending").append(" ");
    else
      s.append("Completed").append(" ");
    s.append("] ");
    s.append("duration (" + std::to_string(m_recoveryDuration) + ") ");
    s.append("triggered (" + std::to_string(m_triggerTimestamp) + ") ");
    return s;
  }

 private:
  void setComplete() {
    if (m_state != RecoveryState::FINISHED /*&& m_readyFlag.load()*/) {
      m_state = RecoveryState::FINISHED;
      if (m_recoveryCounter) {
        std::atomic_thread_fence(std::memory_order_release);
        m_recoveryCounter->fetch_add(1);
        std::cout << "[DBG] recovery "
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