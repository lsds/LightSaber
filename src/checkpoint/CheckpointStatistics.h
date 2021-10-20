#pragma once

#include <cstdlib>
#include <string>

/*
 * \brief Utilities to keep track of basic checkpoint statistics, such
 * as average duration, or average size before and after compression.
 *
 * */

class CheckpointStatistics {
 private:
  double m_duration = 0;
  size_t m_durationCounter = 0;
  double m_preparation = 0;
  size_t m_preparationCounter = 0;
  size_t m_initialSize = 0;
  size_t m_initialSizeCounter = 0;
  size_t m_checkpointSize = 0;
  size_t m_checkpointCounter = 0;

 public:
  CheckpointStatistics() {

  }

  void registerDuration(double duration) {
    m_duration += duration;
    m_durationCounter++;
  }

  void registerPreparation(double preparation) {
    m_preparation += preparation;
    m_preparationCounter++;
  }

  void registerSize(size_t size) {
    m_initialSize += size;
    m_initialSizeCounter++;
  }

  void registerCheckpointSize(size_t size) {
    m_checkpointSize += size;
    m_checkpointCounter++;
  }

  std::string toString() {
    std::string s;
    s.append("[prep ")
        .append(std::to_string(double(m_preparation/m_preparationCounter)))
        .append(" dur ")
        .append(std::to_string(double(m_duration/m_durationCounter)))
        .append(" initial bytes ")
        .append(std::to_string(m_initialSize/m_initialSizeCounter))
        .append(" stored bytes ")
        .append(std::to_string(m_checkpointSize/m_checkpointCounter))
        .append("]");
    return s;
  }
};