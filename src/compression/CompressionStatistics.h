#pragma once

#include <atomic>
#include <cfloat>
#include <chrono>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "utils/Utils.h"

class ColumnReference;
struct CompressionColumn;

/*
 * \biref This class represents the compression statistics, such as the number of
 * consecutive values, min/max, and max difference of each column.
 *
 * */

struct CompressionStatistics {
  const int m_id;
  std::vector<ColumnReference *> *m_cols;
  size_t m_colsSize;
  std::mutex m_mutex;
  struct consAvg {
    explicit consAvg() = default;
    double m_consPercentage = 0.;
    uint32_t m_counter = 0;
    [[nodiscard]] double getAverage() const {
      return (m_counter == 0) ? 0 : m_consPercentage/(double)m_counter;
    }
    void reset() {
      m_consPercentage = 0.;
      m_counter = 0;
    }
  };
  std::vector<uint32_t> m_distinctVals;
  std::vector<consAvg> m_consecutiveVals; // run-length greater or equal to 3 or keep the average run-length!!!
  std::vector<double> m_min, m_max, m_maxDiff;
  std::vector<uint32_t> m_precision, m_diffPrecision, m_floatMult, m_RLEPrecision,
      m_prevPrecision, m_prevDiffPrecision, m_prevFloatMult, m_prevRLEPrecision;
  std::vector<bool> m_useRLE, m_prevUseRLE;
  std::vector<double> m_compRatio;
  double m_throughput;
  std::vector<std::shared_ptr<CompressionColumn>> m_compCols;
  bool m_hasData = false;
  bool m_useAgrresivelyRLE = true;

  const bool m_debug = false;

  explicit CompressionStatistics(int id, std::vector<ColumnReference *> *cols);
  void addStatistics(const uint32_t *distinctVals,
                     const double *consecutiveVals,
                     const double *min,
                     const double *max,
                     const double *maxDiff);
  bool updateCompressionDecision();
  static inline uint32_t getPrecision(double num, uint32_t &floatMult);
  void printStatisticsUnsafe() const;
  void printStatisticsDecisionUnsafe() const;

  template <typename T>
  static inline void initialize(std::vector<T> &left, const T *right) {
    if (!right) return;
    auto size = right->size();
    for (size_t i = 0; i < size; ++i) {
      left[i] = (*right)[i];
    }
  }

  template <typename T>
  static inline void initializeGT(std::vector<T> &left, const T *right) {
    if (!right) return;
    auto size = left.size();
    for (size_t i = 0; i < size; ++i) {
      left[i] = (left[i] > right[i]) ? left[i] : right[i];
    }
  }

  template <typename T>
  static inline void initializeLT(std::vector<T> &left, const T *right) {
    if (!right) return;
    auto size = left.size();
    for (size_t i = 0; i < size; ++i) {
      left[i] = (left[i] < right[i]) ? left[i] : right[i];
    }
  }

  static inline void initializeAVG(std::vector<consAvg> &left, const double *right) {
    if (!right) return;
    auto size = left.size();
    for (size_t i = 0; i < size; ++i) {
      left[i].m_consPercentage += right[i];
      left[i].m_counter++;
    }
  }

  template <typename T>
  static inline void checkOrInitialize(std::vector<T> &left,
                                       std::vector<T> *right) {
    if (!right) return;
    auto size = right->size();
    if (left.empty()) {
      left.resize(size);
    } else {
      if (size != left.size()) {
        throw std::runtime_error(
            "error: wrong number of columns during initialization");
      }
    }
    for (size_t i = 0; i < size; ++i) {
      left[i] = (*right)[i];
    }
  }
};