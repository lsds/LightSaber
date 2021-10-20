#include "compression/CompressionStatistics.h"

#include "compression/CompressionCodeGenUtils.h"
#include "cql/expressions/ColumnReference.h"
#include "utils/Utils.h"

class ColumnReference;

CompressionStatistics::CompressionStatistics(
    int id, std::vector<ColumnReference *> *cols)
    : m_id(id),
      m_cols(cols),
      m_colsSize(cols->size()),
      m_distinctVals(m_colsSize, 0),
      m_consecutiveVals(m_colsSize, consAvg()),
      m_min(m_colsSize, DBL_MAX),
      m_max(m_colsSize, DBL_MIN),
      m_maxDiff(m_colsSize, DBL_MIN),
      m_precision(m_colsSize, 32),
      m_diffPrecision(m_colsSize, 32),
      m_floatMult(m_colsSize, 1),
      m_RLEPrecision(m_colsSize, 32),
      m_prevPrecision(m_colsSize, 32),
      m_prevDiffPrecision(m_colsSize, 32),
      m_prevFloatMult(m_colsSize, 1),
      m_prevRLEPrecision(m_colsSize, 32),
      m_useRLE(m_colsSize, false),
      m_prevUseRLE(m_colsSize, false),
      m_compRatio(m_colsSize, 0),
      m_throughput(0.),
      m_compCols(cols->size(), nullptr) {
  //std::sort(m_cols->begin(), m_cols->end());
}

void CompressionStatistics::addStatistics(const uint32_t *distinctVals,
                                          const double *consecutiveVals,
                                          const double *min, const double *max,
                                          const double *maxDiff) {
  const std::lock_guard<std::mutex> lock(m_mutex);
  initializeGT<uint32_t>(m_distinctVals, distinctVals);
  initializeAVG(m_consecutiveVals, consecutiveVals);
  initializeLT<double>(m_min, min);
  initializeGT<double>(m_max, max);
  initializeGT<double>(m_maxDiff, maxDiff);
  m_hasData = true;
  if (m_debug) {
    printStatisticsUnsafe();
  }
}

bool CompressionStatistics::updateCompressionDecision() {
  bool hasChanged = false;
  const std::lock_guard<std::mutex> lock(m_mutex);
  if (!m_hasData)
    return hasChanged;
  // all vectors have the same size
  for (size_t i = 0; i < m_colsSize; ++i) {
    // update m_precision
    uint32_t precision = 0;
    precision = std::max(precision, getPrecision(m_min[i], m_floatMult[i]));
    precision = std::max(precision, getPrecision(m_max[i], m_floatMult[i]));
    m_precision[i] = precision;
    // we reset the min/max values later

    // update max diff precision
    precision = 0;
    //precision = std::max(precision, getPrecision(m_maxDiff[i], m_floatMult[i]));
    auto maxDiff = m_max[i] - m_min[i];
    precision = std::max(precision, getPrecision(maxDiff, m_floatMult[i]));
    m_diffPrecision[i] = precision;
    m_maxDiff[i] = DBL_MIN;

    // update rle decision
    int avg = (int)std::ceil(m_consecutiveVals[i].getAverage()) - 1; // todo: replace with ceil
    uint32_t dMul = 0;
    m_RLEPrecision[i] = getPrecision(avg, dMul);
    //if (m_RLEPrecision[i] >= 7) {
    //  m_RLEPrecision[i] = 6; // todo: remove this
    //}
    m_useRLE[i] = false;
    if (avg >= 5) {
      m_useRLE[i] = true;
    }
    m_consecutiveVals[i].reset();

    // todo: update dict decision

    // update previous values for comparison
    if (m_prevPrecision[i] != m_precision[i] ||
        m_prevDiffPrecision[i] != m_diffPrecision[i] ||
        m_prevUseRLE[i] != m_useRLE[i] ||
        m_floatMult[i] != m_prevFloatMult[i] ||
        m_RLEPrecision[i] != m_prevRLEPrecision[i]) {
      hasChanged = true;
      /*std::cout << "Compression: col " << i << " min " << m_min[i] << " max "
                << m_max[i] << " prec " << m_precision[i] << " diffPrec "
                << m_diffPrecision[i] << " RLE " << m_useRLE[i] << " RLEprec "
                << m_RLEPrecision[i] << " fMul " << m_floatMult[i] << std::endl;*/

      // create the column if it doesn't exist
      if (!m_compCols[i]) {
        m_compCols[i] = std::make_shared<CompressionColumn>(
            (*m_cols)[i]->getColumn(), (*m_cols)[i]->getBasicType(),
            m_precision[i], m_diffPrecision[i], m_RLEPrecision[i],
            m_floatMult[i], m_min[i], m_max[i]);
        if ((*m_cols)[i]->getColumn() == -1) {
          m_compCols[i]->m_expression = (*m_cols)[i]->getExpression();
        }
      } else {
        m_compCols[i]->m_precision = m_precision[i];
        m_compCols[i]->m_diffPrecision = m_diffPrecision[i];
        m_compCols[i]->m_RLEPrecision = m_RLEPrecision[i];
        m_compCols[i]->m_multiplier = m_floatMult[i];
        m_compCols[i]->m_min = m_min[i];
        m_compCols[i]->m_max = m_max[i];
      }
      // clear previous decisions
      m_compCols[i]->m_comps.clear();

      if (m_compCols[i]->m_column == 0) {
        // for timestamps use BaseDelta
        m_compCols[i]->m_comps.insert(CompressionType::BaseDelta);
      } else if (m_compCols[i]->m_type == BasicType::LongLong ||
                 (m_compCols[i]->m_type != BasicType::Float &&
                  m_compCols[i]->m_precision > 16)) {
        // for uint128 use Dictionary compression
        // todo: this can be group keys
        m_compCols[i]->m_comps.insert(CompressionType::Dictionary);
      } else if (m_compCols[i]->m_type == BasicType::Float) {
        // for floats use Float Multiplier
        m_compCols[i]->m_comps.insert(CompressionType::FloatMult);
        if (m_useAgrresivelyRLE) {
          m_compCols[i]->m_comps.insert(CompressionType::RLE);
        }
      } else {
        // use casting to reduce precision if possible
        m_compCols[i]->m_comps.insert(CompressionType::None);
      }
      // finally check if RLE is needed
      if (m_useRLE[i]) {
        m_compCols[i]->m_comps.insert(CompressionType::RLE);
      }
    }
    m_min[i] = DBL_MAX;
    m_max[i] = DBL_MIN;
    m_prevPrecision[i] = m_precision[i];
    m_prevDiffPrecision[i] = m_diffPrecision[i];
    m_prevRLEPrecision[i] = m_RLEPrecision[i];
    m_prevUseRLE[i] = m_useRLE[i];
    m_prevFloatMult[i] = m_floatMult[i];
  }

  m_hasData = false;
  if (m_debug) {
    printStatisticsDecisionUnsafe();
  }
  return hasChanged;
}

// todo: handle higher precisions and remove the std::abs(floatpart) >=
// 0.0000001 assumption
inline uint32_t CompressionStatistics::getPrecision(double num,
                                                    uint32_t &floatMult) {
  if (num < 0) {
    throw std::runtime_error(
        "error: cannot measure the precision of a negative number yet");
  }
  uint32_t bits = 0;
  double intpart;
  // does it have a fractional part?
  if (modf(num, &intpart) == 0) {
    auto dec = (uint32_t)intpart;  // use std::abs for negative values?
    auto clz = __builtin_clz(dec);
    bits = (dec == 0) ? 1 : 32 - clz;
  } else {
    auto count = 0;
    auto floatpart = std::abs(num);
    floatpart = floatpart - int(floatpart);
    while (std::abs(floatpart) >= 0.0000001) {
      floatpart = floatpart * 10;
      count = count + 1;
      floatpart = floatpart - int(floatpart);
    }
    count = (count > 3) ? 3 : count;  // todo: fix this
    floatMult = std::pow(10, count);
    auto dec = (uint32_t)(num * floatMult);
    auto clz = __builtin_clz(dec);
    bits = (dec == 0) ? 1 : 32 - clz;
  }

  // std::cout << "The precision of " << num << " is " <<
  // Utils::getPowerOfTwo(bits) << " bits." << std::endl;
  // do we need to round the result to a power of two?
  if (bits == 1)
    bits = 2;
  return bits;
}

void CompressionStatistics::printStatisticsUnsafe() const {
  std::ostringstream streamObj;
  streamObj << "[MON] [CompressionStatistics] ID-" + std::to_string(m_id) + " ";
  streamObj << std::fixed << std::setprecision(2);
  for (size_t i = 0; i < m_colsSize; ++i) {
    streamObj << "[COL-" + std::to_string(i) + " ";
    streamObj << "MIN " << m_min[i] << " ";
    streamObj << "MAX " << m_max[i] << " ";
    streamObj << "MD " << m_maxDiff[i] << " ";
    streamObj << "CV "
              << m_consecutiveVals[i].m_consPercentage /
                     m_consecutiveVals[i].m_counter;
    streamObj << "]";
  }
  std::cout << streamObj.str() << std::endl;
}

void CompressionStatistics::printStatisticsDecisionUnsafe() const {
  std::ostringstream streamObj;
  streamObj << "[MON] [CompressionStatistics] ID-" + std::to_string(m_id) +
                   " RES ";
  for (size_t i = 0; i < m_colsSize; ++i) {
    streamObj << "[COL-" + std::to_string(i) + " ";
    streamObj << "P " + std::to_string(m_precision[i]) + " ";
    streamObj << "DP " + std::to_string(m_diffPrecision[i]) + " ";
    streamObj << "FM " + std::to_string(m_floatMult[i]) + " ";
    streamObj << "RLE " + std::to_string(m_useRLE[i]);
    streamObj << "]";
  }
  std::cout << streamObj.str() << std::endl;
}
