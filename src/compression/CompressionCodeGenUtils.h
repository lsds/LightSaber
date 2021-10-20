#pragma once

#include <cfloat>
#include <unordered_set>
#include <utility>

#include "utils/AttributeType.h"

/*
 * \brief Utilities for code generating compression algorithms.
 *
 * */

enum class CompressionType { None, FloatMult, BaseDelta, Dictionary, RLE };
uint32_t getRoundedTypeInt(uint32_t precision);
std::string getRoundedType(uint32_t precision);
std::string getType(uint32_t precision);
std::string getIncludesString();
std::string getInstrumentationMetrics(size_t workers, size_t cols);
std::string getCompressionAlgorithms();
std::string getCompressionVars(bool hasDict, size_t workers, size_t cols);

struct alignas(16) CompressionColumn {
  int m_column;
  BasicType m_type;
  std::string m_typeString;
  std::unordered_set<CompressionType> m_comps;
  uint32_t m_precision;
  uint32_t m_diffPrecision;
  uint32_t m_RLEPrecision;
  uint32_t m_multiplier;
  double m_min, m_max;
  std::string m_expression;
  explicit CompressionColumn(int col = 0, BasicType type = BasicType::Long,
                             uint32_t precision = 64, uint32_t diffPrecision = 64,
                             uint32_t rlePrecision = 64, uint32_t multiplier = 1,
                             double min = DBL_MIN, double max = DBL_MAX)
      : m_column(col),
        m_type(type),
        m_typeString(AttributeType::m_typeNames.find(type)->second),
        m_precision(precision),
        m_diffPrecision(diffPrecision),
        m_RLEPrecision(rlePrecision),
        m_multiplier(multiplier),
        m_min(min),
        m_max(max){}
};