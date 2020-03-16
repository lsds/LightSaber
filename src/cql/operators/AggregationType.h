#pragma once

#include <algorithm>
#include <string>
#include <stdexcept>

/*
 * \brief Helper utils for aggregation functions.
 *
 * */

enum AggregationType { MAX, MIN, CNT, SUM, AVG, W_AVG };

namespace AggregationTypes {
static inline const std::string toString(AggregationType v) {
  switch (v) {
    case MAX: return "MAX";
    case MIN: return "MIN";
    case CNT: return "CNT";
    case SUM: return "SUM";
    case AVG: return "AVG";
    case W_AVG: return "W_AVG";
    default:throw std::runtime_error("error: unknown aggregation type");
  }
}
static inline bool isInvertible(AggregationType v) {
  switch (v) {
    case CNT:
    case SUM:
    case AVG:
    case W_AVG:return true;
    default:return false;
  }
}
static inline AggregationType fromString(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), ::tolower);
  if (s == "avg")
    return AVG;
  else if (s == "sum")
    return SUM;
  else if (s == "cnt")
    return CNT;
  else if (s == "min")
    return MIN;
  else if (s == "max")
    return MAX;
  else
    throw std::runtime_error("error: unknown aggregation type");
}
}