#pragma once
#include <cstdint>
#include <utility>

namespace zz {
inline uint64_t encode(int64_t i) { return (i >> 63) ^ (i << 1); }

inline int64_t decode(uint64_t i) { return (i >> 1) ^ (-(i & 1)); }
}  // namespace zz