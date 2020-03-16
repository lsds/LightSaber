#pragma once

#include <stddef.h>
#include <atomic>

struct PaddedLong {
  long m_value;
  long m_pad[7];
  PaddedLong(long val = 0L) : m_value(val) {};
};

struct AtomicPaddedLong {
  std::atomic<long> m_value;
  long m_pad[7];
  AtomicPaddedLong(long val = 0L) : m_value(val) {};
};
