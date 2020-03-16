#pragma once

#include <stddef.h>
#include <atomic>

struct PaddedInt {
  int m_value;
  int m_pad[15];
  PaddedInt(int val = 0) : m_value(val) {};
};

struct AtomicPaddedInt {
  std::atomic<int> m_value;
  int m_pad[15];
  AtomicPaddedInt(int val = 0) : m_value(val) {};
  AtomicPaddedInt &operator=(int val) {
    m_value = val;
    return *this;
  }
  void store(int val) { m_value.store(val); }
  bool compare_exchange_weak(int &x, int y) { return m_value.compare_exchange_weak(x, y); }
  bool compare_exchange_strong(int &x, int y) { return m_value.compare_exchange_strong(x, y); }
};
