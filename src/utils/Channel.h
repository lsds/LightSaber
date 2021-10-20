#pragma once

#include <condition_variable>
#include <mutex>
#include <list>
#include <thread>

/*
 * \brief A simple communication channel implementation.
 *
 * */

template <class item>
class Channel {
 private:
  std::list<item> m_queue;
  std::mutex m_mutex;
  std::condition_variable m_cv;
  bool m_closed;

 public:
  Channel() : m_closed(false) {}
  void close() {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_closed = true;
    m_cv.notify_all();
  }
  bool is_closed() {
    std::unique_lock<std::mutex> lock(m_mutex);
    return m_closed;
  }

  void put(const item &i) {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_closed) throw std::logic_error("error: put to closed channel");
    m_queue.push_back(i);
    m_cv.notify_one();
  }

  bool get(item &out, bool wait = true) {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (wait) m_cv.wait(lock, [&]() { return m_closed || !m_queue.empty(); });
    if (m_queue.empty()) return false;
    out = m_queue.front();
    m_queue.pop_front();
    return true;
  }
};