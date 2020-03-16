#include "monitors/LatencyMonitor.h"
#include "utils/Utils.h"
#include "buffers/QueryBuffer.h"

LatencyMonitor::LatencyMonitor(long timeReference) : m_count(0L),
                                                     m_min(DBL_MAX),
                                                     m_max(DBL_MIN),
                                                     m_avg(0.0),
                                                     m_timestampReference(timeReference),
                                                     m_latency(0.0),
                                                     m_active(true) {}

void LatencyMonitor::disable() { m_active.store(false); }

std::string LatencyMonitor::toString() {
  std::string latencyString;
  if (m_count < 2 || !m_active.load())
    return latencyString;

  m_avg = m_latency / ((double) m_count);
  std::ostringstream streamObj;
  streamObj << std::fixed;
  streamObj << std::setprecision(3);
  streamObj << " [avg " << std::to_string(m_avg);
  streamObj << " min " << std::to_string(m_min);
  streamObj << " max " << std::to_string(m_max);
  streamObj << "]";
  latencyString = streamObj.str();

  return latencyString;
}

void LatencyMonitor::monitor(QueryBuffer &buffer, long latencyMark) {

  (void) buffer;

  if (!m_active.load()) {
    return;
  }

  double dt = 0;
  /* Check buffer */
  long t1 = latencyMark; //(long) Utils::getSystemTimestamp (buffer.getLong(mark));
  auto currentTime = std::chrono::high_resolution_clock::now();
  auto currentTimeNano = std::chrono::duration_cast<std::chrono::nanoseconds>(currentTime.time_since_epoch()).count();
  long t2 = (currentTimeNano - m_timestampReference) / 1000L;
  dt = ((double) (t2 - t1)) / 1000.; /* In milliseconds */

  m_measurements.push_back(dt);

  m_latency += dt;
  m_count += 1;

  m_min = std::min(dt, m_min);
  m_max = std::max(dt, m_max);
}

void LatencyMonitor::stop() {
  m_active.store(false);

  int length = m_measurements.size();

  std::cout << "[MON] [LatencyMonitor] " << std::to_string(length) << " measurements" << std::endl;

  if (length < 1)
    return;

  std::sort(m_measurements.begin(), m_measurements.end());

  std::ostringstream streamObj;
  streamObj << std::fixed;
  streamObj << std::setprecision(3);
  streamObj << "[MON] [LatencyMonitor] 5th " << std::to_string(evaluateSorted(5));
  streamObj << " 25th " << std::to_string(evaluateSorted(25));
  streamObj << " 50th " << std::to_string(evaluateSorted(50));
  streamObj << " 75th " << std::to_string(evaluateSorted(75));
  streamObj << " 99th " << std::to_string(evaluateSorted(99));
  std::cout << streamObj.str() << std::endl;
}

double LatencyMonitor::evaluateSorted(const double p) {
  double n = m_measurements.size();
  double pos = p * (n + 1) / 100;
  double fpos = floor(pos);
  int intPos = (int) fpos;
  double dif = pos - fpos;

  if (pos < 1) {
    return m_measurements[0];
  }
  if (pos >= n) {
    return m_measurements[m_measurements.size() - 1];
  }

  double lower = m_measurements[intPos - 1];
  double upper = m_measurements[intPos];
  return lower + dif * (upper - lower);
}
