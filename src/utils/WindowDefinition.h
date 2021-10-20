#pragma once

#include <string>
#include <stdexcept>

/*
 * \brief The class used to defined different window types.
 *
 * At the moment only tumbling and sliding windows are supported.
 *
 * */

enum WindowMeasure { ROW_BASED, RANGE_BASED };
enum WindowType { TUMBLING, SLIDING, SESSION };
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-function"
static WindowMeasure fromString(std::string &measure) {
  if (measure == "row")
    return ROW_BASED;
  else if (measure == "range")
    return RANGE_BASED;
  else
    throw std::invalid_argument("error: unknown window type " + measure);
}
#pragma clang diagnostic pop

class WindowDefinition {
 private:
  long m_size;
  long m_slide;
  long m_paneSize;
  long m_gap;
  WindowMeasure m_windowMeasure;
  WindowType m_type;

  long gcd(long a, long b) {
    if (b == 0)
      return a;
    return
        gcd(b, a % b);
  }

 public:
  WindowDefinition(WindowMeasure measure = WindowMeasure::ROW_BASED, long size = 1, long slide = 1)
      : m_size(size), m_slide(slide), m_gap(0), m_windowMeasure(measure) {
    m_paneSize = gcd(m_size, m_slide);
    if (slide < size)
      m_type = SLIDING;
    else if (slide == size)
      m_type = TUMBLING;
    else
      throw std::runtime_error("error: wrong input parameters for window definition");
  }

  WindowDefinition(WindowMeasure measure, long gap)
      : m_size(0), m_slide(0), m_paneSize(0), m_gap(gap), m_windowMeasure(measure) {
    m_type = SESSION;
  }

  long getSize() {
    return m_size;
  }

  long getSlide() {
    return m_slide;
  }

  long getGap() {
    return m_gap;
  }

  WindowMeasure getWindowMeasure() {
    return m_windowMeasure;
  }

  WindowType getWindowType() {
    return m_type;
  }

  long getPaneSize() {
    return m_paneSize;
  }

  long numberOfPanes() {
    return (m_size / m_paneSize);
  }

  long panesPerSlide() {
    return (m_slide / m_paneSize);
  }

  bool isRowBased() {
    return (m_windowMeasure == ROW_BASED);
  }

  bool isRangeBased() {
    return (m_windowMeasure == RANGE_BASED);
  }

  bool isTumbling() {
    return (m_size == m_slide);
  }
};