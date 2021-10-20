#include "PerformanceMonitor.h"
#include "Measurement.h"
#include "LatencyMonitor.h"
#include "utils/QueryApplication.h"
#include "utils/TupleSchema.h"
#include "tasks/TaskFactory.h"
#include "buffers/PartialWindowResultsFactory.h"
#include "buffers/UnboundedQueryBufferFactory.h"
#include "tasks/WindowBatchFactory.h"

PerformanceMonitor::PerformanceMonitor(QueryApplication &application) : m_application(application),
                                                                        m_size(application.getQueries().size()),
                                                                        m_measurements(m_size) {

  // assume queries are pre-sorted based on their id
  for (int idx = 0; idx < m_size; ++idx) {
    auto currentMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    std::cout << "[MON] [MultiOperator] " << currentMs <<" S" << std::setfill('0') << std::setw(3)
              << std::to_string(application.getQueries()[idx]->getId()) << std::endl;
    m_measurements[idx] = new Measurement(
        application.getQueries()[idx]->getId(),
        application.getQueries()[idx]->getTaskDispatcher().get(),
        &application.getQueries()[idx]->getLatencyMonitor()
    );
  }
  m_t1 = std::chrono::high_resolution_clock::now();
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
void PerformanceMonitor::operator()() {
  while (true) {
    try {
      std::this_thread::sleep_for(std::chrono::milliseconds(SystemConf::getInstance().PERFORMANCE_MONITOR_INTERVAL));
    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
    }
    auto t2 = std::chrono::system_clock::now();
    m_time = t2.time_since_epoch() / std::chrono::milliseconds(1); // milliseconds
    m_dt = m_time - m__time;

    std::string builder;
    builder.append("[MON]");
    auto currentMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    builder.append(" " + std::to_string(currentMs));
    for (int i = 0; i < m_size; i++)
      builder.append(m_measurements[i]->getInfo(m_dt,
                                                (*m_application.getQueries()[i]).getSchema()->getTupleSize(),
                                                (*m_application.getQueries()[i]).getOutputSchema()->getTupleSize()));
    builder.append(" q " + std::to_string(m_application.getTaskQueueSize()));
    /* Append factory sizes */
    builder.append(" t " + std::to_string(TaskFactory::getInstance().getCount()));
    builder.append(" w " + std::to_string(WindowBatchFactory::getInstance().getCount()));
    builder.append(" b " + std::to_string(UnboundedQueryBufferFactory::getInstance().getCount()));
    builder.append(" p " + std::to_string(PartialWindowResultsFactory::getInstance().getCount()));

    std::cout << std::setprecision(3) << builder << std::endl;

    m__time = m_time;
    if (SystemConf::getInstance().DURATION > 0) {
      auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - m_t1);
      if (time_span.count() > SystemConf::getInstance().DURATION) {
        for (int i = 0; i < m_size; i++)
          m_measurements[i]->stop();
        std::cout << "[MON] Done." << std::endl;
        break;
      }
    }
  }
}
#pragma clang diagnostic pop

PerformanceMonitor::~PerformanceMonitor() {
  for (int idx = 0; idx < m_size; ++idx)
    delete (m_measurements[idx]);
}
