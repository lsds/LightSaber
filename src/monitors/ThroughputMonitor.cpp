#include <fstream>
#include <iterator>

#include "ThroughputMonitor.h"
#include "LatencyMonitor.h"
#include "utils/QueryApplication.h"

long SimpleMeasurement::m_sumTuples = 0;
int SimpleMeasurement::m_measurements = 0;

SimpleMeasurement::SimpleMeasurement(std::atomic<long> &bytesProcessed) : m_bytesProcessedAtomic(bytesProcessed) {}

std::string SimpleMeasurement::getInfo(long delta, int inputTuple, int outputTuple) {
  std::string s;
  m_bytesProcessed = m_bytesProcessedAtomic.load(std::memory_order_relaxed);
  if (m__bytesProcessed > 0) {
    m_Dt = ((double) delta / 1000.0);
    m_MBpsProcessed = ((double) m_bytesProcessed - (double) m__bytesProcessed) / m__1MB_ / m_Dt;
    m_MBpsGenerated = ((double) m_bytesGenerated - (double) m__bytesGenerated) / m__1MB_ / m_Dt;
    std::string q_id = std::to_string(m_id);
    q_id = std::string(3 - q_id.length(), '0') + q_id;

    // Create an output string stream
    std::ostringstream streamObj;
    streamObj << std::fixed;
    streamObj << std::setprecision(3);
    streamObj << " S" + q_id + " " << m_MBpsProcessed << " MB/s ";
    if (inputTuple != 0) {
      streamObj << "(" << (m_bytesProcessed - m__bytesProcessed) / inputTuple << " t/sec) ";
      m_sumTuples += (m_bytesProcessed - m__bytesProcessed) / inputTuple;
      m_measurements++;
      streamObj << "(Average: " << m_sumTuples / m_measurements << " t/sec) ";
    }
    streamObj << "output " << m_MBpsGenerated << " MB/s "; //["+std::to_string(monitor)+"]";
    if (outputTuple != 0) {
      streamObj << "(" << (m_bytesGenerated - m__bytesGenerated) / outputTuple << " t/sec) ";
    }

    streamObj << "m_bytesProcessed " << m_bytesProcessed << " ";
    s = streamObj.str();
  }
  m__bytesProcessed = m_bytesProcessed;
  m__bytesGenerated = m_bytesGenerated;
  return s;
}

std::string SimpleMeasurement::getThroughput(long delta, int inputTuple, int outputTuple) {
  std::string s;
  m_bytesProcessed = m_bytesProcessedAtomic.load(std::memory_order_relaxed);
  if (m__bytesProcessed > 0) {
    m_Dt = ((double) delta / 1000.0);
    m_MBpsProcessed = ((double) m_bytesProcessed - (double) m__bytesProcessed) / m__1MB_ / m_Dt;
    m_MBpsGenerated = ((double) m_bytesGenerated - (double) m__bytesGenerated) / m__1MB_ / m_Dt;
    std::string q_id = std::to_string(m_id);
    q_id = std::string(3 - q_id.length(), '0') + q_id;

    // Create an output string stream
    std::ostringstream streamObj;
    streamObj << std::fixed;
    streamObj << std::setprecision(3);
    streamObj << m_MBpsProcessed << " MB/s ";
    if (inputTuple != 0) {
      streamObj << "(" << (m_bytesProcessed - m__bytesProcessed) / inputTuple
                << " t/sec) ";
      m_sumTuples += (m_bytesProcessed - m__bytesProcessed) / inputTuple;
      m_measurements++;
      streamObj << "(Average: " << m_sumTuples / m_measurements
                << " t/sec) ";
    }

    s = streamObj.str();
  }
  m__bytesProcessed = m_bytesProcessed;
  m__bytesGenerated = m_bytesGenerated;
  return s;
}

SimpleMeasurement::~SimpleMeasurement() {};

ThroughputMonitor::ThroughputMonitor(std::atomic<long> &bytesProcessed, int inputTuple, int outputTuple) : m_bytesProcessed(bytesProcessed),
                                                                            m_measurement(std::make_unique<SimpleMeasurement>(bytesProcessed)),
                                                                            m_inputTupleSize(inputTuple), m_outputTupleSize(outputTuple) {}
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
void ThroughputMonitor::operator()() {
  while (true) {
    try {
      std::this_thread::sleep_for(std::chrono::milliseconds(m_throughputMonitorInterval));
    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
    }
    m_time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1); // milliseconds
    m_dt = m_time - m__time;

    std::string builder;
    if (m_printInConsole) {
      builder.append("[Q-MON]");
      builder.append(m_measurement->getInfo(m_dt, m_inputTupleSize, m_outputTupleSize));
      std::cout << std::setprecision(3) << builder << std::endl;
    } else {
      builder.append(std::to_string(m_counter*m_throughputMonitorInterval) + " ");
      builder.append(m_measurement->getThroughput(m_dt, m_inputTupleSize, m_outputTupleSize));
      m_storedMeasurements.emplace_back(builder);
    }

    m__time = m_time;
    if (m_duration > 0) {
      if (m_counter++ > m_duration) {
        if (!m_printInConsole) {
          std::ofstream output_file("throughput_measurements.txt");
          std::ostream_iterator<std::string> output_iterator(output_file, "\n");
          std::copy(m_storedMeasurements.begin(), m_storedMeasurements.end(), output_iterator);
        }
        std::cout << "[Q-MON] Done." << std::endl;
        break;
      }
    }
  }
}
#pragma clang diagnostic pop

ThroughputMonitor::~ThroughputMonitor() {}