#pragma once

#include <atomic>
#include <cfloat>
#include <chrono>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "utils/Utils.h"

class Query;
class QueryApplication;
class QueryBuffer;
class ColumnReference;

/*
 * \brief This class is used to measure the compression statistics of a pipeline.
 *
 * */

class CompressionMonitor {
 private:
  long m_time, m__time = 0L;
  long m_dt;
  QueryApplication *m_application;
  int m_size;
  bool m_writeMetadata = false;

  // define (disk throughput) ?
  // codegen
  std::vector<std::thread> m_threads;
  std::vector<size_t> m_codeGenPos;
  Utils::DynamicLoader m_dLoader;
  //std::vector<std::function<void(int, char *, int, int, char *, int &, int, bool &, long)>> m_compressionFP;
  //std::vector<std::function<void(int, char *, int, int, char *, int &, int, bool &, long)>> m_decompressionFP;
  //std::function<void(int, char *, int, uint32_t *, double *, double *, double *, double *)> m_instrFP;

 public:
  explicit CompressionMonitor(QueryApplication *application);
  void operator()();

 private:
  void generateInstrumentation(Query *query);
  void generateCode(Query *query);
};