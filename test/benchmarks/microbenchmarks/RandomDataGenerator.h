#pragma once

#include <random>

#include "utils/TupleSchema.h"
#include "utils/QueryApplication.h"
#include "utils/Utils.h"
#include "benchmarks/applications/BenchmarkQuery.h"

class RandomDataGenerator : public BenchmarkQuery {
 private:
  struct InputSchema {
    long timestamp;
    int attr1;
    int attr2;
  };

 public:
  TupleSchema *m_schema = nullptr;
  QueryApplication *m_application = nullptr;
  std::vector<char> *m_data = nullptr;
  bool m_debug = false;

  QueryApplication *getApplication() override {
    return m_application;
  }

  virtual void createApplication() = 0;

  void loadInMemoryData() {
    size_t len = SystemConf::getInstance().BUNDLE_SIZE;
    m_data = new std::vector<char>(len);
    auto buf = (InputSchema *) m_data->data();

    const int range_from = 1;
    const int range_to = 1000;
    std::random_device rand_dev;
    std::mt19937 generator(rand_dev());
    std::uniform_int_distribution<int> distr(range_from, range_to);

    unsigned long idx = 0;
    while (idx < len / sizeof(InputSchema)) {
      buf[idx].timestamp = idx;
      buf[idx].attr1 = distr(generator);
      buf[idx].attr2 = distr(generator);
      idx++;
    }

    if (m_debug) {
      std::cout << "timestamp jobId machineId eventType userId category priority cpu ram disk constraints" << std::endl;
      for (unsigned long i = 0; i < m_data->size() / sizeof(InputSchema); ++i) {
        printf("[DBG] %09d: %7d %8d %8d  \n",
               i, buf[i].timestamp, buf[i].attr1, buf[i].attr2);
      }
    }
  };

  std::vector<char> *getInMemoryData() override {
    return m_data;
  }

  std::vector<char> *getStaticData() override {
    throw std::runtime_error("error: this benchmark does not have static data");
  }

  TupleSchema *getSchema() override {
    if (m_schema == nullptr)
      createSchema();
    return m_schema;
  }

  void createSchema() {
    m_schema = new TupleSchema(3, "ClusterMonitoring");
    auto longAttr = AttributeType(BasicType::Long);
    auto intAttr = AttributeType(BasicType::Integer);

    m_schema->setAttributeType(0, longAttr); /*   timestamp:  long */
    m_schema->setAttributeType(1, intAttr);
    m_schema->setAttributeType(2, intAttr);
  }
};
