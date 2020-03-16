#pragma once

#include <iostream>
#include <fstream>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>

#include "utils/TupleSchema.h"
#include "utils/QueryApplication.h"
#include "utils/Utils.h"
#include "benchmarks/applications/BenchmarkQuery.h"

class LinearRoadBenchmark : public BenchmarkQuery {
 private:
  struct InputSchema {
    long timestamp;
    int vehicle;
    float speed;
    int highway;
    int lane;
    int direction;
    int position;

    static void parse(InputSchema &tuple, std::string &line) {
      std::istringstream iss(line);
      std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                     std::istream_iterator<std::string>{}};
      tuple.timestamp = std::stol(words[0]);
      tuple.vehicle = std::stoi(words[1]);
      tuple.speed = std::stof(words[2]);
      tuple.highway = std::stoi(words[3]);
      tuple.lane = std::stoi(words[4]);
      tuple.direction = std::stoi(words[5]);
      tuple.position = std::stoi(words[6]);
    }
  };

 public:
  TupleSchema *m_schema = nullptr;
  QueryApplication *m_application = nullptr;
  std::vector<char> *m_data = nullptr;
  bool m_debug = false;
  std::string m_fileName;

  QueryApplication *getApplication() override {
    return m_application;
  }

  virtual void createApplication() = 0;

  void loadInMemoryData() {
    size_t len = SystemConf::getInstance().BUNDLE_SIZE;
    m_data = new std::vector<char>(len);
    auto buf = (InputSchema *) m_data->data();

    std::string filePath = Utils::GetHomeDir() + "/LightSaber/resources/datasets/lrb/";
    std::ifstream file(filePath + m_fileName);
    std::string line;
    unsigned long idx = 0;
    while (std::getline(file, line) && idx < len / sizeof(InputSchema)) {
      InputSchema::parse(buf[idx], line);
      idx++;
    }

    if (m_debug) {
      std::cout << "timestamp vehicle speed highway lane direction position" << std::endl;
      for (unsigned long i = 0; i < m_data->size() / sizeof(InputSchema); ++i) {
        printf("[DBG] %09d: %7d %8d %5.3f %13d %3d %6d %2d \n",
               i, buf[i].timestamp, buf[i].vehicle,
               buf[i].speed, buf[i].highway, buf[i].lane,
               buf[i].direction, buf[i].position);
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
    m_schema = new TupleSchema(7, "LinearRoadBenchmark");
    auto longAttr = AttributeType(BasicType::Long);
    auto intAttr = AttributeType(BasicType::Integer);
    auto floatAttr = AttributeType(BasicType::Float);

    m_schema->setAttributeType(0, longAttr);  /*  timestamp:  long */
    m_schema->setAttributeType(1, intAttr);   /*    vehicle:   int */
    m_schema->setAttributeType(2, floatAttr); /*      speed: float */
    m_schema->setAttributeType(3, intAttr);   /*    highway:   int */
    m_schema->setAttributeType(4, intAttr);   /*       lane:   int */
    m_schema->setAttributeType(5, intAttr);   /*  direction:   int */
    m_schema->setAttributeType(6, intAttr);   /*   position:   int */
  }
};
