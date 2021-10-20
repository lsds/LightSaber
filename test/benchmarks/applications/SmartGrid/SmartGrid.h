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

class SmartGrid : public BenchmarkQuery {
 private:
  long normalisedTimestamp = -1;
  struct InputSchema {
    long timestamp;
    float value;
    int property;
    int plug;
    int household;
    int house;
    int padding;

    static void parse(InputSchema &tuple, std::string &line, long &normalisedTimestamp) {
      std::istringstream iss(line);
      std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                     std::istream_iterator<std::string>{}};
      if (normalisedTimestamp == -1)
        normalisedTimestamp = std::stol(words[0]);

      tuple.timestamp = std::stol(words[0]) - normalisedTimestamp;
      tuple.value = std::stof(words[1]);
      tuple.property = std::stoi(words[2]);
      tuple.plug = std::stoi(words[3]);
      tuple.household = std::stoi(words[4]);
      tuple.house = std::stoi(words[5]);
    }
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

    std::string filePath = Utils::getHomeDir() + "/LightSaber/resources/datasets/smartgrid/";
    std::ifstream file(filePath + "smartgrid-data.txt");
    if (!file.good())
      throw std::runtime_error("error: input file does not exist, check the path.");
    std::string line;
    unsigned long idx = 0;
    while (std::getline(file, line) && idx < len / sizeof(InputSchema)) {
      InputSchema::parse(buf[idx], line, normalisedTimestamp);
      if (m_startTimestamp == 0) {
        m_startTimestamp = buf[0].timestamp;
      }
      m_endTimestamp = buf[idx].timestamp;
      idx++;
    }

    if (idx < len / sizeof(InputSchema)) {
      unsigned long iter = 0;
      auto barrier = idx-1;
      long lastTime = buf[idx-1].timestamp;
      while (idx < len / sizeof(InputSchema)) {
        std::memcpy(&buf[idx], &buf[iter], sizeof(InputSchema));
        buf[idx].timestamp += lastTime;
        m_endTimestamp = buf[idx].timestamp;
        idx++;
        iter++;
        if (iter == barrier) {
          iter = 0;
          lastTime = buf[idx-1].timestamp;
        }
      }
    }

    /*for (unsigned long i = 0; i < m_data->size() / sizeof(InputSchema); ++i) {
      if (i%10==0) {
        auto value = (int) std::round(buf[i].value * 1000);
        auto ii = 0;
        for (;ii < 10 && i < m_data->size() / sizeof(InputSchema); ++i) {
          buf[i].value = value;
          ii++;
        }
      }
    }*/

    if (m_debug) {
      std::cout << "timestamp value property plug household house" << std::endl;
      for (unsigned long i = 0; i < m_data->size() / sizeof(InputSchema); ++i) {
        printf("[DBG] %09d: %7d %5.3f %5d %5d %5d %5d \n",
               i, buf[i].timestamp, buf[i].value, buf[i].property, buf[i].plug,
               buf[i].household, buf[i].house);
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
    m_schema = new TupleSchema(7, "SmartGrid");
    auto longAttr = AttributeType(BasicType::Long);
    auto intAttr = AttributeType(BasicType::Integer);
    auto floatAttr = AttributeType(BasicType::Float);

    m_schema->setAttributeType(0, longAttr);  /* timestamp:  long */
    m_schema->setAttributeType(1, floatAttr); /*     value: float */
    m_schema->setAttributeType(2, intAttr);   /*  property:   int */
    m_schema->setAttributeType(3, intAttr);   /*      plug:   int */
    m_schema->setAttributeType(4, intAttr);   /* household:   int */
    m_schema->setAttributeType(5, intAttr);   /*     house:   int */
    m_schema->setAttributeType(6, intAttr);   /*   padding:   int */
  }
};
