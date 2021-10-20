#pragma once

#include <iostream>
#include <fstream>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include "boost/date_time/posix_time/posix_time.hpp"

#include "utils/TupleSchema.h"
#include "utils/QueryApplication.h"
#include "utils/Utils.h"
#include "benchmarks/applications/BenchmarkQuery.h"

class ManufacturingEquipment : public BenchmarkQuery {
 private:
  struct alignas(64) InputSchema {
    long timestamp;
    long messageIndex;
    int mf01;           //Electrical Power Main Phase 1
    int mf02;           //Electrical Power Main Phase 2
    int mf03;           //Electrical Power Main Phase 3
    int pc13;           //Anode Current Drop Detection Cell 1
    int pc14;           //Anode Current Drop Detection Cell 2
    int pc15;           //Anode Current Drop Detection Cell 3
    unsigned int pc25;  //Anode Voltage Drop Detection Cell 1
    unsigned int pc26;  //Anode Voltage Drop Detection Cell 2
    unsigned int pc27;  //Anode Voltage Drop Detection Cell 3
    unsigned int res;
    int bm05 = 0;
    int bm06 = 0;
    /*bool bm05 = false;
    bool bm06 = false;
    bool bm07 = false;
    bool bm08 = false;
    bool bm09 = false;
    bool bm10 = false;*/

    static void parse(InputSchema &tuple, std::string &line, boost::posix_time::ptime &myEpoch) {
      std::istringstream iss(line);
      std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                     std::istream_iterator<std::string>{}};

      const std::locale
          loc = std::locale(std::locale::classic(), new boost::posix_time::time_input_facet("%Y-%m-%dT%H:%M:%S%f"));
      std::istringstream is(words[0]);
      is.imbue(loc);
      boost::posix_time::ptime myTime;
      is >> myTime;
      boost::posix_time::time_duration myTimeFromEpoch = myTime - myEpoch;

      tuple.timestamp = myTimeFromEpoch.total_milliseconds() / 1000;
      tuple.messageIndex = std::stol(words[1]);
      tuple.mf01 = std::stoi(words[2]);
      tuple.mf02 = std::stoi(words[3]);
      tuple.mf03 = std::stoi(words[4]);
      tuple.pc13 = std::stoi(words[5]);
      tuple.pc14 = std::stoi(words[6]);
      tuple.pc15 = std::stoi(words[7]);
      tuple.pc25 = std::stoi(words[8]);
      tuple.pc26 = std::stoi(words[9]);
      tuple.pc27 = std::stoi(words[10]);
      tuple.res = std::stoi(words[11]);
    }
  };

 public:
  TupleSchema *m_schema = nullptr;
  QueryApplication *m_application = nullptr;
  std::vector<char> *m_data = nullptr;
  std::vector<char> *m_secData = nullptr;
  bool m_debug = false;

  QueryApplication *getApplication() override {
    return m_application;
  }

  virtual void createApplication() = 0;

  void loadInMemoryData() {
    size_t len = SystemConf::getInstance().BUNDLE_SIZE;
    m_data = new std::vector<char>(len);
    m_secData = new std::vector<char>(len);
    auto buf = (InputSchema *) m_data->data();
    auto secBuf = (InputSchema *) m_secData->data();

    const std::string cell = "2012-02-22T16:46:28.9670320+00:00";
    const std::locale
        loc = std::locale(std::locale::classic(), new boost::posix_time::time_input_facet("%Y-%m-%dT%H:%M:%S%f"));
    std::istringstream is(cell);
    is.imbue(loc);
    boost::posix_time::ptime myEpoch;
    is >> myEpoch;
    //std::cout << myEpoch << std::endl;

    std::string filePath = Utils::getHomeDir() + "/LightSaber/resources/datasets/manufacturing_equipment/";
    std::ifstream file(filePath + "DEBS2012-small.txt");
    if (!file.good())
      throw std::runtime_error("error: input file does not exist, check the path.");
    std::string line;
    unsigned long idx = 0;
    while (std::getline(file, line) && idx < len / sizeof(InputSchema)) {
      InputSchema::parse(buf[idx], line, myEpoch);
      if (m_startTimestamp == 0) {
        m_startTimestamp = buf[0].timestamp;
      }
      m_endTimestamp = buf[idx].timestamp;
      idx++;
    }

    if (SystemConf::getInstance().ADAPTIVE_CHANGE_DATA) {
      for (unsigned long i = 0; i < idx; ++i) {
        // if (i%10==0) {
        auto mf01 = buf[i].mf01 % 16;
        auto mf02 = buf[i].mf02 % 16;
        auto mf03 = buf[i].mf03 % 16;
        auto ii = 0;
        for (; ii < 14 && i < idx; ++i) {
          secBuf[i].timestamp = buf[i].timestamp;
          secBuf[i].mf01 = mf01;
          secBuf[i].mf02 = mf02;
          secBuf[i].mf03 = mf03;
          ii++;
        }
        i = i - 1;
        //}
      }

      for (unsigned long i = 0; i < idx; ++i) {
        // if (i%14==0) {
        // auto mf01 = buf[i].mf01 %16;
        // auto mf02 = buf[i].mf02 %16;
        // auto mf03 = buf[i].mf03 %16;
        auto mf01 = buf[i].mf01 % 4095;
        auto mf02 = buf[i].mf02 % 4095;
        auto mf03 = buf[i].mf03 % 4095;
        auto ii = 0;
        for (; ii < 14 && i < m_data->size() / sizeof(InputSchema); ++i) {
          buf[i].mf01 = mf01;
          buf[i].mf02 = mf02;
          buf[i].mf03 = mf03;
          ii++;
        }
        i = i - 1;
        //}
      }
    }

    if (m_debug) {
      std::cout
          << "timestamp messageIndex mf01 mf02 mf03 pc13 pc14 pc15 pc25 pc26 pc27 res bm05 bm06 bm07 bm08 bm09 bm10"
          << std::endl;
      for (unsigned long i = 0; i < m_data->size() / sizeof(InputSchema); ++i) {
        printf("[DBG] %06d: %09d %09d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d"
               " %1d %1d \n",
               i, buf[i].timestamp, buf[i].messageIndex, buf[i].mf01,
               buf[i].mf02, buf[i].mf03, buf[i].pc13, buf[i].pc14, buf[i].pc15, buf[i].pc25,
               buf[i].pc26, buf[i].pc27, buf[i].res, buf[i].bm05, buf[i].bm06
            //buf[i].bm07, buf[i].bm08, buf[i].bm09, buf[i].bm10
        );
      }
    }
  };

  std::vector<char> *getInMemoryData() override {
    return m_data;
  }

  std::vector<char> *getSecondInMemoryData() override {
    return m_secData;
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
    m_schema = new TupleSchema(14, "ManufactoringEquipment");
    auto longAttr = AttributeType(BasicType::Long);
    auto intAttr = AttributeType(BasicType::Integer);
    auto boolAttr = AttributeType(BasicType::Char);

    m_schema->setAttributeType(0, longAttr);
    m_schema->setAttributeType(1, longAttr);
    m_schema->setAttributeType(2, intAttr);
    m_schema->setAttributeType(3, intAttr);
    m_schema->setAttributeType(4, intAttr);
    m_schema->setAttributeType(5, intAttr);
    m_schema->setAttributeType(6, intAttr);
    m_schema->setAttributeType(7, intAttr);
    m_schema->setAttributeType(8, intAttr);
    m_schema->setAttributeType(9, intAttr);
    m_schema->setAttributeType(10, intAttr);
    m_schema->setAttributeType(11, intAttr);
    m_schema->setAttributeType(12, intAttr);
    m_schema->setAttributeType(13, intAttr);
    //m_schema->setAttributeType(14,  boolAttr );
    //m_schema->setAttributeType(15,  boolAttr );
    //m_schema->setAttributeType(16,  boolAttr );
    //m_schema->setAttributeType(17,  boolAttr );
  }
};
