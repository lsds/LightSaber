#pragma once

#include "utils/TupleSchema.h"
#include "utils/QueryApplication.h"
#include "utils/Utils.h"
#include "../BenchmarkQuery.h"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <random>
#include <limits>
#include <fstream>
#include <unordered_set>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>

class YahooBenchmark : public BenchmarkQuery {
 private:

  struct InputSchema_64 {
    long timestamp;
    long user_id;
    long page_id;
    long ad_id;
    long ad_type;
    long event_type;
    long ip_address;
    long padding;

    static void parse(InputSchema_64 &tuple, std::string &line) {
      std::istringstream iss(line);
      std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                     std::istream_iterator<std::string>{}};
      tuple.timestamp = std::stol(words[0]);
      tuple.user_id = std::stoul(words[1]);
      tuple.page_id = std::stoul(words[2]);
      tuple.ad_id = std::stoul(words[3]);
      tuple.ad_type = std::stoul(words[4]);
      tuple.event_type = std::stoul(words[5]);
      tuple.ip_address = std::stoul(words[6]);
    }
  };

  struct InputSchema_128 {
    long timestamp;
    long padding_0;
    __uint128_t user_id;
    __uint128_t page_id;
    __uint128_t ad_id;
    long ad_type;
    long event_type;
    __uint128_t ip_address;
    __uint128_t padding_1;
    __uint128_t padding_2;

    static void parse(InputSchema_128 &tuple, std::string &line) {
      std::istringstream iss(line);
      std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                     std::istream_iterator<std::string>{}};
      tuple.timestamp = std::stol(words[0]);
      tuple.user_id = std::stoul(words[1]);
      tuple.page_id = std::stoul(words[2]);
      tuple.ad_id = std::stoul(words[3]);
      tuple.ad_type = std::stoul(words[4]);
      tuple.event_type = std::stoul(words[5]);
      tuple.ip_address = std::stoul(words[6]);
    }
  };

 public:
  TupleSchema *m_schema = nullptr;
  QueryApplication *m_application = nullptr;
  std::vector<char> *m_data = nullptr;
  std::vector<char> *m_staticData = nullptr;
  bool m_debug = false;
  bool m_is64 = false;

  QueryApplication *getApplication() override {
    return m_application;
  }

  virtual void createApplication() = 0;

  void loadInMemoryData(uint32_t campaignNum = 100) {
    if (m_is64)
      loadInMemoryData_64();
    else
      loadInMemoryData_128(campaignNum);
  };

  void loadInMemoryData_64() {
    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<long> distr(0, 1000000);

    std::unordered_set<long> set;

    size_t len = SystemConf::getInstance().BUNDLE_SIZE;
    m_data = new std::vector<char>(len);
    m_staticData = new std::vector<char>(2 * sizeof(long) * 1024);
    auto buf = (InputSchema_64 *) m_data->data();
    auto staticBuf = (long *) m_staticData->data();

    long campaign_id = distr(eng); //0;
    set.insert(campaign_id);
    for (unsigned long i = 0; i < 1000; ++i) {
      if (i > 0 && i % 10 == 0) {
        campaign_id = distr(eng); //++;
        bool is_in = set.find(campaign_id) != set.end();
        while (is_in) {
          campaign_id = distr(eng);
          is_in = set.find(campaign_id) != set.end();
        }
      }
      staticBuf[i * 2] = i;
      staticBuf[i * 2 + 1] = campaign_id;
    }

    std::string line;
    auto user_id = distr(eng);
    auto page_id = distr(eng);
    unsigned long idx = 0;
    while (idx < len / sizeof(InputSchema_64)) {
      auto ad_id = staticBuf[((idx % 100000) % 1000) * 2];
      auto ad_type = (idx % 100000) % 5;
      auto event_type = (idx % 100000) % 3;
      line = std::to_string(idx / 1000) + " " + std::to_string(user_id) + " " + std::to_string(page_id) + " " +
          std::to_string(ad_id) + " " + std::to_string(ad_type) + " " + std::to_string(event_type) + " " +
          std::to_string(-1);
      InputSchema_64::parse(buf[idx], line);
      if (m_startTimestamp == 0) {
        m_startTimestamp = buf[0].timestamp;
      }
      m_endTimestamp = buf[idx].timestamp;
      idx++;
    }

    if (m_debug) {
      std::cout << "timestamp user_id page_id ad_id ad_type event_type ip_address" << std::endl;
      for (unsigned long i = 0; i < m_data->size() / sizeof(InputSchema_64); ++i) {
        printf("[DBG] %09d: %7d %13ld %8ld %13ld %3ld %6ld %2ld \n",
               i, buf[i].timestamp, buf[i].user_id, buf[i].page_id, buf[i].ad_id,
               buf[i].ad_type, buf[i].event_type, buf[i].ip_address);
      }
    }
  };

  void loadInMemoryData_128(uint32_t campaignNum) {
    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<long> distr(0, 1000000);

    std::unordered_set<long> set;

    auto adsNum = campaignNum * 10;
    assert(adsNum <= 100000);
    size_t totalSize = Utils::getPowerOfTwo(adsNum);
    size_t len = SystemConf::getInstance().BUNDLE_SIZE;
    m_data = new std::vector<char>(len);
    m_staticData = new std::vector<char>(2 * sizeof(__uint128_t) * totalSize);
    auto buf = (InputSchema_128 *) m_data->data();
    auto staticBuf = (__uint128_t *) m_staticData->data();

    long campaign_id = distr(eng); //0;
    set.insert(campaign_id);
    for (unsigned long i = 0; i < adsNum; ++i) {
      if (i > 0 && i % 10 == 0) {
        campaign_id = distr(eng); //++;
        bool is_in = set.find(campaign_id) != set.end();
        while (is_in) {
          campaign_id = distr(eng);
          is_in = set.find(campaign_id) != set.end();
        }
      }
      staticBuf[i * 2] = i;
      staticBuf[i * 2 + 1] = (__uint128_t) campaign_id;
    }

    std::string line;
    auto user_id = distr(eng);
    auto page_id = distr(eng);
    unsigned long idx = 0;
    while (idx < len / sizeof(InputSchema_128)) {
      auto ad_id = staticBuf[((idx % 100000) % adsNum) * 2];
      auto ad_type = (idx % 100000) % 5;
      auto event_type = (idx % 100000) % 3;
      line = std::to_string(idx / 1000) + " " + std::to_string(user_id) + " " + std::to_string(page_id) + " " +
          std::to_string((long) ad_id) + " " + std::to_string(ad_type) + " " + std::to_string(event_type) + " " +
          std::to_string(-1);
      InputSchema_128::parse(buf[idx], line);
      if (m_startTimestamp == 0) {
        m_startTimestamp = buf[0].timestamp;
      }
      m_endTimestamp = buf[idx].timestamp;
      idx++;
    }

    if (m_debug) {
      std::cout << "timestamp user_id page_id ad_id ad_type event_type ip_address" << std::endl;
      for (unsigned long i = 0; i < m_data->size() / sizeof(InputSchema_128); ++i) {
        printf("[DBG] %09d: %7d %13ld %8ld %13ld %3ld %6ld %2ld \n",
               i, buf[i].timestamp, (long) buf[i].user_id, (long) buf[i].page_id, (long) buf[i].ad_id,
               buf[i].ad_type, buf[i].event_type, (long) buf[i].ip_address);
      }
    }
  };

  std::vector<char> *getInMemoryData() override {
    return m_data;
  }

  std::vector<char> *getStaticData() override {
    return m_staticData;
  }

  TupleSchema *getSchema() override {
    if (m_schema == nullptr)
      createSchema();
    return m_schema;
  }

  void createSchema() {
    if (m_is64)
      createSchema_64();
    else
      createSchema_128();
  }

  void createSchema_64() {
    m_schema = new TupleSchema(8, "YahooBenchmark");
    auto longAttr = AttributeType(BasicType::Long);

    m_schema->setAttributeType(0, longAttr); /*   timestamp:  long  */
    m_schema->setAttributeType(1, longAttr); /*     user_id:  long */
    m_schema->setAttributeType(2, longAttr); /*     page_id:  long */
    m_schema->setAttributeType(3, longAttr); /*       ad_id:  long */
    m_schema->setAttributeType(4, longAttr); /*     ad_type:  long  */
    m_schema->setAttributeType(5, longAttr); /*  event_type:  long  */
    m_schema->setAttributeType(6, longAttr); /*  ip_address:  long  */
    m_schema->setAttributeType(7, longAttr); /*     padding:  long */
  }

  void createSchema_128() {
    m_schema = new TupleSchema(10, "YahooBenchmark");
    auto longAttr = AttributeType(BasicType::Long);
    auto lLongAttr = AttributeType(BasicType::LongLong);

    m_schema->setAttributeType(0, longAttr);  /*   timestamp:  long     */
    m_schema->setAttributeType(1, longAttr);  /*     padding:  long     */
    m_schema->setAttributeType(2, lLongAttr); /*     user_id:  longLong */
    m_schema->setAttributeType(3, lLongAttr); /*     page_id:  longLong */
    m_schema->setAttributeType(4, lLongAttr); /*       ad_id:  longLong */
    m_schema->setAttributeType(5, longAttr);  /*     ad_type: long     */
    m_schema->setAttributeType(6, longAttr);  /*  event_type: long     */
    m_schema->setAttributeType(7, lLongAttr); /*  ip_address:  longLong */
    m_schema->setAttributeType(8, lLongAttr); /*     padding:  longLong */
    m_schema->setAttributeType(9, lLongAttr); /*     padding:  longLong */
  }
};
