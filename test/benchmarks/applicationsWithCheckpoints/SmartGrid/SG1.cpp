#include "benchmarks/applications/SmartGrid/SmartGrid.h"
#include "compression/Compressor.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/AggregationType.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "snappy.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"
#include "utils/WindowDefinition.h"

namespace SG1Compress {
struct alignas(16) input_tuple_t {
  long timestamp;
  float value;
  int property;
  int plug;
  int household;
  int house;
  int padding;
};

std::vector<std::string, tbb::cache_aligned_allocator<std::string>> *metadata;

void compressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (2);
  BaseDeltaCompressor<long, uint8_t> bcomp(data[0].timestamp);
  DummyFloatCompressor fcomp(1000000);
  struct t_1 {
    uint16_t timestamp : 8;
    uint16_t counter   :  8;
  };
  struct t_2 {
    uint32_t value   : 22;
    uint16_t counter : 10;
  };

  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  t_1 *buf1 = (t_1 *)(output);
  t_2 *buf2 = (t_2 *)(output + (int) (length*0.5));
  size_t n = (end - start) / sizeof(input_tuple_t);
  uint16_t count_1 = 1;
  uint8_t count_2 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    auto fVal_1 = (uint16_t) bcomp.compress(data[idx].timestamp);
    auto sVal_1 = fVal_1;
    if (idx < n - 1 && fVal_1 ==
        (sVal_1 = bcomp.compress(data[idx + 1].timestamp))) {
      count_1++;
    } else {
      buf1[idxs[0]++] = {fVal_1, count_1};
      fVal_1 = sVal_1;
      count_1 = 1;
    }

    auto fVal_2 = fcomp.compress(data[idx].value);
    auto sVal_2 = fVal_2;
    if (idx < n - 1 &&
        fVal_2 == (sVal_2 = fcomp.compress(data[idx + 1].value))) {
      count_2++;
    } else {
      buf2[idxs[1]++] = {(uint16_t)fVal_2, count_2};
      fVal_2 = sVal_2;
      count_2 = 1;
    }
  }
  std::memcpy((void *)(output + idxs[0] * sizeof(t_1)), (void *)buf2,
              idxs[1] * sizeof(t_2));
  writePos += idxs[0] * sizeof(t_1) + idxs[1] * sizeof(t_2);
}

void compressInput_(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (2);
  BaseDeltaCompressor<long, uint8_t> bcomp(data[0].timestamp);
  DummyFloatCompressor fcomp(1000);
  struct t_1 {
    uint16_t timestamp : 8;
    uint16_t counter   :  8;
  };
  struct t_2 {
    uint32_t value   : 20;
    uint16_t counter : 12;
  };

  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  t_1 *buf1 = (t_1 *)(output);
  t_2 *buf2 = (t_2 *)(output + (int) (length*0.5));
  size_t n = (end - start) / sizeof(input_tuple_t);
  uint16_t count_1 = 1;
  uint16_t count_2 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    auto fVal_1 = (uint16_t) bcomp.compress(data[idx].timestamp);
    auto sVal_1 = fVal_1;
    if (idx < n - 1 && count_1 < 255 && fVal_1 ==
        (sVal_1 = bcomp.compress(data[idx + 1].timestamp))) {
      count_1++;
    } else {
      buf1[idxs[0]++] = {fVal_1, count_1};
      fVal_1 = sVal_1;
      count_1 = 1;
    }

    auto fVal_2 = fcomp.compress(data[idx].value);
    auto sVal_2 = fVal_2;
    if (idx < n - 1 && count_2 < 4095 &&
        fVal_2 == (sVal_2 = fcomp.compress(data[idx].value))) {
      count_2++;
    } else {
      buf2[idxs[1]++] = {(uint8_t)fVal_2, count_2};
      fVal_2 = sVal_2;
      count_2 = 1;
    }
    //buf2[idxs[1]++] = {(uint16_t)std::round(data[idx].value*1)};
  }

  writePos += idxs[0] * sizeof(t_1);
  (*metadata)[pid] = "c0 BS " + std::to_string(data[0].timestamp)  + " {uint8_t:8;uint8_t:8} " + std::to_string(writePos);
  std::memcpy((void *)(output + writePos), (void *)buf2,
              idxs[1] * sizeof(t_2));
  writePos += idxs[1] * sizeof(t_2);
  (*metadata)[pid] += "c1 FM 1000 {uint32_t:20;uint16_t:12} " + std::to_string(writePos);

  if (SystemConf::getInstance().LATENCY_ON) {
    auto value = data[0].timestamp;
    latency = (int) (value >> 32);
    (*metadata)[pid] += " " + std::to_string(latency) + " ";
  }
  //(*metadata)[pid] = "r0 " + std::to_string(idxs[0]) + " r1 " + std::to_string(idxs[1]) + " ";
  if ((*metadata)[pid].size() > 128) {
    throw std::runtime_error("error: increase the metadata size");
  }
  std::memcpy((void *)(output - 128), (*metadata)[pid].data(), (*metadata)[pid].size());
  (*metadata)[pid].clear();
}

void decompressInput_(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &copy, long latency = -1) {
  // parse metadata
  std::string meta;
  if (copy) {
    std::memcpy(output, input, end);
    for (size_t i = 0; i < 128; i++) {
      meta += input[i];
    }
  } else {
    for (size_t i = 0; i < 128; i++) {
      meta += output[i];
    }
  }

  std::istringstream iss(meta);
  std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                 std::istream_iterator<std::string>{}};

  auto base = std::stoi(words[2]);
  auto mul = std::stoi(words[7]);
  std::vector<size_t> idxs (2);
  idxs[0] = std::stoi(words[4]);
  idxs[1] = std::stoi(words[9]);
  if (SystemConf::getInstance().LATENCY_ON) {
    latency = std::stoi(words[10]);
  }

  struct t_1 {
    uint16_t timestamp : 8;
    uint16_t counter   :  8;
  };
  struct t_2 {
    uint32_t value   : 20;
    uint16_t counter : 12;
  };

  auto res = (input_tuple_t*) input;
  t_1 *col0 = (t_1 *)(output + 128);
  auto *col1 = (t_2 *)(output + 128 + idxs[0]);
  auto wPos = 0;
  auto dataSize = end / sizeof(input_tuple_t);
  auto col1Size = idxs[0] / sizeof(t_1);
  for (int idx = 0; idx < col1Size; ++idx) {
    auto temp = col0[idx];
    for (int it = 0; it < temp.counter; ++it) {
      res[wPos++].timestamp = temp.timestamp + base;
      if (wPos * sizeof(input_tuple_t) > SystemConf::getInstance().BATCH_SIZE) {
        throw std::runtime_error("error: the write position exceeds the batch size");
      }
    }
  }

  if (SystemConf::getInstance().LATENCY_ON) {
    res[0].timestamp = Utils::pack(latency, (int)res[0].timestamp);
  }

  // c1
  wPos = 0;
  for (int idx = 0; idx < col1Size; ++idx) {
    auto temp = col1[idx];
    for (int it = 0; it < temp.counter; ++it) {
      res[wPos++].value = (float) temp.value * mul;
      if (wPos * sizeof(input_tuple_t) > SystemConf::getInstance().BATCH_SIZE) {
        throw std::runtime_error("error: the write position exceeds the batch size");
      }
    }
  }

  writePos = wPos * sizeof(input_tuple_t);

  /*std::cout << "===========decompress===========" << std::endl;
  auto n = dataSize;
  for (int i = 0; i <n; i++) {
    std::cout << i << " " << res[i].timestamp << " " << res[i].value << std::endl;
  }
  std::cout << "======================" << std::endl;*/
}

struct tempV {
  int _1;
};
std::vector<tempV> tempVec[20];
bool isFirst[20] = {false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false};
void compressGenInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (2);
  GorillaTimestampCompressor<long, uint64_t> gorillaComp;
  Simple8 simpleComp;

  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  size_t n = (end - start) / sizeof(input_tuple_t);
  // gorilla timestamp
  auto buf1 = (uint64_t *)output;
  uint8_t count_1 = 14;  // as the first delta is stored in 14 bits
  // store first timestamp in 64bits + first delta int 14 bits
  buf1[idxs[0]++] = data[0].timestamp;
  int64_t newDelta = data[1].timestamp - data[0].timestamp;
  buf1[idxs[0]] = newDelta << (64 - count_1);

  if (!isFirst[pid]) {
    tempVec[pid].resize(n);
    isFirst[pid] = true;
  }

  uint8_t count_2 = 1;
  uint16_t count_3 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    if (idx > 1) {
      auto [deltaD, deltaLength] = gorillaComp.compress(
          data[idx].timestamp, data[idx - 1].timestamp,
          data[idx - 2].timestamp);
      if (count_1 + deltaLength > 64) {
        uint8_t split = (64 - count_1);
        if (deltaLength > 1) {
          buf1[idxs[0]] |= deltaD >> (deltaLength - split);
        }
        ++idxs[0];
        count_1 = deltaLength - split;
      } else {
        count_1 += deltaLength;
      }
      buf1[idxs[0]] |= deltaD << (64 - count_1);
    }
    tempVec[pid][idx]._1 = data[idx].value;
  }

  // simple 8
  auto tempData = tempVec[pid].data();
  auto buf2 = (uint64_t *)(output + (int) (length*0.33));
  int32_t inOffset = 0;
  int32_t outOffset = 0;
  idxs[1] = simpleComp.compress(
      inOffset, outOffset, n, &tempData, &tempV::_1, 1, buf2);

  std::memcpy((void *)(output + idxs[0] * sizeof(uint64_t)), (void *)buf2,
              idxs[1] * sizeof(uint64_t));
  writePos += idxs[0] * sizeof(uint64_t) + idxs[1] * sizeof(uint64_t);
}

void noCompressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  struct res1 {
    long timestamp;
  };
  struct res2 {
    float value;
  };
  size_t n = (end - start) / sizeof(input_tuple_t);
  auto out1 = (res1*) output;
  auto out2 = (res2*) (output + (int)(n * sizeof(res1)));
  for (size_t idx = 0; idx < n; idx++) {
    out1[idx] = {data[idx].timestamp};
    out2[idx] = {data[idx].value};
  }
  writePos = n * sizeof(res1) + n * sizeof(res2);
}

void decompressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &copy, long latency = -1) {
  throw std::runtime_error("error: the decompression function is not implemented");
}

void filterInput(char *input, int start, int end, char *output, int startOutput, int &writePos) {
  auto data = (input_tuple_t *)input;
  struct res {
    long timestamp;
    long _pad;
    __uint128_t userId;
  };
  auto out = (res*) output;
  size_t n = (end - start) / sizeof(input_tuple_t);
  size_t outIdx = startOutput;
  for (size_t idx = 0; idx < n; idx++) {
    std::memcpy(&out[idx], &data[idx], 16);
  }
  writePos = n * sizeof(res);
}

void onlyCompressInputLossless(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (1);
  for (auto &i : idxs) {
    i = 0;
  }

  size_t output_length;
  auto buf1 = (uint64_t *)input;
  snappy::RawCompress((const char *)(buf1), end, (char*)(output), &output_length);
  writePos += output_length;
}
};

class SG1 : public SmartGrid {
 private:
  void createApplication() override {
    SystemConf::getInstance().PARTIAL_WINDOWS = 3800;

    bool persistInput = SystemConf::getInstance().PERSIST_INPUT;

    // Configure first query
    std::vector<AggregationType> aggregationTypes(1);
    aggregationTypes[0] = AggregationTypes::fromString("avg");

    std::vector<ColumnReference *> aggregationAttributes(1);
    aggregationAttributes[0] = new ColumnReference(1, BasicType::Float);

    std::vector<Expression *> groupByAttributes;

    auto window = new WindowDefinition(RANGE_BASED, 3600, 1); //ROW_BASED, 85*400, 1*400);
    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

#if defined(TCP_INPUT)
    bool replayTimestamps = false;
#elif defined(RDMA_INPUT)
    bool replayTimestamps = false;
#else
    bool replayTimestamps = window->isRangeBased();
#endif

    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true);
    genCode->setInputSchema(getSchema());
    genCode->setAggregation(aggregation);
    genCode->setQueryId(0);
    genCode->setup();
    OperatorCode *cpuCode = genCode;

    // Print operator
    std::cout << genCode->toSExpr() << std::endl;

    // Define an ft-operator
    auto queryOperator = new QueryOperator(*cpuCode, true);
    std::vector<QueryOperator *> operators;
    operators.push_back(queryOperator);

    // this is used for latency measurements
    m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();

    std::vector<std::shared_ptr<Query>> queries(1);
    queries[0] = std::make_shared<Query>(0,
                                         operators,
                                         *window,
                                         m_schema,
                                         m_timestampReference,
                                         true,
                                         replayTimestamps,
                                         !replayTimestamps, false,
                                         0, persistInput, nullptr, !SystemConf::getInstance().RECOVER);

#if defined(RDMA_INPUT)
    //queries[0]->getBuffer()->setFilterFP(SG1Compress::filterInput);
#endif

    if (persistInput && SystemConf::getInstance().CHECKPOINT_COMPRESSION) {
      queries[0]->getBuffer()->setCompressionFP(SG1Compress::compressInput_);
      queries[0]->getBuffer()->setDecompressionFP(SG1Compress::decompressInput_);
    }

    m_application = new QueryApplication(queries, SystemConf::getInstance().CHECKPOINT_ON, !SystemConf::getInstance().RECOVER);
    m_application->setup();

    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION && (SystemConf::getInstance().CHECKPOINT_ON || persistInput)) {
      SG1Compress::metadata = new std::vector<std::string, tbb::cache_aligned_allocator<std::string>>(SystemConf::getInstance().WORKER_THREADS, "");
      //m_application->getCheckpointCoordinator()->setCompressionFP(0, SG1Compress::compress);
    }
  }

 public:
  SG1(bool inMemory = true) {
    m_name = "SG1";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};
