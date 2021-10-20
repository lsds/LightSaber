#include "benchmarks/applications/ManufacturingEquipment/ManufacturingEquipment.h"
#include "compression/Compressor.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/AggregationType.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "snappy.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"
#include "utils/WindowDefinition.h"

// --unbounded-size 32768 --circular-size 16777216 --batch-size 262144 --bundle-size 262144 --disk-block-size 32768 --latency tru --checkpoint-compression true --persist-input true --lineage true --threads 10
namespace ME1Compress {
struct alignas(16) input_tuple_t {
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
};

void compressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (4);
  BaseDeltaCompressor<long, uint8_t> bcomp(data[0].timestamp);
  struct t_1 {
    uint8_t timestamp : 6;
    uint16_t counter : 10;
  };
  struct t_2 {
    //uint16_t mf01 : 12;
    uint8_t mf01 : 4;
    uint8_t counter : 4;
  };
  struct t_3 {
    //uint16_t mf02 : 12;
    uint8_t mf02 : 4;
    uint8_t counter : 4;
  };
  struct t_4 {
    //uint16_t mf03 : 12;
    uint8_t mf03 : 4;
    uint8_t counter : 4;
  };

  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  t_1 *buf1 = (t_1 *)(output);
  t_2 *buf2 = (t_2 *)(output + (int) (length*0.25));
  t_3 *buf3 = (t_3 *)(output + (int) (length*0.5));
  t_3 *buf4 = (t_3 *)(output + (int) (length*0.75));
  size_t n = (end - start) / sizeof(input_tuple_t);
  uint16_t count_1 = 1;
  uint8_t count_2 = 1;
  uint8_t count_3 = 1;
  uint8_t count_4 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    auto fVal_1 = bcomp.compress(data[idx].timestamp);
    auto sVal_1 = fVal_1;
    if (idx < n - 1 && fVal_1 ==
        (sVal_1 = bcomp.compress(data[idx + 1].timestamp))) {
      count_1++;
    } else {
      buf1[idxs[0]++] = {fVal_1, count_1};
      fVal_1 = sVal_1;
      count_1 = 1;
    }

    auto fVal_2 = (uint16_t)data[idx].mf01;
    auto sVal_2 = fVal_2;
    if (idx < n - 1 &&
        fVal_2 == (sVal_2 = (uint16_t)data[idx + 1].mf01)) {
      count_2++;
    } else {
      buf2[idxs[1]++] = {(uint8_t)fVal_2, count_2};
      fVal_2 = sVal_2;
      count_2 = 1;
    }

    auto fVal_3 = (uint16_t)data[idx].mf02;
    auto sVal_3 = fVal_3;
    if (idx < n - 1 &&
        fVal_3 == (sVal_3 = (uint16_t)data[idx + 1].mf02)) {
      count_3++;
    } else {
      buf3[idxs[2]++] = {(uint8_t)fVal_3, count_3};
      fVal_3 = sVal_3;
      count_3 = 1;
    }

    auto fVal_4 = (uint16_t)data[idx].mf03;
    auto sVal_4 = fVal_4;
    if (idx < n - 1 &&
        fVal_4 == (sVal_4 = (uint16_t)data[idx + 1].mf03)) {
      count_4++;
    } else {
      buf4[idxs[3]++] = {(uint8_t)fVal_4, count_4};
      fVal_4 = sVal_4;
      count_4 = 1;
    }
  }
  std::memcpy((void *)(output + idxs[0] * sizeof(t_1)), (void *)buf2,
              idxs[1] * sizeof(t_2));
  std::memcpy((void *)(output + idxs[0] * sizeof(t_1) +
                  idxs[1] * sizeof(t_2)),
              (void *)buf3, idxs[2] * sizeof(t_3));
  std::memcpy((void *)(output + idxs[0] * sizeof(t_1) +
                  idxs[1] * sizeof(t_2) + idxs[2] * sizeof(t_3)),
              (void *)buf4, idxs[3] * sizeof(t_4));
  writePos += idxs[0] * sizeof(t_1) + idxs[1] * sizeof(t_2) +
      idxs[2] * sizeof(t_3) + idxs[3] * sizeof(t_4);
}

inline void compressInput_(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency) {
  if (start == 0 && end == -1) {
    return;
  }
  if (clear) {
    clear = false;
  }

  if (start == end || end < start) {
    return;
  }

  // Input Buffer
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (4, 0);
  uint16_t count_0 = 1;
  BaseDeltaCompressor<long, uint8_t> comp_0(data[0].timestamp);
  auto temp_0 = comp_0.compress(data[0].timestamp);
  struct t_0 {
    uint8_t _0 : 6;
    uint16_t counter : 10;
  };
  uint8_t count_1 = 1;
  auto temp_1 = (uint16_t)data[0].mf01;
  struct t_1 {
    uint16_t _1 : 10;
    uint8_t counter : 4;
  };
  uint8_t count_2 = 1;
  auto temp_2 = (uint16_t)data[0].mf02;
  struct t_2 {
    uint16_t _2 : 12;
    uint8_t counter : 4;
  };
  uint8_t count_3 = 1;
  auto temp_3 = (uint16_t)data[0].mf03;
  struct t_3 {
    uint16_t _3 : 12;
    uint8_t counter : 4;
  };
  // output buffers
  int barriers[4];
  barriers[0] = (int)(length*0.000000);
  t_0 *buf0 = (t_0 *) (output + barriers[0]);
  barriers[1] = (int)(length*0.250000);
  t_1 *buf1 = (t_1 *) (output + barriers[1]);
  barriers[2] = (int)(length*0.500000);
  t_2 *buf2 = (t_2 *) (output + barriers[2]);
  barriers[3] = (int)(length*0.750000);
  t_3 *buf3 = (t_3 *) (output + barriers[3]);
  size_t n = (end - start) / sizeof(input_tuple_t);

  for (size_t idx = 0; idx < n; idx++) {
    // apply compression
    //if (comp_0.check(data[idx].timestamp)) {
    //  std::cout << "warning: falling back to the original compression scheme"<< std::endl;
    //  clear = true;
    //  return;
    //}
    auto res_0 = comp_0.compress(data[idx].timestamp);
    // apply RLE
    if (temp_0 != res_0 || count_0 >= 1023.000000) {
      buf0[idxs[0]++] = {temp_0, count_0};
      count_0 = 0;
      temp_0 = res_0;
    } else {
      count_0++;
    }
    //if (!CanTypeFitValue<uint16_t,int>(data[idx].mf01)) {
//      std::cout << "warning: falling back to the original compression scheme"<< std::endl;
//      clear = true;
//      return;
//    }
    uint16_t res_1 = (uint16_t) data[idx].mf01;
    // apply RLE
    if (temp_1 != res_1 || count_1 >= 15.000000) {
      buf1[idxs[1]++] = {temp_1, count_1};
      count_1 = 0;
      temp_1 = res_1;
    } else {
      count_1++;
    }
    /*if (!CanTypeFitValue<uint16_t,int>(data[idx]._3)) {
      std::cout << "warning: falling back to the original compression scheme"<< std::endl;
      clear = true;
      return;
    }*/
    uint16_t res_2 = (uint16_t) data[idx].mf02;
    // apply RLE
    if (temp_2 != res_2 || count_2 >= 15.000000) {
      buf2[idxs[2]++] = {temp_2, count_2};
      count_2 = 0;
      temp_2 = res_2;
    } else {
      count_2++;
    }
    /*if (!CanTypeFitValue<uint16_t,int>(data[idx]._4)) {
      std::cout << "warning: falling back to the original compression scheme"<< std::endl;
      clear = true;
      return;
    }*/
    uint16_t res_3 = (uint16_t) data[idx].mf03;
    // apply RLE
    if (temp_3 != res_3 || count_3 >= 15.000000) {
      buf3[idxs[3]++] = {temp_3, count_3};
      count_3 = 0;
      temp_3 = res_3;
    } else {
      count_3++;
    }
  }
  if (count_0 != 0) {
    buf0[idxs[0]++] = {temp_0, count_0};
  }
  if (count_1 != 0) {
    buf1[idxs[1]++] = {temp_1, count_1};
  }
  if (count_2 != 0) {
    buf2[idxs[2]++] = {temp_2, count_2};
  }
  if (count_3 != 0) {
    buf3[idxs[3]++] = {temp_3, count_3};
  }
  // copy results and set output pointers
  writePos += idxs[0] * sizeof(t_0);
  if (writePos > barriers[1]) {throw std::runtime_error("error: larger barriers needed");}
  std::memcpy((void *)(output + writePos), (void *)buf0, idxs[0] * sizeof(t_0));
  writePos += idxs[1] * sizeof(t_1);
  if (writePos > barriers[2]) {throw std::runtime_error("error: larger barriers needed");}
  std::memcpy((void *)(output + writePos), (void *)buf1, idxs[1] * sizeof(t_1));
  writePos += idxs[2] * sizeof(t_2);
  if (writePos > barriers[3]) {throw std::runtime_error("error: larger barriers needed");}
  std::memcpy((void *)(output + writePos), (void *)buf2, idxs[2] * sizeof(t_2));
  writePos += idxs[3] * sizeof(t_3);
  if (writePos > length) {throw std::runtime_error("error: larger barriers needed");}
}

void compressGenInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (4);
  GorillaTimestampCompressor<long, uint64_t> gorillaComp;
  Simple8 simpleComp1;
  Simple8 simpleComp2;
  Simple8 simpleComp3;
  //GorillaValuesCompressor xorComp;
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

  // simple 8
  auto buf2 = (uint64_t *)(output + (int) (length*0.25));
  int32_t inOffset = 0;
  int32_t outOffset = 0;
  idxs[1] = simpleComp1.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::mf01, 1, buf2);

  // simple 8
  auto buf3 = (uint64_t *)(output + (int) (length*0.5));
  inOffset = 0;
  outOffset = 0;
  idxs[2] = simpleComp2.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::mf02, 1, buf3);

  // simple 8
  auto buf4 = (uint64_t *)(output + (int) (length*0.75));
  inOffset = 0;
  outOffset = 0;
  idxs[3] = simpleComp3.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::mf03, 1, buf4);


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
  }

  std::memcpy((void *)(output + idxs[0] * sizeof(uint64_t)), (void *)buf2,
              idxs[1] * sizeof(uint64_t));
  std::memcpy((void *)(output + idxs[0] * sizeof(uint64_t) +
                  idxs[1] * sizeof(uint64_t)),
              (void *)buf3, idxs[2] * sizeof(uint64_t));
  std::memcpy((void *)(output + idxs[0] * sizeof(uint64_t) +
                  idxs[1] * sizeof(uint64_t) + idxs[2] * sizeof(uint64_t)),
              (void *)buf4, idxs[3] * sizeof(uint64_t));
  writePos += idxs[0] * sizeof(uint64_t) + idxs[1] * sizeof(uint64_t) +
      idxs[2] * sizeof(uint64_t) + idxs[3] * sizeof(uint64_t);
}

void noCompressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  struct res {
    long timestamp;
    int mf01;
    int mf02;
    int mf03;
  };
  auto out = (res*) output;
  size_t n = (end - start) / sizeof(input_tuple_t);
  for (size_t idx = 0; idx < n; idx++) {
    out[idx] = {data[idx].timestamp, data[idx].mf01, data[idx].mf02, data[idx].mf02};
  }
  writePos = n * sizeof(res);
}

void decompressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &copy, long latency = -1) {
  throw std::runtime_error("error: the decompression function is not implemented");
}

void onlyCompressInputLossless(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;

  size_t output_length;
  auto buf1 = (uint64_t *)input;
  snappy::RawCompress((const char *)(buf1), end, (char*)(output), &output_length);
  writePos += output_length;
}

inline void filterAndCompress(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency) {
  if (start == 0 && end == -1) {
    return;
  }
  if (clear) {
    clear = false;
  }

  if (start == end || end < start) {
    return;
  }

  struct tempS {
    long timestamp;
    int mf01;
    int mf02;
    int mf03;
  };
  // Input Buffer
  auto data = (input_tuple_t *)input;
  auto buf = (tempS *) (output + (int)(end*0.5));
  std::vector<size_t> idxs (4, 0);
  size_t n = (end - start) / sizeof(input_tuple_t);

  for (size_t idx = 0; idx < n; idx++) {
    buf[idx] = {data[idx].timestamp, data[idx].mf01, data[idx].mf02, data[idx].mf03};
  }
  size_t output_length;
  snappy::RawCompress((const char *)(buf),n*sizeof(tempS), (char*)(output), &output_length);
  writePos += output_length;
}
};

class ME1 : public ManufacturingEquipment {
 private:
  void createApplication() override {
    //SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE = 4096;
    SystemConf::getInstance().SLOTS = 128;
    SystemConf::getInstance().PARTIAL_WINDOWS = 2*128; // change this depending on the batch size

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;
    bool persistInput = SystemConf::getInstance().PERSIST_INPUT;

    // Configure first query
    std::vector<AggregationType> aggregationTypes(3);
    aggregationTypes[0] = AggregationTypes::fromString("avg");
    aggregationTypes[1] = AggregationTypes::fromString("avg");
    aggregationTypes[2] = AggregationTypes::fromString("avg");

    std::vector<ColumnReference *> aggregationAttributes(3);
    aggregationAttributes[0] = new ColumnReference(2, BasicType::Integer);
    aggregationAttributes[1] = new ColumnReference(3, BasicType::Integer);
    aggregationAttributes[2] = new ColumnReference(4, BasicType::Integer);

    std::vector<Expression *> groupByAttributes;

    auto window = new WindowDefinition(RANGE_BASED, 60, 1);

    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

#if defined(TCP_INPUT)
    bool replayTimestamps = false;
#elif defined(RDMA_INPUT)
    bool replayTimestamps = false;
#else
    bool replayTimestamps = window->isRangeBased();
#endif

    // Set up code-generated operator
    // reuse previous generated file
    OperatorKernel *genCode = new OperatorKernel(true, true, useParallelMerge, true);
    genCode->setInputSchema(getSchema());
    genCode->setAggregation(aggregation);
    genCode->setQueryId(0);
    genCode->setup();
    OperatorCode *cpuCode = genCode;


    // Print operator
    std::cout << cpuCode->toSExpr() << std::endl;

    // Define an ft-operator
    auto queryOperator = new QueryOperator(*cpuCode, true);
    std::vector<QueryOperator *> operators;
    operators.push_back(queryOperator);

    m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();

    std::vector<std::shared_ptr<Query>> queries(1);
    queries[0] = std::make_shared<Query>(0,
                                         operators,
                                         *window,
                                         m_schema,
                                         m_timestampReference,
                                         true,
                                         replayTimestamps,
                                         !replayTimestamps,
                                         useParallelMerge,
                                         0, persistInput, nullptr, !SystemConf::getInstance().RECOVER);

    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION) {
      if (SystemConf::getInstance().ADAPTIVE_CHANGE_DATA) {
        queries[0]->getBuffer()->setCompressionFP(ME1Compress::compressGenInput);
        //queries[0]->getBuffer()->setCompressionFP(ME1Compress::compressInput_);
      } else {
        queries[0]->getBuffer()->setCompressionFP(ME1Compress::compressInput_);
      }
      queries[0]->getBuffer()->setDecompressionFP(ME1Compress::decompressInput);
    }

    m_application = new QueryApplication(queries, SystemConf::getInstance().CHECKPOINT_ON, !SystemConf::getInstance().RECOVER);
    m_application->setup();
  }

 public:
  ME1(bool inMemory = true) {
    m_name = "ME1";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};
